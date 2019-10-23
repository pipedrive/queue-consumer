import time
import weakref
from multiprocessing import cpu_count
from threading import Thread, Event

from .support import support
from .worker import Worker


__all__ = (
    'Consumer',
)


HANDLER_NAME = 'message_handler'


def _process_handler(messages):
    # It calls injected handler and passes messages interator there.
    try:
        globals()[HANDLER_NAME](messages)
    except Exception as exc:
        exc.args += (messages,)
        raise
    # It needs to say parent process about the iterator state and what messages
    # weren't processed. That why it returns iterator or attaches it to exception.
    return messages


class Consumer:

    def __init__(self,
                 queue,
                 handler=None,
                 max_workers=cpu_count(),
                 max_handlers=cpu_count(),
                 messages_bulk_size=1,
                 worker_polling_time=0,
                 pool_initializer=None,
                 with_thread_executor=True):
        handler = getattr(queue, 'handler', handler)
        assert handler, 'Messages handler is not defined!'


        if with_thread_executor:
            from bounded_pool import BoundedThreadPool as Pool
            initializer = pool_initializer

            def _thread_handler(messages):
                try:
                    handler(messages)
                except Exception as exc:
                    exc.args += (messages,)
                    raise
                return messages

            self._handler = _thread_handler
        else:
            from bounded_pool import BoundedProcessPool as Pool

            def initializer():
                import os
                support.logger.debug(f'Pool worker launched in pid {os.getpid()}')
                # It injects handler into child process one time only.
                # Then it will be just called as many time as need.
                globals()[HANDLER_NAME] = handler
                support.logger.debug(f'Handler {HANDLER_NAME!r} is injected')
                pool_initializer and pool_initializer()

            self._handler = _process_handler

        self._queue = queue
        self._handlers_pool = weakref.WeakKeyDictionary()
        self._stuck_handlers = weakref.WeakSet()
        self._max_handlers = max_handlers
        self._executor = Pool(max_handlers, initializer=initializer)
        self._messages_bulk_size = messages_bulk_size
        self._worker_polling_time = worker_polling_time
        self._workers = [self._get_worker() for _ in range(max_workers)]
        self._shutdown = False
        self._no_supervise = Event()
        support.statsd.increment('revived.workers', 0)
        support.statsd.increment('stuck.handlers', 0)

    def start(self):
        for worker in self._workers:
            worker.start()
        support.logger.info('Queue Consumer is started')

    def shutdown(self):
        self._shutdown = True
        self._no_supervise.wait()
        for worker in self._workers:
            worker.shutdown()
        support.logger.info('Queue Consumer is shutdown')

    def supervise(self,
                  blocking=False,
                  polling_time=1,
                  stuck_time=60,
                  stuck_limit=None):
        stuck_limit = stuck_limit or self._max_handlers
        support.logger.debug(
            'Queue Consumer supervising in '
            f'{"blocking" if blocking else "non-blocking"} mode ...')

        args = (polling_time, stuck_time, stuck_limit)
        if blocking:
            self._supervise(*args)
        else:
            t = Thread(target=self._supervise, args=args, daemon=True)
            t.start()
            return t

    def _supervise(self, polling_time, stuck_time, stuck_limit):
        while True:
            self._check_workers()
            self._check_handlers(stuck_time)

            stuck_handlers_number = len(self._stuck_handlers)
            if stuck_handlers_number > stuck_limit:
                raise RuntimeError(
                    f'Number of stuck handlers {stuck_handlers_number} more '
                    f'than limit {stuck_limit}')

            if self._shutdown:
                self._no_supervise.set()
                return

            time.sleep(polling_time)

    def _check_workers(self):
        workers = []
        for worker in self._workers:

            if worker.is_alive():
                workers.append(worker)
            else:
                new_worker = self._get_worker()
                new_worker.start()
                workers.append(new_worker)
                support.statsd.increment('revived.workers')
        self._workers = workers

    def _check_handlers(self, time_limit):
        for handler, timestamp in self._handlers_pool.items():

            if not handler.running():
                continue

            if time.time() - timestamp < time_limit:
                continue

            if not self._executor.release(handler):
                continue

            self._stuck_handlers.add(handler)
            support.statsd.increment('stuck.handlers')

    def _get_worker(self):
        return Worker(self._queue,
                      self._executor,
                      self._handler,
                      self._handlers_pool,
                      self._messages_bulk_size,
                      self._worker_polling_time)
