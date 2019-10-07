import os
import time
from multiprocessing import cpu_count
from threading import Thread, Event

from .support import support
from .worker import Worker


__all__ = (
    'Consumer',
)


HANDLER_NAME = '__message_handler'


def _handler(messages):
    try:
        globals()[HANDLER_NAME](messages)
    except Exception as exc:
        exc.args += (messages,)
        raise
    return messages


class Consumer:

    def __init__(self,
                 queue,
                 handler=None,
                 max_workers=cpu_count() * 4,
                 max_handlers=cpu_count() * 4 * 4,
                 messages_bulk_size=1,
                 worker_polling_time=0,
                 initializer=None,
                 process_pool=False):
        self._queue = queue
        self._handler = getattr(self._queue, 'handler', handler)
        assert self._handler, 'Messages handler is not defined!'

        if process_pool:
            from bounded_pool import BoundedProcessPool as Pool

            def init():
                import os
                support.logger.debug(f'Pool worker launched in pid {os.getpid()}')
                globals()[HANDLER_NAME] = handler
                support.logger.debug(f'Handler {HANDLER_NAME!r} is injected')
                initializer()

            self._handler = _handler

        else:
            from bounded_pool import BoundedThreadPool as Pool
            init = initializer

        self._executor = Pool(max_handlers, initializer=init)
        self._messages_bulk_size = messages_bulk_size
        self._worker_polling_time = worker_polling_time
        self._workers = [self._get_worker() for _ in range(max_workers)]
        self._shutdown = False
        self._no_supervise = Event()

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

    def supervise(self, blocking=False, polling_time=1):
        support.logger.debug(
            f'Queue Consumer supervising in {"blocking" if blocking else "non-blocking"} mode ...')

        def _supervise():
            while True:

                workers = []
                for worker in self._workers:

                    if worker.is_alive():
                        workers.append(worker)
                    else:
                        support.statsd.increment('revived.workers')

                        new_worker = self._get_worker()
                        new_worker.start()
                        workers.append(new_worker)
                self._workers = workers

                if self._shutdown:
                    self._no_supervise.set()
                    return

                time.sleep(polling_time)

        if blocking:
            _supervise()
        else:
            Thread(target=_supervise, daemon=True).start()

    def _get_worker(self):
        return Worker(self._queue,
                      self._executor,
                      self._handler,
                      self._messages_bulk_size,
                      self._worker_polling_time)
