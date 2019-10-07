# Concurrent queue consumer

#### Service, which consumes messages from a queue in multithreading mode and passes them to handlers, launched with other threads as well.

This project was born under inspiration of https://github.com/goodmanship/sqsworkers/ , where I would like to implement my vision of proper architecture in such projects.

My main concern was to make consumer more abstract, not related with AWS SQS only. Although I plan to use it mostly for AWS SQS consuming.

It follows the principle of conveyor assembly, when each stage of the assembly pass the items that have passed the previous stage. Failed items are rejected.

## How to use

```python
import boto3
from queue_consumer import Consumer


class Queue:

    def __init__(self,
                 queue_name,
                 max_number_of_messages=10,
                 wait_time_seconds=20):
        self._max_number_of_messages = max_number_of_messages
        self._wait_time_seconds = wait_time_seconds
        self._sqs = boto3.resource("sqs").get_queue_by_name(
            QueueName=queue_name)

    def get(self):
        return self._sqs.receive_messages(
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=self._max_number_of_messages,
            WaitTimeSeconds=self._wait_time_seconds,
        )

    def cleanup(self, messages):
        self._sqs.delete_messages(
            Entries=[
                {
                    "Id": message.message_id,
                    "ReceiptHandle": message.receipt_handle,
                }
                for message in messages
            ]
        )

    def handler(self, messages):
        for message in messages:
            do_some_stuff(message)


consumer = Consumer(Queue("my_queue"))
consumer.start()
consumer.supervise(blocking=True)
```

Method to `cleanup` is optional. SQS requires explicit item deletion from queue.
Also if there is a queue with `get` already, handler can be defined separately, like:

```python
def handler(messages):
    for message in messages:
        do_some_stuff(message)

consumer = Consumer(queue, handler=handler)
```

A handler takes iterator as argument. If handler raises exception, worker defines not processed (failed) messages basing on iterator remaining content. That's why messages should be read & processed one-by-one. To read all iterator before processing is bad idea.

**Right:**

```python
def handler(message):
    for message in messages:
        process(message)
```

**Wrong:**

```python
def handler(messages):
    for message in list(messages):
        process(message)
```

It doesn't matter with `messages_bulk_size=1` (default), when to read one message is the same as to read all iterator.

## API

```python
Consumer(
    queue,
    handler=None,
    max_workers=cpu_count(),
    max_handlers=cpu_count(),
    messages_bulk_size=1,
    worker_polling_time=0,
    pool_initializer=None,
    with_thread_executor=True)
```

Instantiate `consumer` object. Arguments:
- `queue` - Queue which to consume messages from.
- `handler=None` - Handler to processes messages. If `None` it try to take `queue.handler`. If no one exception is raised.
- `max_workers=cpu_count()` - Maximum number of concurrent workers to read messages from queue.
- `max_handlers=cpu_count()` - Maximum number of concurrent handlers to process messages from all workers.
- `messages_bulk_size=1` - Maximum number of messages sending to handler. Not bigger that `queue.get` can return.
- `worker_polling_time=0` - Seconds to sleep between `queue.get` calls. Can be fractional.
- `pool_initializer=None` - Function which will be called on pool executor workers start. **NB!** Don't confuse with `consumer` workers.
- `with_thread_executor=True` - Flag to launch pool workers as threads (`True`) or as processes (`False`).

```python
consumer.start()
```

Starts consumer workers.

```python
consumer.shutdown()
```

Shuts down consumer workers. **NB!** Can't be `.start()` again.

```python
consumer.supervise(blocking=False, polling_time=1)
```

Starts to supervise workers and to revive died. Arguments:
- `blocking=False` - Supervise will not block / block main thread.
- `polling_time=1` - Seconds to sleep between workers checking. Can be fractional.
