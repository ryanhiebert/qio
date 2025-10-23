from collections.abc import Callable
from collections.abc import Iterator
from threading import Lock
from typing import Any
from typing import cast

from pika import BlockingConnection
from pika import ConnectionParameters
from pika import URLParameters

from qio.broker import Broker
from qio.message import Message
from qio.queuespec import QueueSpec


class PikaBroker(Broker):
    """A broker enables producing and consuming messages on a queue."""

    @classmethod
    def from_uri(cls, uri: str, /):
        """Create a broker instance from a URI."""
        amqp_uri = "amqp:" + uri.removeprefix("pika:")
        return cls(URLParameters(amqp_uri))

    def __init__(self, connection_params: ConnectionParameters | URLParameters):
        self.__connection_params = connection_params
        self.__producer_channel_lock = Lock()
        self.__producer_channel = BlockingConnection(self.__connection_params).channel()
        self.__consumers = set[_Consumer]()
        self.__messages = dict[Message, tuple[_Consumer, int]]()
        self.__shutdown_lock = Lock()
        self.__shutdown = False
        self.__suspended = set[Message]()

    def enqueue(self, body: bytes, /, *, queue: str):
        with self.__producer_channel_lock:
            self.__producer_channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=body,
            )

    def purge(self, *, queue: str):
        with self.__producer_channel_lock:
            self.__producer_channel.queue_declare(queue=queue, durable=True)
            self.__producer_channel.queue_purge(queue=queue)

    def consume(self, queuespec: QueueSpec, /) -> Iterator[Message]:
        if not queuespec.queues:
            raise ValueError("QueueSpec must have at least one queue")
        if len(queuespec.queues) != 1:
            raise ValueError("Only one queue is supported")

        queue = queuespec.queues[0]
        prefetch = queuespec.concurrency

        consumer = _Consumer(
            connection_params=self.__connection_params,
            queue=queue,
            prefetch=prefetch,
        )
        self.__consumers.add(consumer)
        for tag, body in consumer:
            message = Message(body=body)
            self.__messages[message] = (consumer, tag)
            yield message

    def start(self, _: Message, /):
        """Start processing a message."""
        pass  # Assume that the message has been started

    def suspend(self, message: Message, /):
        """Report that the processing of a message has been suspended."""
        if message in self.__suspended:
            # Already acked previously
            return
        self.__suspended.add(message)
        # Can't change prefetch window dynamically on the consumer
        consumer, tag = self.__messages[message]
        consumer.ack(tag)

    def unsuspend(self, message: Message, /):
        """Report that the processing of a message has stopped being suspended."""
        pass  # Message has already been acked, do nothing.

    def complete(self, message: Message, /):
        """Report that the processing of a message has completed."""
        if message in self.__suspended:
            self.__suspended.remove(message)
            return  # Message has already been acked, do nothing.
        consumer, tag = self.__messages.pop(message)
        consumer.ack(tag)

    def shutdown(self):
        """Signal the final shutdown of the broker."""
        with self.__shutdown_lock:
            if self.__shutdown:
                return
            self.__shutdown = True
            for consumer in self.__consumers:
                consumer.shutdown()


class _Consumer:
    def __init__(
        self,
        *,
        connection_params: ConnectionParameters | URLParameters,
        queue: str,
        prefetch: int,
    ):
        self.__connection = BlockingConnection(connection_params)
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue=queue, durable=True)
        self.__channel.basic_qos(prefetch_count=prefetch)
        self.__iterator = self.__channel.consume(queue=queue)

    def __iter__(self):
        return self

    def __next__(self) -> tuple[int, bytes]:
        method, _, body = next(self.__iterator)
        return cast(int, method.delivery_tag), body

    def __blocking_callback(self, fn: Callable[[], Any]):
        """Queue a callback and block until it is executed."""
        lock = Lock()
        lock.acquire()

        def callback():
            try:
                fn()
            finally:
                lock.release()

        self.__connection.add_callback_threadsafe(callback)
        with lock:
            return

    def ack(self, delivery_tag: int, /):
        self.__blocking_callback(lambda: self.__ack(delivery_tag=delivery_tag))

    def shutdown(self):
        self.__blocking_callback(lambda: self.__shutdown())

    def __ack(self, *, delivery_tag: int):
        self.__channel.basic_ack(delivery_tag=delivery_tag)

    def __shutdown(self):
        self.__channel.cancel()
        self.__connection.close()
