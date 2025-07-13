from collections.abc import Iterator
from threading import Lock

from pika import BlockingConnection

from qio.message import Message

from .consumer import Consumer

QUEUE_NAME = "qio"


class Broker:
    """A broker enables producing and consuming messages on a queue."""

    def __init__(self):
        self.__producer_channel_lock = Lock()
        self.__producer_channel = BlockingConnection().channel()
        self.__consumers = set[Consumer]()
        self.__messages = dict[Message, tuple[Consumer, int]]()

    def enqueue(self, body: bytes, /):
        with self.__producer_channel_lock:
            self.__producer_channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=body,
            )

    def consume(self, *, prefetch: int) -> Iterator[Message]:
        consumer = Consumer(queue=QUEUE_NAME, prefetch=prefetch)
        self.__consumers.add(consumer)
        for tag, body in consumer:
            message = Message(body=body)
            self.__messages[message] = (consumer, tag)
            yield message

    def start(self, _: Message, /):
        """Start processing a message."""
        pass  # We assume that the message has been started

    def suspend(self, message: Message, /):
        """Report that the processing of a message has been suspended."""
        consumer, tag = self.__messages[message]
        consumer.delay(tag)

    def resume(self, message: Message, /):
        """Report that the processing of a message has resumed."""
        consumer, tag = self.__messages[message]
        consumer.undelay(tag)

    def complete(self, message: Message, /):
        """Report that the processing of a message has completed."""
        consumer, tag = self.__messages.pop(message)
        consumer.ack(tag)

    def shutdown(self):
        """Signal the final shutdown of the broker."""
        for consumer in self.__consumers:
            consumer.shutdown()
