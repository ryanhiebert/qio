from collections.abc import Callable
from threading import Lock
from typing import Any
from typing import cast

from pika import BlockingConnection

from .invocation import Invocation
from .invocation import deserialize


class Consumer:
    """Consume from the RabbitMQ queue."""

    def __init__(self, *, queue: str, prefetch: int):
        self.__connection = BlockingConnection()
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue=queue, durable=True)
        self.__channel.basic_qos(prefetch_count=prefetch)
        self.__iterator = self.__channel.consume(queue=queue)

    def __iter__(self):
        return self

    def __next__(self) -> tuple[int, Invocation]:
        method, _, body = next(self.__iterator)
        return cast(int, method.delivery_tag), deserialize(body)

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
