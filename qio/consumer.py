from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Any
from typing import cast

from pika import BlockingConnection


class Consumer:
    """Consume from the RabbitMQ queue."""

    def __init__(self, *, queue: str, prefetch: int):
        self._connection = BlockingConnection()
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=queue)
        self._channel.basic_qos(prefetch_count=prefetch)
        self._iterator = self._channel.consume(queue=queue)

    def __iter__(self):
        return self

    def __next__(self) -> tuple[int, bytes]:
        method, _, body = next(self._iterator)
        return cast(int, method.delivery_tag), body

    def __blocking_callback(self, fn: Callable[[], Any]):
        """Queue a callback and block until it is executed."""
        lock = Lock()

        def callback():
            try:
                fn()
            finally:
                lock.release()

        lock.acquire()
        self._connection.add_callback_threadsafe(callback)
        lock.acquire()

    def ack(self, delivery_tag: int, /):
        self.__blocking_callback(lambda: self.__ack(delivery_tag=delivery_tag))

    def shutdown(self):
        self.__blocking_callback(lambda: self.__shutdown())

    def __ack(self, *, delivery_tag: int):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def __shutdown(self):
        self._channel.cancel()
