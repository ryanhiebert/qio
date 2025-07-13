from collections.abc import Callable
from threading import Lock
from typing import Any
from typing import cast

from pika import BlockingConnection


class Consumer:
    """Consume from the RabbitMQ queue."""

    def __init__(self, *, queue: str, prefetch: int):
        self.__qos_lock = Lock()
        self.__qos_prefetch = prefetch
        self.__qos_delayed = set[int]()
        self.__connection = BlockingConnection()
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

    def delay(self, delivery_tag: int, /):
        """Report that a message is delayed, and add more capacity."""
        with self.__qos_lock:
            self.__qos_delayed.add(delivery_tag)
            self.__blocking_callback(
                lambda: self.__channel.basic_qos(
                    prefetch_count=self.__qos_prefetch + len(self.__qos_delayed)
                )
            )

    def undelay(self, delivery_tag: int, /):
        """Report that a delayed message has been resumed.

        Remove the additional capacity previously added.
        """
        with self.__qos_lock:
            self.__qos_delayed.remove(delivery_tag)
            self.__blocking_callback(
                lambda: self.__channel.basic_qos(
                    prefetch_count=self.__qos_prefetch + len(self.__qos_delayed)
                )
            )

    def ack(self, delivery_tag: int, /):
        """Report that a message is fully completed.

        If the message was previously delayed, remove the additional capacity.
        """
        if delivery_tag in self.__qos_delayed:
            with self.__qos_lock:
                self.__qos_delayed.remove(delivery_tag)
                self.__blocking_callback(
                    lambda: self.__channel.basic_qos(
                        prefetch_count=self.__qos_prefetch + len(self.__qos_delayed)
                    )
                )
        self.__blocking_callback(lambda: self.__ack(delivery_tag=delivery_tag))

    def shutdown(self):
        self.__blocking_callback(lambda: self.__shutdown())

    def __ack(self, *, delivery_tag: int):
        self.__channel.basic_ack(delivery_tag=delivery_tag)

    def __shutdown(self):
        self.__channel.cancel()
        self.__connection.close()
