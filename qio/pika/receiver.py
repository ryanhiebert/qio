from collections.abc import Callable
from collections.abc import Iterator
from threading import Lock
from typing import Any
from typing import cast

from pika import BlockingConnection
from pika import ConnectionParameters
from pika import URLParameters

from qio.message import Message
from qio.receiver import Receiver


class PikaReceiver(Receiver):
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
        self.__tag = dict[Message, int]()
        self.__suspended = set[Message]()

    def __iter__(self) -> Iterator[Message]:
        for method, _, body in self.__iterator:
            message = Message(body)
            tag = cast(int, method.delivery_tag)
            self.__tag[message] = tag
            yield message

    def pause(self, message: Message, /):
        """Pause processing of a message.

        The message processing is not completed, and is expected to unpause,
        but its assigned capacity may be allocated elsewhere temporarily.
        """
        if message in self.__suspended:
            # Already acked previously
            return

        # Can't change prefetch window dynamically on the consumer
        self.__suspended.add(message)
        self.__blocking_callback(lambda: self.__ack(delivery_tag=self.__tag[message]))

    def unpause(self, message: Message, /):
        """Unpause processing of a message.

        The previously paused message processing is resuming, so its assigned
        capacity is no longer available for allocation elsewhere.
        """
        pass  # Message has already been acked, do nothing.

    def finish(self, message: Message, /):
        """Finish processing a message.

        The message is done processing, and its assigned capacity may be
        allocated elsewhere permanently.
        """
        if message in self.__suspended:
            self.__suspended.remove(message)
            return  # Message has already been acked, do nothing.
        self.__blocking_callback(
            lambda: self.__ack(delivery_tag=self.__tag.pop(message))
        )

    def shutdown(self):
        self.__blocking_callback(lambda: self.__shutdown())

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

    def __ack(self, *, delivery_tag: int):
        self.__channel.basic_ack(delivery_tag=delivery_tag)

    def __shutdown(self):
        self.__channel.cancel()
        self.__connection.close()
