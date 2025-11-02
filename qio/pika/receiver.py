from collections.abc import Iterator
from threading import Lock
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
        self.__prefetch_lock = Lock()
        self.__prefetch = prefetch
        self.__channel.basic_qos(prefetch_count=prefetch, global_qos=True)
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
        with self.__prefetch_lock:
            self.__prefetch += 1
            prefetch = self.__prefetch  # Memo for the lambda
            self.__connection.add_callback_threadsafe(
                lambda: self.__channel.basic_qos(prefetch_count=prefetch)
            )

    def unpause(self, message: Message, /):
        """Unpause processing of a message.

        The previously paused message processing is resuming, so its assigned
        capacity is no longer available for allocation elsewhere.
        """
        with self.__prefetch_lock:
            self.__prefetch -= 1
            prefetch = self.__prefetch  # Memo for the lambda
            self.__connection.add_callback_threadsafe(
                lambda: self.__channel.basic_qos(prefetch_count=prefetch)
            )

    def finish(self, message: Message, /):
        """Finish processing a message.

        The message is done processing, and its assigned capacity may be
        allocated elsewhere permanently.
        """
        self.__connection.add_callback_threadsafe(
            lambda: self.__channel.basic_ack(delivery_tag=self.__tag.pop(message))
        )

    def shutdown(self):
        self.__connection.add_callback_threadsafe(self.__shutdown)

    def __shutdown(self):
        self.__channel.cancel()
        self.__connection.close()
