from collections.abc import Iterator
from threading import Lock
from typing import cast

from qio.message import Message
from qio.queuespec import QueueSpec
from qio.receiver import Receiver

from .threadsafe import ThreadsafeConnection


class PikaReceiver(Receiver):
    def __init__(
        self,
        connection: ThreadsafeConnection,
        queuespec: QueueSpec,
        /,
    ):
        if len(queuespec.queues) == 0:
            raise ValueError("Must specify at least one queue")

        self.__channel = connection.channel()
        self.__consumer_tag = dict[str, str]()
        self.__tag = dict[Message, int]()

        self.__prefetch_lock = Lock()
        self.__prefetch = 0
        self.__adjust_prefetch(+queuespec.concurrency)

        declared_queues = set()
        for queue in queuespec.queues:
            if queue not in declared_queues:
                self.__channel.declare_queue(queue=queue, durable=True)
                declared_queues.add(queue)
            result = self.__channel.consume(queue)
            self.__consumer_tag[queue] = cast(str, result.method.consumer_tag)

    def __adjust_prefetch(self, change: int) -> None:
        with self.__prefetch_lock:
            self.__prefetch += change
            self.__channel.qos(prefetch_count=self.__prefetch, global_qos=True)

    def __iter__(self) -> Iterator[Message]:
        for method, _, body in self.__channel.messages():
            message = Message(body)
            tag = cast(int, method.delivery_tag)
            self.__tag[message] = tag
            yield message

    def pause(self, message: Message, /):
        """Pause processing of a message.

        The message processing is not completed, and is expected to unpause,
        but its assigned capacity may be allocated elsewhere temporarily.
        """
        self.__adjust_prefetch(+1)

    def unpause(self, message: Message, /):
        """Unpause processing of a message.

        The previously paused message processing is resuming, so its assigned
        capacity is no longer available for allocation elsewhere.
        """
        self.__adjust_prefetch(-1)

    def finish(self, message: Message, /):
        """Finish processing a message.

        The message is done processing, and its assigned capacity may be
        allocated elsewhere permanently.
        """
        self.__channel.ack(delivery_tag=self.__tag.pop(message))

    def shutdown(self):
        for consumer_tag in self.__consumer_tag.values():
            self.__channel.cancel(consumer_tag=consumer_tag)
