from collections import defaultdict
from threading import Lock

from qio.broker import Broker
from qio.message import Message
from qio.queue import Queue
from qio.queuespec import QueueSpec

from .receiver import StubReceiver


class StubBroker(Broker):
    def __init__(self):
        self.__queues = defaultdict[str, Queue[bytes]](Queue)
        self.__processing = set[Message]()
        self.__suspended = set[Message]()
        self.__receivers = set[StubReceiver]()
        self.__shutdown_lock = Lock()
        self.__shutdown = False

    @classmethod
    def from_uri(cls, uri: str, /):
        return cls()

    def enqueue(self, body: bytes, /, *, queue: str):
        self.__queues[queue].put(body)

    def purge(self, *, queue: str):
        # This doesn't account for active receivers
        self.__queues[queue] = Queue[bytes]()

    def receive(self, queuespec: QueueSpec, /) -> StubReceiver:
        if not queuespec.queues:
            raise ValueError("Must specify at least one queue")

        receiver = StubReceiver(
            queues=[self.__queues[queue] for queue in queuespec.queues],
            capacity=queuespec.concurrency,
        )
        self.__receivers.add(receiver)
        return receiver

    def shutdown(self):
        with self.__shutdown_lock:
            if self.__shutdown:
                return
            self.__shutdown = True

            # First notify all consumers to wake up from capacity waits
            for receiver in set(self.__receivers):
                receiver.shutdown()

            for queue in self.__queues.values():
                queue.shutdown(immediate=True)

            self.__queues.clear()
            self.__processing.clear()
            self.__suspended.clear()
            self.__receivers.clear()
