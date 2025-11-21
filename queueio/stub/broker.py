from threading import Lock

from queueio.broker import Broker
from queueio.message import Message
from queueio.queue import Queue
from queueio.queuespec import QueueSpec

from .receiver import StubReceiver


class StubBroker(Broker):
    def __init__(self):
        self.__queues = dict[str, Queue[bytes]]()
        self.__processing = set[Message]()
        self.__suspended = set[Message]()
        self.__receivers = set[StubReceiver]()
        self.__shutdown_lock = Lock()
        self.__shutdown = False

    @classmethod
    def from_uri(cls, uri: str, /):
        return cls()

    def enqueue(self, body: bytes, /, *, queue: str):
        if queue not in self.__queues:
            raise ValueError(f"Queue '{queue}' does not exist")
        self.__queues[queue].put(body)

    def create(self, *, queue: str):
        self.__queues[queue] = Queue[bytes]()

    def purge(self, *, queue: str):
        if queue not in self.__queues:
            raise ValueError(f"Queue '{queue}' does not exist")
        # This doesn't account for active receivers
        self.__queues[queue] = Queue[bytes]()

    def receive(self, queuespec: QueueSpec, /) -> StubReceiver:
        if not queuespec.queues:
            raise ValueError("Must specify at least one queue")

        missing_queues = [q for q in queuespec.queues if q not in self.__queues]
        if missing_queues:
            raise ValueError(f"Queues do not exist: {missing_queues}")

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
