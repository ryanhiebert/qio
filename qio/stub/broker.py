import threading
from collections import defaultdict
from collections import deque
from collections.abc import Iterable
from collections.abc import Iterator
from random import randrange

from qio.broker import Broker
from qio.broker import Message
from qio.queue import Queue
from qio.queue import ShutDown
from qio.queuespec import QueueSpec
from qio.select import select


class StubBroker(Broker):
    def __init__(self):
        self.__queues = defaultdict[str, Queue[bytes]](Queue)
        self.__processing = set[Message]()
        self.__suspended = set[Message]()
        self.__consumers: dict[Message, _Consumer] = {}
        self.__shutdown_lock = threading.Lock()
        self.__shutdown = False

    @classmethod
    def from_uri(cls, uri: str, /):
        return cls()

    def enqueue(self, body: bytes, /, *, queue: str):
        self.__queues[queue].put(body)

    def purge(self, *, queue: str):
        self.__queues[queue] = Queue[bytes]()

    def consume(self, queuespec: QueueSpec, /) -> Iterator[Message]:
        if not queuespec.queues:
            raise ValueError("QueueSpec must have at least one queue")

        queues = [self.__queues[queue] for queue in queuespec.queues]
        consumer = _Consumer(queues, queuespec.concurrency)

        for payload in consumer:
            message = Message(body=payload)
            self.__consumers[message] = consumer
            yield message

    def start(self, message: Message, /):
        self.__processing.add(message)

    def suspend(self, message: Message, /):
        self.__suspended.add(message)
        self.__processing.discard(message)
        consumer = self.__consumers.get(message)
        if consumer:
            consumer.ack()

    def unsuspend(self, message: Message, /):
        if message in self.__suspended:
            self.__suspended.discard(message)
            self.__processing.add(message)
            consumer = self.__consumers.get(message)
            if consumer:
                consumer.unack()

    def complete(self, message: Message, /):
        self.__processing.discard(message)
        self.__suspended.discard(message)
        consumer = self.__consumers.pop(message, None)
        if consumer:
            consumer.ack()

    def shutdown(self):
        with self.__shutdown_lock:
            if self.__shutdown:
                return
            self.__shutdown = True

            # First notify all consumers to wake up from capacity waits
            for consumer in set(self.__consumers.values()):
                consumer.shutdown()

            for q in self.__queues.values():
                q.shutdown(immediate=True)
            self.__queues.clear()
            self.__processing.clear()
            self.__suspended.clear()
            self.__consumers.clear()


class _Consumer:
    def __init__(self, queues: Iterable[Queue[bytes]], prefetch: int):
        self.__queues = deque(queues)
        self.__capacity = prefetch
        # A bounded semaphore won't work because resume()
        # needs to decrease capacity without blocking
        self.__condition = threading.Condition()
        self.__shutdown = False

        # Randomize the starting position
        for _ in range(randrange(len(self.__queues))):
            self.__queues.append(self.__queues.popleft())

    def __iter__(self) -> Iterator[bytes]:
        while True:
            # Wait for capacity to consume a message
            with self.__condition:
                while self.__capacity <= 0 and not self.__shutdown:
                    self.__condition.wait()
                if self.__shutdown:
                    return
                self.__capacity -= 1
            try:
                i, value = select([queue.get.select() for queue in self.__queues])
            except ShutDown:
                return

            # Cycle past the one that succeeded to do a fair round-robin
            for _ in range(i + 1):
                self.__queues.append(self.__queues.popleft())
            yield value

    def ack(self):
        with self.__condition:
            self.__capacity += 1
            self.__condition.notify()

    def unack(self):
        with self.__condition:
            self.__capacity -= 1

    def shutdown(self):
        with self.__condition:
            self.__shutdown = True
            self.__condition.notify_all()
