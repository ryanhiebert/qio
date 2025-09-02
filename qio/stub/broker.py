import threading
from collections import defaultdict
from collections.abc import Iterator
from queue import Queue
from queue import ShutDown

from qio.broker import Broker
from qio.broker import Message


class StubBroker(Broker):
    def __init__(self):
        self.__queues = defaultdict[str, Queue[bytes]](Queue)
        self.__processing = set[Message]()
        self.__suspended = set[Message]()
        self.__consumers: dict[Message, _Consumer] = {}

    def enqueue(self, body: bytes, /, *, queue: str):
        self.__queues[queue].put(body)

    def purge(self, *, queue: str):
        self.__queues[queue] = Queue[bytes]()

    def consume(self, *, queue: str, prefetch: int) -> Iterator[Message]:
        consumer = _Consumer(self.__queues[queue], prefetch)

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

    def resume(self, message: Message, /):
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
        for q in self.__queues.values():
            q.shutdown(immediate=True)
        self.__queues.clear()
        self.__processing.clear()
        self.__suspended.clear()
        self.__consumers.clear()


class _Consumer:
    def __init__(self, queue: Queue[bytes], prefetch: int):
        self.__queue = queue
        self.__capacity = prefetch
        # A bounded semaphore won't work because resume()
        # needs to decrease capacity without blocking
        self.__condition = threading.Condition()

    def __iter__(self) -> Iterator[bytes]:
        while True:
            # Wait for capacity to consume a message
            with self.__condition:
                while self.__capacity <= 0:
                    self.__condition.wait()
                self.__capacity -= 1
            try:
                yield self.__queue.get()
            except ShutDown:
                return

    def ack(self):
        with self.__condition:
            self.__capacity += 1
            self.__condition.notify()

    def unack(self):
        with self.__condition:
            self.__capacity -= 1
