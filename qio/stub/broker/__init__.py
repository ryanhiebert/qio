import queue
from collections.abc import Iterator

from qio.broker import Broker
from qio.broker import Message

from .consumer import Consumer


class StubBroker(Broker):
    def __init__(self):
        self.__queue = queue.Queue[bytes]()
        self.__processing = set[Message]()
        self.__suspended = set[Message]()
        self.__consumers: dict[Message, Consumer] = {}

    def enqueue(self, body: bytes, /):
        self.__queue.put(body)

    def purge(self):
        self.__queue = queue.Queue[bytes]()

    def consume(self, *, prefetch: int) -> Iterator[Message]:
        consumer = Consumer(self.__queue, prefetch)

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
        self.__queue.shutdown(immediate=True)
        self.__processing.clear()
        self.__suspended.clear()
        self.__consumers.clear()
