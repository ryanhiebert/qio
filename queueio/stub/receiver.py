from collections import deque
from collections.abc import Iterable
from collections.abc import Iterator
from random import randrange
from threading import Condition

from queueio.message import Message
from queueio.queue import Queue
from queueio.queue import ShutDown
from queueio.receiver import Receiver
from queueio.select import select


class StubReceiver(Receiver):
    def __init__(self, *, queues: Iterable[Queue[bytes]], capacity: int):
        self.__queues = deque(queues)
        self.__capacity = capacity
        # A bounded semaphore won't work because resume()
        # needs to decrease capacity without blocking
        self.__condition = Condition()
        self.__shutdown = False

        # Randomize the starting position
        with self.__condition:
            for _ in range(randrange(len(self.__queues))):
                self.__queues.append(self.__queues.popleft())

    def __iter__(self) -> Iterator[Message]:
        while True:
            # Wait for capacity to consume a message
            with self.__condition:
                while self.__capacity <= 0 and not self.__shutdown:
                    self.__condition.wait()
                if self.__shutdown:
                    return

                # Get the queue order with the condition lock
                queues = list(self.__queues)
                self.__capacity -= 1

            # Wait for a new message without the condition lock
            try:
                i, value = select([queue.get.select() for queue in queues])
            except ShutDown:
                return

            with self.__condition:
                # Cycle past the one that succeeded to do a fair round-robin
                for _ in range(i + 1):
                    self.__queues.append(self.__queues.popleft())
                yield Message(body=value)

    def pause(self, message: Message, /):
        with self.__condition:
            self.__capacity += 1
            self.__condition.notify()

    def unpause(self, message: Message, /):
        with self.__condition:
            self.__capacity -= 1

    def finish(self, message: Message, /):
        with self.__condition:
            self.__capacity += 1
            self.__condition.notify()

    def shutdown(self):
        with self.__condition:
            self.__shutdown = True
            self.__condition.notify_all()
