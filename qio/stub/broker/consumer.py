import queue
import threading
from collections.abc import Iterator


class Consumer:
    def __init__(self, queue: queue.Queue[bytes], prefetch: int):
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
            except queue.ShutDown:
                return

    def ack(self):
        with self.__condition:
            self.__capacity += 1
            self.__condition.notify()

    def unack(self):
        with self.__condition:
            self.__capacity -= 1
