from collections.abc import Iterator
from queue import Queue
from queue import ShutDown

from qio.transport import Transport


class StubTransport(Transport):
    """An in-memory transport implementation for testing."""

    def __init__(self):
        self.__queue = Queue[bytes]()

    def subscribe(self) -> Iterator[bytes]:
        while True:
            try:
                yield self.__queue.get()
            except ShutDown:
                return

    def publish(self, message: bytes):
        self.__queue.put(message)

    def shutdown(self):
        self.__queue.shutdown()
