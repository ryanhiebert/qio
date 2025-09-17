from collections.abc import Iterator

from qio.queue import Queue
from qio.queue import ShutDown
from qio.transport import Transport


class StubTransport(Transport):
    """An in-memory transport implementation for testing."""

    def __init__(self):
        self.__queue = Queue[bytes]()

    @classmethod
    def from_uri(cls, uri: str, /):
        return cls()

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
