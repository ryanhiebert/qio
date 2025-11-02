import threading
from collections.abc import Iterator

from qio.journal import Journal
from qio.queue import Queue
from qio.queue import ShutDown


class StubJournal(Journal):
    """An in-memory journal implementation for testing."""

    def __init__(self):
        self.__queue = Queue[bytes]()
        self.__shutdown_lock = threading.Lock()
        self.__shutdown = False

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
        with self.__shutdown_lock:
            if self.__shutdown:
                return
            self.__shutdown = True
            self.__queue.shutdown()
