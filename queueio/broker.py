from abc import ABC
from abc import abstractmethod

from .queuespec import QueueSpec
from .receiver import Receiver


class Broker(ABC):
    """A broker enables sending and receiving messages with a queue.

    Messages sent by a broker are assumed to be idempotent, and may be
    delivered multiple times in some conditions in order to ensure
    at-least-once delivery.
    """

    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str, /):
        """Create a broker instance from a URI."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def enqueue(self, body: bytes, /, *, queue: str):
        """Enqueue a message."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def create(self, *, queue: str):
        """Create a queue if it doesn't exist."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def purge(self, *, queue: str):
        """Purge all messages from the queue."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def receive(self, queuespec: QueueSpec, /) -> Receiver:
        """Receive messages as specified by the QueueSpec."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def shutdown(self):
        """Signal the final shutdown of the broker."""
        raise NotImplementedError("Subclasses must implement this method.")
