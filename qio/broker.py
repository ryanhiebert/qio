from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(eq=False, frozen=True)
class Message:
    """A message from a broker."""

    body: bytes


class Broker(ABC):
    """A broker enables producing and consuming messages on a queue."""

    @abstractmethod
    def enqueue(self, body: bytes, /):
        """Enqueue a message."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def consume(self, *, prefetch: int) -> Iterator[Message]:
        """Consume messages from the queue."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def start(self, _: Message, /):
        """Start processing a message."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def suspend(self, message: Message, /):
        """Report that the processing of a message has been suspended."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def resume(self, message: Message, /):
        """Report that the processing of a message has resumed."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def complete(self, message: Message, /):
        """Report that the processing of a message has completed."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def shutdown(self):
        """Signal the final shutdown of the broker."""
        raise NotImplementedError("Subclasses must implement this method.")
