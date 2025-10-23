from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator

from .message import Message
from .queuespec import QueueSpec


class Broker(ABC):
    """A broker enables producing and consuming messages on a queue.

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
    def purge(self, *, queue: str):
        """Purge all messages from the queue."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def consume(self, queuespec: QueueSpec, /) -> Iterator[Message]:
        """Consume messages from the queue."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def start(self, _: Message, /):
        """Report that processing of a message has started."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def suspend(self, message: Message, /):
        """Report that the processing of a message has been suspended.

        The message is not completed, and is expected to resume.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def unsuspend(self, message: Message, /):
        """Report that the processing of a message has been unsuspended.

        It is expected to resume shortly.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def complete(self, message: Message, /):
        """Report that the processing of a message has completed.

        When complete, no other worker will need to process the message again.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def shutdown(self):
        """Signal the final shutdown of the broker."""
        raise NotImplementedError("Subclasses must implement this method.")
