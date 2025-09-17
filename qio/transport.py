from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator


class Transport(ABC):
    @classmethod
    @abstractmethod
    def from_uri(cls, uri: str, /):
        """Create a transport instance from a URI."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def subscribe(self) -> Iterator[bytes]:
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def publish(self, message: bytes):
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError("Subclasses must implement this method.")
