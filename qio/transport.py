from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator


class Transport(ABC):
    @abstractmethod
    def subscribe(self) -> Iterator[bytes]:
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def publish(self, message: bytes):
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError("Subclasses must implement this method.")
