from abc import ABC
from abc import abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from dataclasses import field

from .id import random_id


@dataclass(eq=False, kw_only=True)
class Suspension[R](ABC):
    """Base class for all suspension types in the system."""

    id: str = field(default_factory=random_id)

    @abstractmethod
    def start(self) -> Future[R]:
        raise NotImplementedError("Subclasses must implement this method.")
