from abc import abstractmethod
from collections.abc import Awaitable
from collections.abc import Generator
from concurrent.futures import Future
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self

from .event import Event
from .id import random_id


@dataclass(eq=False, kw_only=True)
class Suspension[R](Awaitable[R]):
    """Base class for all suspension types in the system."""

    id: str = field(default_factory=random_id)

    @abstractmethod
    def start(self) -> Future[R]:
        raise NotImplementedError("Subclasses must implement this method.")

    def __await__(self) -> Generator[Self, R, R]:
        return (yield self)


@dataclass(eq=False, kw_only=True)
class SuspensionSubmitted(Event): ...


@dataclass(eq=False, kw_only=True)
class SuspensionCompleted(Event): ...


@dataclass(eq=False, kw_only=True)
class SuspensionSucceeded(SuspensionCompleted):
    value: Any = field(repr=False)


@dataclass(eq=False, kw_only=True)
class SuspensionErrored(SuspensionCompleted):
    exception: Exception = field(repr=False)
