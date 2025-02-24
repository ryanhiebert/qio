from __future__ import annotations

from collections.abc import Callable
from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .id import random_id
from .invocation import Invocation


@dataclass(eq=False, kw_only=True)
class Continuation[T: Callable[..., Any] = Callable[..., Any]]:
    id: str = field(default_factory=random_id)
    invocation: Invocation[T]
    generator: Generator[Invocation[T], Any, Any]


@dataclass(eq=False, kw_only=True)
class SendContinuation(Continuation):
    value: Any

    def send(self) -> Any:
        return self.generator.send(self.value)


@dataclass(eq=False, kw_only=True)
class ThrowContinuation(Continuation):
    exception: Exception

    def throw(self) -> Any:
        return self.generator.throw(self.exception)
