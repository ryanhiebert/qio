from __future__ import annotations

from collections.abc import Callable
from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

from .id import random_id
from .routine import Routine


@dataclass(eq=False, kw_only=True)
class Invocation[T: Callable[..., Any] = Callable[..., Any]]:
    id: str = field(default_factory=random_id)
    routine: Routine[T]
    args: tuple[Any]
    kwargs: dict[str, Any]

    def run(self) -> Any:
        return self.routine.fn(*self.args, **self.kwargs)

    def __await__(self) -> Any:
        return cast(Any, (yield self))

    def __repr__(self):
        params_repr = ", ".join(
            (*map(repr, self.args), *(f"{k}={v!r}" for k, v in self.kwargs.items())),
        )
        return f"<{type(self).__name__} {self.id!r} {self.routine.name}({params_repr})>"


@dataclass(eq=True, kw_only=True)
class InvocationEvent:
    id: str = field(default_factory=random_id)
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    invocation: Invocation

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation!r}>"


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationEnqueued(InvocationEvent): ...


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationStarted(InvocationEvent): ...


@dataclass(eq=False, kw_only=True)
class InvocationSuspended(InvocationEvent):
    generator: Generator[Invocation, Any, Any]
    suspension: Invocation

    def __repr__(self):
        return (
            f"<{type(self).__name__} {self.invocation!r}"
            f" suspension={self.suspension!r}>"
        )


@dataclass(eq=False, kw_only=True)
class InvocationContinued(InvocationEvent):
    generator: Generator[Invocation, Any, Any]
    value: Any

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation!r} value={self.value!r}>"


@dataclass(eq=False, kw_only=True)
class InvocationResumed(InvocationEvent):
    value: Any

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation!r} value={self.value!r}>"


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationCompleted(InvocationEvent): ...


@dataclass(eq=False, kw_only=True)
class InvocationSucceeded(InvocationCompleted):
    value: Any

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation!r} value={self.value!r}>"


@dataclass(eq=False, kw_only=True)
class InvocationErrored(InvocationCompleted):
    exception: Exception

    def __repr__(self):
        return (
            f"<{type(self).__name__} {self.invocation!r} exception={self.exception!r}>"
        )
