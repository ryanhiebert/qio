import json
from collections.abc import Callable
from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

from .id import random_id
from .registry import ROUTINE_REGISTRY
from .suspension import Suspension

INVOCATION_QUEUE_NAME = "qio"


@dataclass(eq=False, kw_only=True)
class Invocation:
    id: str = field(default_factory=random_id)
    routine: str
    args: tuple[Any]
    kwargs: dict[str, Any]

    def run(self) -> Any:
        return ROUTINE_REGISTRY[self.routine].fn(*self.args, **self.kwargs)

    def __await__(self) -> Any:
        return cast(Any, (yield InvocationSuspension(invocation=self)))

    def __repr__(self):
        params_repr = ", ".join(
            (*map(repr, self.args), *(f"{k}={v!r}" for k, v in self.kwargs.items())),
        )
        return f"<{type(self).__name__} {self.id!r} {self.routine}({params_repr})>"


@dataclass(eq=False, kw_only=True)
class InvocationSuspension[T: Callable[..., Any] = Callable[..., Any]](Suspension):
    """A suspension that waits on an invocation to complete."""

    invocation: Invocation

    def __repr__(self):
        return f"<{type(self).__name__} {self.id} {self.invocation!r}>"


def serialize(invocation: Invocation, /) -> bytes:
    return json.dumps(
        {
            "id": invocation.id,
            "routine": invocation.routine,
            "args": invocation.args,
            "kwargs": invocation.kwargs,
        }
    ).encode()


def deserialize(serialized: bytes, /) -> Invocation:
    data = json.loads(serialized.decode())
    return Invocation(
        id=data["id"],
        routine=data["routine"],
        args=data["args"],
        kwargs=data["kwargs"],
    )


@dataclass(eq=False, kw_only=True)
class InvocationEvent:
    id: str = field(default_factory=random_id)
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    invocation_id: str

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation_id}>"


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationSubmitted(InvocationEvent):
    routine: str
    args: tuple[Any]
    kwargs: dict[str, Any]

    def __repr__(self):
        params_repr = ", ".join(
            (*map(repr, self.args), *(f"{k}={v!r}" for k, v in self.kwargs.items())),
        )
        return (
            f"<{type(self).__name__} {self.invocation_id} "
            f"{self.routine}({params_repr})>"
        )


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationStarted(InvocationEvent): ...


@dataclass(eq=False, kw_only=True)
class BaseInvocationSuspended(InvocationEvent):
    suspension: Suspension

    def __repr__(self):
        return (
            f"<{type(self).__name__} {self.invocation_id}"
            f" suspension={self.suspension!r}>"
        )


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationSuspended(BaseInvocationSuspended): ...


@dataclass(eq=False, kw_only=True, repr=False)
class LocalInvocationSuspended(BaseInvocationSuspended):
    generator: Generator[Invocation, Any, Any]
    invocation: Invocation


@dataclass(eq=False, kw_only=True)
class BaseInvocationContinued(InvocationEvent):
    value: Any

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation_id} value={self.value!r}>"


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationContinued(BaseInvocationContinued): ...


@dataclass(eq=False, kw_only=True, repr=False)
class LocalInvocationContinued(BaseInvocationContinued):
    generator: Generator[Invocation, Any, Any]


@dataclass(eq=False, kw_only=True)
class BaseInvocationThrew(InvocationEvent):
    exception: Exception

    def __repr__(self):
        return (
            f"<{type(self).__name__} {self.invocation_id} exception={self.exception!r}>"
        )


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationThrew(BaseInvocationThrew): ...


@dataclass(eq=False, kw_only=True, repr=False)
class LocalInvocationThrew(BaseInvocationThrew):
    generator: Generator[Invocation, Any, Any]


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationResumed(InvocationEvent): ...


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationCompleted(InvocationEvent): ...


@dataclass(eq=False, kw_only=True)
class InvocationSucceeded(InvocationCompleted):
    value: Any

    def __repr__(self):
        return f"<{type(self).__name__} {self.invocation_id} value={self.value!r}>"


@dataclass(eq=False, kw_only=True)
class InvocationErrored(InvocationCompleted):
    exception: Exception

    def __repr__(self):
        return (
            f"<{type(self).__name__} {self.invocation_id} exception={self.exception!r}>"
        )
