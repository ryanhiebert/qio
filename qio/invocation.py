import json
from collections.abc import Callable
from collections.abc import Generator
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self

from .event import Event
from .id import random_id
from .suspension import Suspension
from .suspension import SuspensionCompleted
from .suspension import SuspensionErrored
from .suspension import SuspensionSubmitted
from .suspension import SuspensionSucceeded


@dataclass(eq=False, kw_only=True)
class Invocation[R](Suspension[R]):
    id: str = field(default_factory=random_id)
    routine: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    __handler = ContextVar[Callable[[Self], Future] | None](
        "Invocation.handler", default=None
    )

    @classmethod
    @contextmanager
    def handler(cls, handler: Callable[[Self], Future]):
        token = cls.__handler.set(handler)
        try:
            yield
        finally:
            cls.__handler.reset(token)

    def __repr__(self):
        params_repr = ", ".join(
            (*map(repr, self.args), *(f"{k}={v!r}" for k, v in self.kwargs.items())),
        )
        return f"<{type(self).__name__} {self.id!r} {self.routine}({params_repr})>"

    def __await__(self):
        return (yield self)

    def start(self) -> Future[R]:
        handler = self.__handler.get()
        if handler is None:
            raise RuntimeError("No invocation runner configured.")
        return handler(self)


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


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationSubmitted(SuspensionSubmitted):
    routine: str
    args: tuple[Any]
    kwargs: dict[str, Any]


@dataclass(eq=False, kw_only=True)
class InvocationStarted(Event): ...


@dataclass(eq=False, kw_only=True)
class BaseInvocationSuspended(Event): ...


@dataclass(eq=False, kw_only=True)
class InvocationSuspended(BaseInvocationSuspended): ...


@dataclass(eq=False, kw_only=True)
class LocalInvocationSuspended(BaseInvocationSuspended):
    suspension: Suspension = field(repr=False)
    generator: Generator[Invocation, Any, Any] = field(repr=False)
    invocation: Invocation = field(repr=False)


@dataclass(eq=False, kw_only=True)
class BaseInvocationContinued(Event):
    value: Any


@dataclass(eq=False, kw_only=True)
class InvocationContinued(BaseInvocationContinued): ...


@dataclass(eq=False, kw_only=True)
class LocalInvocationContinued(BaseInvocationContinued):
    generator: Generator[Suspension, Any, Any] = field(repr=False)


@dataclass(eq=False, kw_only=True)
class BaseInvocationThrew(Event):
    exception: Exception


@dataclass(eq=False, kw_only=True)
class InvocationThrew(BaseInvocationThrew): ...


@dataclass(eq=False, kw_only=True)
class LocalInvocationThrew(BaseInvocationThrew):
    generator: Generator[Suspension, Any, Any] = field(repr=False)


@dataclass(eq=False, kw_only=True)
class InvocationResumed(Event): ...


@dataclass(eq=False, kw_only=True)
class InvocationCompleted(SuspensionCompleted): ...


@dataclass(eq=False, kw_only=True)
class InvocationSucceeded(SuspensionSucceeded): ...


@dataclass(eq=False, kw_only=True)
class InvocationErrored(SuspensionErrored): ...
