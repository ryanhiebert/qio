import json
from collections.abc import Callable
from collections.abc import Generator
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import Self

from .id import random_id
from .suspendable import Suspendable
from .suspension import Suspension


@dataclass(eq=False, kw_only=True)
class Invocation[R](Suspendable[R], Suspension[R]):
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
class BaseInvocationSuspended(InvocationEvent): ...


@dataclass(eq=False, kw_only=True, repr=False)
class InvocationSuspended(BaseInvocationSuspended): ...


@dataclass(eq=False, kw_only=True, repr=False)
class LocalInvocationSuspended(BaseInvocationSuspended):
    suspension: Suspension
    generator: Generator[Invocation, Any, Any]
    invocation: Invocation

    def __repr__(self):
        return (
            f"<{type(self).__name__} {self.invocation_id}"
            f" suspension={self.suspension!r}>"
        )


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
