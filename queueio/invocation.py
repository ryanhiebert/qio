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

    def serialize(self) -> bytes:
        return json.dumps(
            {
                "id": self.id,
                "routine": self.routine,
                "args": self.args,
                "kwargs": self.kwargs,
            }
        ).encode()

    @classmethod
    def deserialize(cls, serialized: bytes) -> Self:
        data = json.loads(serialized.decode())
        return cls(
            id=data["id"],
            routine=data["routine"],
            args=data["args"],
            kwargs=data["kwargs"],
        )

    @dataclass(eq=False, kw_only=True, repr=False)
    class Submitted(Suspension.Submitted):
        routine: str
        args: tuple[Any]
        kwargs: dict[str, Any]

    @dataclass(eq=False, kw_only=True)
    class Started(Event): ...

    @dataclass(eq=False, kw_only=True)
    class BaseSuspended(Event): ...

    @dataclass(eq=False, kw_only=True)
    class Suspended(BaseSuspended): ...

    @dataclass(eq=False, kw_only=True)
    class LocalSuspended(BaseSuspended):
        suspension: Suspension = field(repr=False)
        generator: Generator[Invocation, Any, Any] = field(repr=False)
        invocation: Invocation = field(repr=False)

    @dataclass(eq=False, kw_only=True)
    class BaseContinued(Event):
        value: Any

    @dataclass(eq=False, kw_only=True)
    class Continued(BaseContinued): ...

    @dataclass(eq=False, kw_only=True)
    class LocalContinued(BaseContinued):
        generator: Generator[Suspension, Any, Any] = field(repr=False)

    @dataclass(eq=False, kw_only=True)
    class BaseThrew(Event):
        exception: Exception

    @dataclass(eq=False, kw_only=True)
    class Threw(BaseThrew): ...

    @dataclass(eq=False, kw_only=True)
    class LocalThrew(BaseThrew):
        generator: Generator[Suspension, Any, Any] = field(repr=False)

    @dataclass(eq=False, kw_only=True)
    class Resumed(Event): ...

    @dataclass(eq=False, kw_only=True)
    class Completed(Suspension.Completed): ...
