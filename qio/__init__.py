from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .invocation import Invocation
from .routine import Routine


class InvocableRoutine[T: Callable[..., Any] = Callable[..., Any]](Routine[T]):
    def __call__(self, *args: Any, **kwargs: Any) -> Invocation[T]:
        return Invocation(routine=self, args=args, kwargs=kwargs)


def routine(*, name: str | None = None):
    """Decorate a function to make it a routine."""

    def create_routine[T: Callable[..., Any] = Callable[..., Any]](
        fn: T,
    ) -> InvocableRoutine[T]:
        return InvocableRoutine(fn, name=name or f"{fn.__module__}.{fn.__qualname__}")

    return create_routine
