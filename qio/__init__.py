from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .invocation import Invocation
from .routine import Routine


class InvocableRoutine(Routine):
    def __call__(self, *args: Any, **kwargs: Any) -> Invocation:
        return Invocation(routine=self, args=args, kwargs=kwargs)


def routine(*, name: str | None = None):
    """Decorate a function to make it a routine."""

    def create_routine(fn: Callable[..., Any]) -> InvocableRoutine:
        return InvocableRoutine(fn, name=name or f"{fn.__module__}.{fn.__qualname__}")

    return create_routine