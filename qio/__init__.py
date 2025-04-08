from collections.abc import Callable
from typing import Any
from typing import cast

from .invocation import Invocation
from .registry import ROUTINE_REGISTRY
from .routine import Routine


class InvocableRoutine[T: Callable[..., Any] = Callable[..., Any]](Routine[T]):
    def __call__(self, *args: Any, **kwargs: Any) -> Invocation[T]:
        return Invocation(routine=self, args=args, kwargs=kwargs)


def routine(*, name: str | None = None):
    """Decorate a function to make it a routine."""

    def create_routine[T: Callable[..., Any] = Callable[..., Any]](
        fn: T,
    ) -> InvocableRoutine[T]:
        routine = InvocableRoutine(fn, name=name or fn.__name__)
        ROUTINE_REGISTRY.setdefault(routine.name, cast(Routine, routine))
        assert ROUTINE_REGISTRY[routine.name] == routine
        return routine

    return create_routine
