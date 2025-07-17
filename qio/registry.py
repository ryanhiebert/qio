from collections.abc import Callable

from .routine import Routine

ROUTINE_REGISTRY: dict[str, Routine] = {}


def routine(*, name: str):
    """Decorate a function to make it a routine."""

    def create_routine[**A, R](fn: Callable[A, R]) -> Routine[A, R]:
        routine = Routine(fn, name=name)
        ROUTINE_REGISTRY.setdefault(routine.name, routine)
        assert ROUTINE_REGISTRY[routine.name] == routine
        return routine

    return create_routine
