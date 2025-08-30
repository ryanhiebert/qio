from collections.abc import Callable
from collections.abc import Generator
from functools import wraps
from typing import Any

from .suspendable import Suspendable
from .suspension import Suspension


class Suspending[R](Suspendable[R]):
    def __init__(self, generator: Generator[Suspension[R], Any, R]):
        self.__generator = generator

    def __await__(self) -> Generator[Suspension[R], Any, R]:
        return (yield from self.__generator)


def suspending[**A, R](fn: Callable[A, Generator[Suspension[R], Any, R]]):
    """Decorate a generator function to return a suspendable."""

    @wraps(fn)
    def wrapper(*args: A.args, **kwargs: A.kwargs) -> Suspending[R]:
        return Suspending(fn(*args, **kwargs))

    return wrapper
