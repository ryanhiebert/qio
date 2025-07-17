from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Generator
from functools import wraps
from typing import Any

from .suspension import Suspension


class Suspendable[R](Awaitable[R]):
    def __init__(self, generator: Generator[Suspension[R], Any, R]):
        self.__generator = generator

    def __await__(self) -> Generator[Suspension[R], Any, R]:
        return (yield from self.__generator)


def suspendable[**A, R](fn: Callable[A, Generator[Suspension[R], Any, R]]):
    """Decorate a generator function to return a suspend."""

    @wraps(fn)
    def wrapper(*args: A.args, **kwargs: A.kwargs) -> Suspendable[R]:
        return Suspendable(fn(*args, **kwargs))

    return wrapper
