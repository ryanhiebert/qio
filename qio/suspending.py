from collections.abc import Callable
from concurrent.futures import Future
from functools import wraps

from .suspendable import Suspendable
from .suspension import Suspension


class Suspending[R](Suspendable[R]):
    def __init__(self, suspension: Suspension[R]):
        self.__suspension = suspension

    def start(self) -> Future[R]:
        return self.__suspension.start()


def suspending[**A, R](fn: Callable[A, Suspension[R]]):
    """Decorate a generator function to return a suspendable."""

    @wraps(fn)
    def wrapper(*args: A.args, **kwargs: A.kwargs) -> Suspending[R]:
        return Suspending(fn(*args, **kwargs))

    return wrapper
