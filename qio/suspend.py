from collections.abc import Awaitable
from collections.abc import Callable
from concurrent.futures import Future
from functools import partial
from typing import Any

from .suspendable import Suspendable
from .suspension import Suspension
from .thread import Thread


class CancelledError(BaseException):
    """A running suspendable has been cancelled."""


class Suspend[R](Suspendable[R]):
    def __init__(self, awaitable: Awaitable[R]):
        self.__awaitable = awaitable
        self.__thread = Thread(target=self.__run)
        self.__future = Future[R]()
        self.__future.add_done_callback(self.__done_callback)
        self.__cancelled = False

    def start(self) -> Future[R]:
        self.__thread.start()
        return self.__future

    def __done_callback(self, future: Future[R]) -> None:
        if future.cancelled():
            self.__cancelled = True

    def __run(self):
        generator = self.__awaitable.__await__()
        next_step: Callable[[], Any] = partial(generator.send, None)

        while True:
            if self.__cancelled:
                # Our future has been cancelled.
                next_step = partial(generator.throw, CancelledError())

            try:
                suspension = next_step()
            except StopIteration as e:
                self.__future.set_result(e.value)
            except Exception as e:
                self.__future.set_exception(e)
            else:
                try:
                    next_step = partial(generator.send, suspension.start().result())
                except BaseException as e:
                    next_step = partial(generator.throw, e)


def suspend[R](awaitable: Awaitable[R]) -> Suspension[R]:
    """Use a thread to convert an awaitable into a suspension."""
    return awaitable if isinstance(awaitable, Suspension) else Suspend(awaitable)
