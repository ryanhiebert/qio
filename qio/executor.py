from collections.abc import Callable
from concurrent.futures import Executor as BaseExecutor
from concurrent.futures import Future
from itertools import count
from threading import Thread
from typing import Any


def annotate[T](**kwargs: Any) -> Callable[[T], T]:
    def decorator(fn: T) -> T:
        for k, v in kwargs.items():
            setattr(fn, k, v)
        return fn

    return decorator


class Executor(BaseExecutor):
    """Run each task in separate threads."""

    # For now, this just creates a new thread for each task.
    # ThreadPoolExecutor isn't used because it cannot cancel
    # running tasks even when shutting the process down.

    def __init__(self, *, name: str):
        self.__name = name
        self.__futures = set[Future[Any]]()
        self.__threads = set[Thread]()
        self.__counter = count(1).__next__

    @annotate(__doc__=BaseExecutor.submit.__doc__)
    def submit[T](
        self, fn: Callable[..., T], /, *args: Any, **kwargs: Any
    ) -> Future[T]:
        future = Future[T]()
        thread = Thread(
            target=self.__run,
            name=f"{self.__name}-{self.__counter()}",
            args=(future, fn, args, kwargs),
        )
        thread.start()
        self.__threads.add(thread)
        return future

    @annotate(__doc__=BaseExecutor.shutdown.__doc__)
    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False):
        if cancel_futures:
            for future in self.__futures:
                future.cancel()
        if wait:
            for thread in self.__threads:
                thread.join()

    def __run[T](
        self,
        future: Future[T],
        fn: Callable[..., T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ):
        if not future.set_running_or_notify_cancel():
            return

        try:
            result = fn(*args, **kwargs)
        except BaseException as exception:
            future.set_exception(exception)
        else:
            future.set_result(result)
