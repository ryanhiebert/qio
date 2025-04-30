from collections.abc import Callable
from concurrent.futures import Future
from threading import Thread as BaseThread
from typing import Any


class Thread[T](BaseThread):
    """A Thread that has a future to gather its result."""

    def __init__(
        self,
        target: Callable[..., T],
        *,
        name: str | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        daemon: bool | None = None,
    ) -> None:
        super().__init__(
            target=self._run,
            name=name,
            args=(target, args, kwargs or {}),
            daemon=daemon,
        )
        self._future = Future[T]()

    def _run(
        self,
        target: Callable[..., T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if not self._future.set_running_or_notify_cancel():
            return

        try:
            result = target(*args, **kwargs)
        except BaseException as exception:
            self._future.set_exception(exception)
        else:
            self._future.set_result(result)

    @property
    def future(self) -> Future[T]:
        """Get the Future associated with this thread."""
        return self._future
