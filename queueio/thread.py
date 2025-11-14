from collections.abc import Callable
from concurrent.futures import Future
from contextvars import copy_context
from threading import Thread as BaseThread
from typing import Any


class Thread[T](BaseThread):
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
        self.__future = Future[T]()

    def start(self) -> None:
        if not hasattr(self, "__context"):
            self.__context = copy_context()
        super().start()

    def _run(
        self,
        target: Callable[..., T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if not self.__future.set_running_or_notify_cancel():
            return

        try:
            result = self.__context.run(target, *args, **kwargs)
        except BaseException as exception:
            self.__future.set_exception(exception)
        else:
            self.__future.set_result(result)

    @property
    def future(self) -> Future[T]:
        """Get the Future associated with this thread."""
        return self.__future
