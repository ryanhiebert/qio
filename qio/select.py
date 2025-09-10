from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from threading import Condition
from threading import Lock
from typing import cast


class Selector[R = None](ABC):
    @abstractmethod
    def result(self, item: R, /) -> bool:
        """Give the result of the selected operation."""
        raise NotImplementedError

    @abstractmethod
    def error(self, error: Exception, /) -> bool:
        """Give the error from the selected operation."""
        raise NotImplementedError

    @abstractmethod
    def abandoned(self) -> bool:
        """Indicate that the selected operation was abandoned."""
        raise NotImplementedError


class SelectorGuard[R](AbstractContextManager[Selector[R]], ABC):
    @abstractmethod
    def __enter__(self) -> Selector[R]:
        raise NotImplementedError

    @abstractmethod
    def collides(self, other: SelectorGuard) -> bool:
        """Check if this guard would collide with another guard."""
        raise NotImplementedError


class Selectable[R](ABC):
    @abstractmethod
    def __select__(self, guard: SelectorGuard[R], /):
        """Register a selector for the operation that created this selectable."""
        raise NotImplementedError


class NotGiven(Exception):
    pass


class AlreadyTaken(Exception):
    pass


class Selection[T, R]:
    """A single-give, single-take box."""

    def __init__(self):
        self.__lock = Lock()
        self.__given = False
        self.__taken = False
        self.__token: T | None = None
        self.__item: R | None = None
        self.__error: Exception | None = None

    def give(self, token: T, item: R, /) -> bool:
        with self.__lock:
            if self.__given:
                return False
            self.__token = token
            self.__item = item
            self.__given = True
        return True

    def error(self, token: T, error: Exception) -> bool:
        with self.__lock:
            if self.__given:
                return False
            self.__token = token
            self.__error = error
            self.__given = True
        return True

    def take(self) -> tuple[T, R]:
        with self.__lock:
            if not self.__given:
                raise NotGiven
            if self.__taken:
                raise AlreadyTaken
            if self.__error is not None:
                raise self.__error
            self.__taken = True
            return cast(T, self.__token), cast(R, self.__item)

    def given(self):
        return self.__given


class SelectionSelector[T, R](Selector[R]):
    def __init__(self, token: T, selection: Selection[T, R]):
        self.__token = token
        self.__selection = selection

    def result(self, item: R, /) -> bool:
        return self.__selection.give(self.__token, item)

    def error(self, error: Exception, /) -> bool:
        return self.__selection.error(self.__token, error)

    def abandoned(self) -> bool:
        return self.__selection.given()


class SelectionSelectorGuard[T, R](SelectorGuard[R]):
    def __init__(
        self,
        token: T,
        condition: Condition,
        selection: Selection[T, R],
    ):
        self.__token = token
        self.__condition = condition
        self.__selection = selection

    def __enter__(self) -> Selector[R]:
        self.__condition.__enter__()
        return SelectionSelector(self.__token, self.__selection)

    def __exit__(self, *args, **kwargs):
        self.__condition.notify_all()
        return self.__condition.__exit__(*args, **kwargs)

    def collides(self, other: SelectorGuard) -> bool:
        """Check if this guard would collide with another guard."""
        return (
            isinstance(other, SelectionSelectorGuard)
            and self.__selection is other.__selection
        )


def select[R](selectables: Iterable[Selectable[R]]) -> tuple[int, R]:
    """Simultaneously wait multiple selectables and complete exactly one."""
    selection = Selection[int, R]()
    with (condition := Condition()):
        for i, selectable in enumerate(selectables):
            guard = SelectionSelectorGuard(i, condition, selection)
            selectable.__select__(guard)
        while not selection.given():
            condition.wait()
        return selection.take()
