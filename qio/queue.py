from __future__ import annotations

from collections import deque
from contextlib import ExitStack
from itertools import chain
from threading import Lock

from .select import SelectorGuard
from .select import selectmethod


class ShutDown(Exception):
    """The queue has been shut down."""


class Collision(Exception):
    """Selectors from the same selection collided."""


class SwapQueue[L, R]:
    """A swap queue trades values between two sides atomically."""

    def __init__(self):
        self.__lock = Lock()
        self.__shutdown: bool = False
        self.__left = deque[tuple[SelectorGuard[R], L]]()
        self.__right = deque[tuple[SelectorGuard[L], R]]()

    @selectmethod
    def left(self, guard: SelectorGuard[R], value: L):
        with self.__lock:
            if self.__shutdown:
                with guard as selector:
                    selector.error(ShutDown())
                    return

            with ExitStack() as stack:
                while self.__right:
                    other_guard, other_value = self.__right.popleft()
                    if guard.collides(other_guard):
                        raise Collision()
                    other_selector = stack.enter_context(other_guard)
                    if not other_selector.abandoned():
                        break
                else:
                    self.__left.append((guard, value))
                    return

                with guard as selector:
                    if selector.result(other_value):
                        other_selector.result(value)

    @selectmethod
    def right(self, guard: SelectorGuard[L], value: R):
        with self.__lock:
            if self.__shutdown:
                with guard as selector:
                    selector.error(ShutDown())
                    return

            with ExitStack() as stack:
                while self.__left:
                    other_guard, other_value = self.__left.popleft()
                    if guard.collides(other_guard):
                        raise Collision()
                    other_selector = stack.enter_context(other_guard)
                    if not other_selector.abandoned():
                        break
                else:
                    self.__right.append((guard, value))
                    return

                with guard as selector:
                    if selector.result(other_value):
                        other_selector.result(value)

    def shutdown(self):
        with self.__lock:
            self.__shutdown = True
            left, right = self.__left, self.__right
            self.__left, self.__right = deque(), deque()
            for guard, _ in chain(left, right):
                with guard as selector:
                    selector.error(ShutDown())


class SyncQueue[T]:
    def __init__(self):
        self.__swap_queue = SwapQueue[T, None]()

    @selectmethod
    def get(self, guard: SelectorGuard[T]):
        """Obtain a selectable to get a value from the queue."""
        return self.__swap_queue.right.select(None)(guard)

    @selectmethod
    def put(self, guard: SelectorGuard[None], value: T):
        """Obtain a selectable to put the value on the queue."""
        return self.__swap_queue.left.select(value)(guard)

    def shutdown(self):
        self.__swap_queue.shutdown()


class Queue[T]:
    def __init__(self, maxsize: int):
        self.__lock = Lock()
        self.__shutdown: bool = False
        self.__maxsize = maxsize
        self.__queue = deque[T]()
        self.__putters = deque[tuple[SelectorGuard[None], T]]()
        self.__getters = deque[tuple[SelectorGuard[T], None]]()

    @selectmethod
    def get(self, guard: SelectorGuard[T]):
        """Obtain a selectable to get a value from the queue."""
        with self.__lock:
            if self.__shutdown:
                with guard as selector:
                    selector.error(ShutDown())
                    return

            if self.__queue:
                with guard as selector:
                    if not selector.abandoned():
                        value = self.__queue.popleft()
                        selector.result(value)
            else:
                self.__getters.append((guard, None))

            while (
                self.__maxsize and self.__putters and self.__maxsize > len(self.__queue)
            ):
                other_guard, other_value = self.__putters.popleft()
                with other_guard as other_selector:
                    if other_selector.result(None):
                        self.__queue.append(other_value)

    @selectmethod
    def put(self, guard: SelectorGuard[None], value: T):
        """Obtain a selectable to put the value on the queue."""
        with self.__lock:
            if self.__shutdown:
                with guard as selector:
                    selector.error(ShutDown())
                    return

            if not self.__maxsize or len(self.__queue) < self.__maxsize:
                with guard as selector:
                    if selector.result(None):
                        self.__queue.append(value)
            else:
                self.__putters.append((guard, value))

            while self.__getters and self.__queue:
                other_guard, other_value = self.__getters.popleft()
                queue_value = self.__queue.popleft()
                with other_guard as other_selector:
                    if not other_selector.result(queue_value):
                        self.__getters.appendleft((other_guard, other_value))
                        self.__queue.appendleft(queue_value)

    def shutdown(self):
        with self.__lock:
            self.__shutdown = True
            putters, getters = self.__putters, self.__getters
            self.__putters.clear()
            self.__getters.clear()
            for guard, _ in chain(putters, getters):
                with guard as selector:
                    selector.error(ShutDown())
