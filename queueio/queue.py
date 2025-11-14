from __future__ import annotations

from collections import deque
from contextlib import ExitStack
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

    def __swap[I, O](
        self,
        queue: deque[tuple[SelectorGuard[O], I]],
        other_queue: deque[tuple[SelectorGuard[I], O]],
        guard: SelectorGuard[O],
        value: I,
    ):
        with self.__lock:
            if self.__shutdown:
                with guard as selector:
                    if selector:
                        selector.error(ShutDown())
                    return

            with ExitStack() as stack:
                while other_queue:
                    other_guard, other_value = other_queue.popleft()
                    if guard.collides(other_guard):
                        raise Collision()
                    other_selector = stack.enter_context(other_guard)
                    if other_selector:
                        break
                else:
                    queue.append((guard, value))
                    return

                with guard as selector:
                    if selector:
                        selector.result(other_value)
                        other_selector.result(value)

    @selectmethod
    def left(self, guard: SelectorGuard[R], value: L):
        self.__swap(self.__left, self.__right, guard, value)

    @selectmethod
    def right(self, guard: SelectorGuard[L], value: R):
        self.__swap(self.__right, self.__left, guard, value)

    def shutdown(self):
        with self.__lock:
            self.__shutdown = True
            while self.__left:
                guard, _ = self.__left.popleft()
                with guard as selector:
                    if selector:
                        selector.error(ShutDown())
            while self.__right:
                guard, _ = self.__right.popleft()
                with guard as selector:
                    if selector:
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
    def __init__(self, maxsize: int = 0):
        self.__lock = Lock()
        self.__half_shutdown: bool = False
        self.__full_shutdown: bool = False
        self.__maxsize = maxsize
        self.__queue = deque[T]()
        self.__putters = deque[tuple[SelectorGuard[None], T]]()
        self.__getters = deque[tuple[SelectorGuard[T], None]]()

    @selectmethod
    def get(self, guard: SelectorGuard[T]):
        """Obtain a selectable to get a value from the queue."""
        with self.__lock:
            if self.__full_shutdown:
                with guard as selector:
                    if selector:
                        selector.error(ShutDown())
                    return

            if self.__queue:
                with guard as selector:
                    if selector:
                        value = self.__queue.popleft()
                        selector.result(value)
            else:
                self.__getters.append((guard, None))

            if not self.__queue and self.__half_shutdown:
                self.__full_shutdown = True

            while (
                self.__maxsize and self.__putters and self.__maxsize > len(self.__queue)
            ):
                other_guard, other_value = self.__putters.popleft()
                with other_guard as other_selector:
                    if other_selector:
                        other_selector.result(None)
                        self.__queue.append(other_value)

    @selectmethod
    def put(self, guard: SelectorGuard[None], value: T):
        """Obtain a selectable to put the value on the queue."""
        with self.__lock:
            if self.__half_shutdown:
                with guard as selector:
                    if selector:
                        selector.error(ShutDown())
                    return

            if not self.__maxsize or len(self.__queue) < self.__maxsize:
                with guard as selector:
                    if selector:
                        selector.result(None)
                        self.__queue.append(value)
            else:
                self.__putters.append((guard, value))

            while self.__getters and self.__queue:
                other_guard, other_value = self.__getters.popleft()
                queue_value = self.__queue.popleft()
                with other_guard as other_selector:
                    if other_selector:
                        other_selector.result(queue_value)
                    else:
                        self.__getters.appendleft((other_guard, other_value))
                        self.__queue.appendleft(queue_value)

    def shutdown(self, *, immediate: bool = False):
        with self.__lock:
            self.__half_shutdown = True
            while self.__putters:
                guard, _ = self.__putters.popleft()
                with guard as selector:
                    if selector:
                        selector.error(ShutDown())
            # There are only getters if there is no queue
            while self.__getters:
                guard, _ = self.__getters.popleft()
                with guard as selector:
                    if selector:
                        selector.error(ShutDown())
            if immediate or not self.__queue:
                self.__full_shutdown = True
