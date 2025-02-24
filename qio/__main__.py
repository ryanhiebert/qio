from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from contextlib import suppress
from queue import SimpleQueue
from time import sleep
from typing import Any
from typing import cast

from . import routine
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation
from .invocation import InvocationCompleted
from .invocation import InvocationContinued
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import InvocationThrew


@routine()
def regular(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        sleep(1)
    print(f"Instance {instance} completed")
    return f"Instance {instance} completed"


@routine()
def raises():
    raise ValueError("This is a test exception")


@routine()
async def aregular(instance: int, iterations: int):
    return await regular(instance, iterations)


async def abstract(instance: int, iterations: int):
    # Works as long as the async call stack goes up to an
    # async def routine.
    with suppress(ValueError):
        await raises()
    return await aregular(instance, iterations)


@routine()
async def irregular():
    await regular(1, 2)
    print("irregular sleep started")
    sleep(1)
    print("irregular sleep ended")
    return await abstract(2, 2)


class Waiting:
    def __init__(self, bus: Bus):
        self.__bus = bus
        self.__waiting: dict[SendContinuation, set[Invocation]] = {}
        self.__waiting_on: dict[Invocation, set[SendContinuation]] = {}
        self.__queue = SimpleQueue[SendContinuation | ThrowContinuation]()
        self.__continued = self.__bus.subscribe({InvocationContinued})
        self.__threw = self.__bus.subscribe({InvocationThrew})
        self.__completed = self.__bus.subscribe({InvocationCompleted})
        self.__suspended = self.__bus.subscribe({InvocationSuspended})

    def empty(self):
        return (
            self.__queue.empty()
            and self.__continued.empty()
            and self.__threw.empty()
            and self.__completed.empty()
            and self.__suspended.empty()
        )

    def process(self):
        """Process the necessary events from the bus."""
        while not self.__completed.empty():
            event = self.__completed.get()
            match event:
                case InvocationSucceeded(invocation=invocation, value=value):
                    self.complete(invocation, value)
                case InvocationErrored(invocation=invocation, exception=exception):
                    self.throw(invocation, exception)
                case _:
                    raise NotImplementedError("Unexpected!!")

        while not self.__continued.empty():
            event = self.__continued.get()
            self.__queue.put(
                SendContinuation(
                    invocation=event.invocation,
                    generator=event.generator,
                    value=event.value,
                )
            )

        while not self.__threw.empty():
            event = self.__threw.get()
            self.__queue.put(
                ThrowContinuation(
                    invocation=event.invocation,
                    generator=event.generator,
                    exception=event.exception,
                )
            )

        while not self.__suspended.empty():
            event = self.__suspended.get()
            continuation = SendContinuation(
                invocation=event.invocation,
                generator=event.generator,
                value=None,
            )
            self.__waiting[continuation] = {event.suspension}
            self.__waiting_on.setdefault(event.suspension, set()).add(continuation)

    def get(self):
        return self.__queue.get()

    def wait(
        self,
        continuation: SendContinuation | ThrowContinuation,
        suspension: Invocation,
    ):
        self.__bus.publish(
            InvocationSuspended(
                invocation=continuation.invocation,
                generator=continuation.generator,
                suspension=suspension,
            )
        )
        self.__bus.publish(InvocationEnqueued(invocation=suspension))

    def start(self, invocation: Invocation, awaitable: Awaitable[Any]):
        self.__bus.publish(
            InvocationContinued(
                invocation=invocation,
                generator=awaitable.__await__(),
                value=None,
            )
        )

    def complete(self, invocation: Invocation, value: Any):
        for continuation in self.__waiting_on.pop(invocation, set()):
            self.__waiting[continuation].remove(invocation)
            if not self.__waiting[continuation]:
                del self.__waiting[continuation]
                self.__bus.publish(
                    InvocationContinued(
                        invocation=continuation.invocation,
                        generator=continuation.generator,
                        value=value,
                    )
                )

    def throw(self, invocation: Invocation, exception: Exception):
        for continuation in self.__waiting_on.pop(invocation, set()):
            self.__waiting[continuation].remove(invocation)
            if not self.__waiting[continuation]:
                del self.__waiting[continuation]
                self.__bus.publish(
                    InvocationThrew(
                        invocation=continuation.invocation,
                        generator=continuation.generator,
                        exception=exception,
                    )
                )
        pass


class Bus:
    def __init__(self):
        self.__subscribers: dict[SimpleQueue[Any], frozenset[type]] = {}
        self.__subscriptions: dict[type, set[SimpleQueue[Any]]] = {}

    def subscribe[T](self, types: Iterable[type[T]]) -> SimpleQueue[T]:
        queue = SimpleQueue[T]()
        self.__subscribers[queue] = frozenset(types)
        for type in types:
            self.__subscriptions.setdefault(type, set()).add(queue)
        return queue

    def publish(self, event: Any):
        print(event)
        subscribers = set[SimpleQueue[Any]]()
        for cls in type(event).__mro__:
            subscribers |= self.__subscriptions.get(cls, set[SimpleQueue[Any]]())

        for subscriber in subscribers:
            subscriber.put(event)


def main(threads: int = 3):
    bus = Bus()
    enqueued = bus.subscribe({InvocationEnqueued})

    queue = SimpleQueue[Invocation]()
    running: dict[Future[Any], Invocation | SendContinuation | ThrowContinuation] = {}
    waiting = Waiting(bus)

    bus.publish(InvocationEnqueued(invocation=regular(0, 2)))
    bus.publish(InvocationEnqueued(invocation=irregular()))

    with ThreadPoolExecutor(max_workers=threads) as executor:
        try:
            while (
                not enqueued.empty()
                or not queue.empty()
                or not waiting.empty()
                or running
            ):
                while not enqueued.empty():
                    queue.put(enqueued.get().invocation)

                waiting.process()

                while len(running) < threads and not waiting.empty():
                    task = waiting.get()
                    bus.publish(InvocationResumed(invocation=task.invocation))
                    if isinstance(task, SendContinuation):
                        future = executor.submit(task.send)
                    else:
                        future = executor.submit(task.throw)
                    running[future] = task

                while len(running) < threads and not queue.empty():
                    task = queue.get()
                    bus.publish(InvocationStarted(invocation=task))
                    future = executor.submit(task.run)
                    running[future] = task

                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    task = running.pop(future)
                    if isinstance(task, SendContinuation | ThrowContinuation):
                        try:
                            wait_for = future.result()
                            waiting.wait(task, wait_for)
                        except StopIteration as stop:
                            bus.publish(
                                InvocationSucceeded(
                                    invocation=task.invocation,
                                    value=stop.value,
                                )
                            )
                        except Exception as exception:
                            bus.publish(
                                InvocationErrored(
                                    invocation=task.invocation,
                                    exception=exception,
                                )
                            )
                    else:
                        try:
                            result = future.result()
                        except Exception as exception:
                            bus.publish(
                                InvocationErrored(
                                    invocation=task,
                                    exception=exception,
                                )
                            )
                        else:
                            if isinstance(result, Awaitable):
                                waiting.start(task, cast(Awaitable[Any], result))
                            else:
                                bus.publish(
                                    InvocationSucceeded(invocation=task, value=result)
                                )
        except KeyboardInterrupt:
            print("Shutting down gracefully.")
            executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
