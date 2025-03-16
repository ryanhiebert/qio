from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Generator
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import wait
from contextlib import suppress
from functools import partial
from queue import Empty
from queue import Queue
from queue import ShutDown
from time import sleep
from typing import Any
from typing import cast

from . import routine
from .bus import Bus
from .concurrency import Concurrency
from .concurrency import Done
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .executor import Executor
from .invocation import Invocation
from .invocation import InvocationContinued
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSubmitted
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
    return await abstract(2, 5)


def invocation_queuer(bus: Bus, tasks: Queue[Invocation]):
    events = bus.subscribe({InvocationSubmitted})

    while True:
        try:
            event = events.get()
        except ShutDown:
            break

        match event:
            case InvocationSubmitted(invocation=invocation):
                tasks.put(invocation)
                bus.publish(InvocationEnqueued(invocation=invocation))


def continuation_queuer(bus: Bus, tasks: Queue[SendContinuation | ThrowContinuation]):
    events = bus.subscribe({InvocationContinued, InvocationThrew})

    while True:
        try:
            event = events.get()
        except ShutDown:
            break

        match event:
            case InvocationContinued(
                invocation=invocation,
                generator=generator,
                value=value,
            ):
                tasks.put(
                    SendContinuation(
                        invocation=invocation,
                        generator=generator,
                        value=value,
                    )
                )
            case InvocationThrew(
                invocation=invocation,
                generator=generator,
                exception=exception,
            ):
                tasks.put(
                    ThrowContinuation(
                        invocation=invocation,
                        generator=generator,
                        exception=exception,
                    )
                )


def invocation_runner(
    bus: Bus,
    tasks: Queue[Invocation],
    executor: Executor,
    concurrency: Concurrency,
):
    while True:
        try:
            concurrency.reserve()
        except Done:
            break

        try:
            task = tasks.get()
        except ShutDown:
            break

        try:
            concurrency.start()
        except Done:
            break

        match task:
            case Invocation() as task:
                future = executor.submit(task.run)
                bus.publish(
                    InvocationStarted(
                        invocation=task,
                        future=future,
                    )
                )


def continuation_runner(
    bus: Bus,
    tasks: Queue[SendContinuation | ThrowContinuation],
    executor: Executor,
    concurrency: Concurrency,
):
    while True:
        try:
            concurrency.reserve()
        except Done:
            break

        try:
            task = tasks.get()
        except ShutDown:
            break

        try:
            concurrency.start()
        except Done:
            break

        match task:
            case SendContinuation(invocation=invocation, generator=generator) as task:
                future = executor.submit(task.send)
                bus.publish(
                    InvocationResumed(
                        invocation=invocation,
                        generator=generator,
                        future=future,
                    )
                )
            case ThrowContinuation(invocation=invocation, generator=generator) as task:
                future = executor.submit(task.throw)
                bus.publish(
                    InvocationResumed(
                        invocation=invocation,
                        generator=generator,
                        future=future,
                    )
                )


def invocation_waiter(bus: Bus, concurrency: Concurrency):
    events = bus.subscribe({InvocationStarted})
    shutdown: bool = False
    running: dict[Future[Any], Invocation] = {}

    while True:
        while True:
            try:
                event = events.get(block=False)
            except Empty:
                break
            except ShutDown:
                shutdown = True
                break

            match event:
                case InvocationStarted(
                    invocation=invocation,
                    future=future,
                ):
                    running[future] = invocation

        if shutdown and not running:
            break

        done, _ = wait(running, return_when=FIRST_COMPLETED)
        for future in done:
            match running.pop(future):
                case Invocation() as task:
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
                            result = cast(Awaitable[Any], result)
                            bus.publish(
                                InvocationContinued(
                                    invocation=task,
                                    generator=result.__await__(),
                                    value=None,
                                )
                            )
                        else:
                            bus.publish(
                                InvocationSucceeded(
                                    invocation=task,
                                    value=result,
                                )
                            )
                    finally:
                        concurrency.stop()


def continuation_waiter(bus: Bus, concurrency: Concurrency):
    events = bus.subscribe({InvocationResumed})
    shutdown: bool = False
    running: dict[Future[Any], tuple[Invocation, Generator[Invocation, Any, Any]]] = {}

    while True:
        while True:
            try:
                event = events.get(block=False)
            except Empty:
                break
            except ShutDown:
                shutdown = True
                break

            match event:
                case InvocationResumed(
                    invocation=invocation,
                    future=future,
                    generator=generator,
                ):
                    running[future] = (invocation, generator)

        if shutdown and not running:
            break

        done, _ = wait(running, return_when=FIRST_COMPLETED)
        for future in done:
            match running.pop(future):
                case (invocation, generator):
                    try:
                        suspension = future.result()
                    except StopIteration as stop:
                        bus.publish(
                            InvocationSucceeded(
                                invocation=invocation,
                                value=stop.value,
                            )
                        )
                    except Exception as exception:
                        bus.publish(
                            InvocationErrored(
                                invocation=invocation,
                                exception=exception,
                            )
                        )
                    else:
                        bus.publish(
                            InvocationSuspended(
                                invocation=invocation,
                                generator=generator,
                                suspension=suspension,
                            )
                        )
                    finally:
                        concurrency.stop()


def continuer(bus: Bus):
    events = bus.subscribe(
        {
            InvocationErrored,
            InvocationSucceeded,
            InvocationSuspended,
        }
    )
    waiting: dict[Invocation, Continuation] = {}

    while True:
        try:
            event = events.get()
        except ShutDown:
            break

        match event:
            case InvocationSucceeded(
                invocation=invocation,
                value=value,
            ):
                if invocation in waiting:
                    continuation = waiting.pop(invocation)
                    bus.publish(
                        InvocationContinued(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            value=value,
                        )
                    )
            case InvocationErrored(
                invocation=invocation,
                exception=exception,
            ):
                if invocation in waiting:
                    continuation = waiting.pop(invocation)
                    bus.publish(
                        InvocationThrew(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            exception=exception,
                        )
                    )
            case InvocationSuspended(
                invocation=invocation,
                generator=generator,
                suspension=suspension,
            ):
                bus.publish(InvocationSubmitted(invocation=suspension))
                waiting[suspension] = Continuation(
                    invocation=invocation,
                    generator=generator,
                )


def inspector(bus: Bus):
    events = bus.subscribe(object)

    while True:
        try:
            print(events.get())
        except ShutDown:
            break


def main():
    bus = Bus()
    invocation_tasks = Queue[Invocation]()
    invocation_concurrency = Concurrency(3)
    continuation_tasks = Queue[SendContinuation | ThrowContinuation]()
    continuation_concurrency = Concurrency(3)

    with Executor(name="qio") as executor:
        try:
            actors = {
                invocation_runner: executor.submit(
                    partial(
                        invocation_runner,
                        bus,
                        invocation_tasks,
                        executor,
                        invocation_concurrency,
                    )
                ),
                invocation_queuer: executor.submit(
                    partial(invocation_queuer, bus, invocation_tasks)
                ),
                invocation_waiter: executor.submit(
                    partial(invocation_waiter, bus, invocation_concurrency)
                ),
                continuation_runner: executor.submit(
                    partial(
                        continuation_runner,
                        bus,
                        continuation_tasks,
                        executor,
                        continuation_concurrency,
                    )
                ),
                continuation_queuer: executor.submit(
                    partial(continuation_queuer, bus, continuation_tasks)
                ),
                continuation_waiter: executor.submit(
                    partial(continuation_waiter, bus, continuation_concurrency)
                ),
                continuer: executor.submit(partial(continuer, bus)),
                inspector: executor.submit(partial(inspector, bus)),
            }

            # Publish initial records
            bus.publish(InvocationSubmitted(invocation=regular(0, 2)))
            bus.publish(InvocationSubmitted(invocation=irregular()))

            # Shut down if any actor finishes
            done, _ = wait(actors.values(), return_when=FIRST_COMPLETED)
            for actor in done:
                actor.result()
            print("Some actor finished unexpectedly.")
            print(actors)
        except KeyboardInterrupt:
            print("Shutting down gracefully.")
            invocation_concurrency.shutdown(wait=True)
            continuation_concurrency.shutdown(wait=True)
        finally:
            bus.shutdown()
            invocation_tasks.shutdown(immediate=True)
            continuation_tasks.shutdown(immediate=True)


if __name__ == "__main__":
    main()
