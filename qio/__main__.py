from __future__ import annotations

from collections.abc import Awaitable
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
from .bus import Bus
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation
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


def main(threads: int = 3):
    bus = Bus()
    events = bus.subscribe(
        {
            InvocationEnqueued,
            InvocationContinued,
            InvocationThrew,
            InvocationErrored,
            InvocationSucceeded,
            InvocationSuspended,
        }
    )
    tasks = SimpleQueue[Invocation | SendContinuation | ThrowContinuation]()
    running: dict[Future[Any], Invocation | Continuation] = {}
    waiting: dict[Invocation, Continuation] = {}

    bus.publish(InvocationEnqueued(invocation=regular(0, 2)))
    bus.publish(InvocationEnqueued(invocation=irregular()))

    with ThreadPoolExecutor(max_workers=threads) as executor:
        try:
            while not events.empty() or not tasks.empty() or running:
                while not events.empty():
                    match events.get():
                        case InvocationEnqueued(invocation=invocation):
                            tasks.put(invocation)
                        case InvocationSucceeded(invocation=invocation, value=value):
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
                        case InvocationSuspended(
                            invocation=invocation,
                            generator=generator,
                            suspension=suspension,
                        ):
                            bus.publish(InvocationEnqueued(invocation=suspension))
                            continuation = Continuation(
                                invocation=invocation,
                                generator=generator,
                            )
                            waiting[suspension] = continuation

                while len(running) < threads and not tasks.empty():
                    match task := tasks.get():
                        case SendContinuation(invocation=invocation):
                            bus.publish(InvocationResumed(invocation=invocation))
                            future = executor.submit(task.send)
                            running[future] = task
                        case ThrowContinuation(invocation=invocation):
                            bus.publish(InvocationResumed(invocation=invocation))
                            future = executor.submit(task.throw)
                            running[future] = task
                        case Invocation():
                            bus.publish(InvocationStarted(invocation=task))
                            future = executor.submit(task.run)
                            running[future] = task

                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    match task := running.pop(future):
                        case Continuation(invocation=invocation, generator=generator):
                            try:
                                suspension = future.result()
                            except StopIteration as stop:
                                bus.publish(
                                    InvocationSucceeded(
                                        invocation=invocation, value=stop.value
                                    )
                                )
                            except Exception as exception:
                                bus.publish(
                                    InvocationErrored(
                                        invocation=invocation, exception=exception
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
                        case Invocation():
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

        except KeyboardInterrupt:
            print("Shutting down gracefully.")
            executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
