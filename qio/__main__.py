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
    enqueued = bus.subscribe(InvocationEnqueued)
    events = bus.subscribe(
        {
            InvocationContinued,
            InvocationThrew,
            InvocationErrored,
            InvocationSucceeded,
            InvocationSuspended,
        }
    )

    invocation_queue = SimpleQueue[Invocation]()
    continuation_queue = SimpleQueue[SendContinuation | ThrowContinuation]()
    running: dict[Future[Any], Invocation | SendContinuation | ThrowContinuation] = {}
    waiting_on: dict[Invocation, SendContinuation] = {}

    bus.publish(InvocationEnqueued(invocation=regular(0, 2)))
    bus.publish(InvocationEnqueued(invocation=irregular()))

    with ThreadPoolExecutor(max_workers=threads) as executor:
        try:
            while (
                not enqueued.empty()
                or not events.empty()
                or not invocation_queue.empty()
                or not continuation_queue.empty()
                or running
            ):
                while not enqueued.empty():
                    invocation_queue.put(enqueued.get().invocation)

                while not events.empty():
                    match events.get():
                        case InvocationSucceeded(invocation=invocation, value=value):
                            if invocation in waiting_on:
                                continuation = waiting_on.pop(invocation)
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
                            if invocation in waiting_on:
                                continuation = waiting_on.pop(invocation)
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
                            continuation_queue.put(
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
                            continuation_queue.put(
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
                            continuation = SendContinuation(
                                invocation=invocation,
                                generator=generator,
                                value=None,
                            )
                            waiting_on[suspension] = continuation

                while len(running) < threads and not continuation_queue.empty():
                    task = continuation_queue.get()
                    bus.publish(InvocationResumed(invocation=task.invocation))
                    if isinstance(task, SendContinuation):
                        future = executor.submit(task.send)
                    else:
                        future = executor.submit(task.throw)
                    running[future] = task

                while len(running) < threads and not invocation_queue.empty():
                    task = invocation_queue.get()
                    bus.publish(InvocationStarted(invocation=task))
                    future = executor.submit(task.run)
                    running[future] = task

                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    task = running.pop(future)
                    if isinstance(task, SendContinuation | ThrowContinuation):
                        try:
                            suspension = future.result()
                            bus.publish(
                                InvocationSuspended(
                                    invocation=task.invocation,
                                    generator=task.generator,
                                    suspension=suspension,
                                )
                            )
                            bus.publish(InvocationEnqueued(invocation=suspension))
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
                                bus.publish(
                                    InvocationContinued(
                                        invocation=task,
                                        generator=cast(
                                            Awaitable[Any], result
                                        ).__await__(),
                                        value=None,
                                    )
                                )
                            else:
                                bus.publish(
                                    InvocationSucceeded(invocation=task, value=result)
                                )
        except KeyboardInterrupt:
            print("Shutting down gracefully.")
            executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
