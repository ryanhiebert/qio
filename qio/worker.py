from collections.abc import Awaitable
from collections.abc import Callable
from functools import partial
from queue import Queue
from queue import ShutDown
from typing import Any
from typing import cast

from .bus import Bus
from .concurrency import Concurrency
from .concurrency import Done
from .consumer import Consumer
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .executor import Executor
from .invocation import Invocation
from .invocation import InvocationContinued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended


def consume(consumer: Consumer, queue: Queue[tuple[int, Invocation]]):
    """Consume the consumer and put them onto the queue."""
    # This needs to be run in a dedicated thread.
    for message in consumer:
        queue.put(message)


def invocation_starter(
    bus: Bus,
    executor: Executor,
    concurrency: Concurrency,
    consumer: Consumer,
    tasks: Queue[tuple[int, Invocation]],
    continuations: Queue[SendContinuation | ThrowContinuation],
):
    while True:
        try:
            concurrency.reserve()
        except Done:
            break

        try:
            delivery_tag, invocation = tasks.get()
        except ShutDown:
            break

        try:
            concurrency.start()
        except Done:
            break
        
        def on_complete(delivery_tag: int):
            consumer.ack(delivery_tag)
            concurrency.stop()

        match invocation:
            case Invocation():
                executor.submit(
                    partial(
                        invocation_runner,
                        bus,
                        consumer,
                        continuations,
                        invocation,
                        partial(on_complete, delivery_tag),
                    )
                )
                bus.publish(
                    InvocationStarted(
                        invocation=invocation,
                    )
                )


def invocation_runner(
    bus: Bus,
    consumer: Consumer,
    continuations: Queue[SendContinuation | ThrowContinuation],
    invocation: Invocation,
    on_completion: Callable[[], None],
):
    try:
        result = invocation.run()
    except Exception as exception:
        bus.publish(
            InvocationErrored(
                invocation=invocation,
                exception=exception,
            )
        )
    else:
        if isinstance(result, Awaitable):
            result = cast(Awaitable[Any], result)
            generator = result.__await__()
            bus.publish(
                InvocationContinued(
                    invocation=invocation,
                    value=None,
                )
            )
            bus.publish_local(
                LocalInvocationContinued(
                    invocation=invocation,
                    generator=generator,
                    value=None,
                )
            )
            continuations.put(
                SendContinuation(
                    invocation=invocation,
                    generator=generator,
                    value=None,
                )
            )
        else:
            bus.publish(
                InvocationSucceeded(
                    invocation=invocation,
                    value=result,
                )
            )
    finally:
        on_completion()


def continuation_starter(
    bus: Bus,
    continuations: Queue[SendContinuation | ThrowContinuation],
    executor: Executor,
    concurrency: Concurrency,
):
    while True:
        try:
            concurrency.reserve()
        except Done:
            break

        try:
            continuation = continuations.get()
        except ShutDown:
            break

        try:
            concurrency.start()
        except Done:
            break

        bus.publish(
            InvocationResumed(
                invocation=continuation.invocation,
            )
        )
        executor.submit(
            partial(
                continuation_runner,
                bus,
                continuation,
                concurrency.stop,
            )
        )


def continuation_runner(
    bus: Bus,
    continuation: SendContinuation | ThrowContinuation,
    on_completion: Callable[[], None],
):
    match continuation:
        case SendContinuation():
            method = continuation.send
        case ThrowContinuation():
            method = continuation.throw

    invocation, generator = continuation.invocation, continuation.generator
    try:
        suspension = method()
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
                suspension=suspension,
            )
        )
        bus.publish_local(
            LocalInvocationSuspended(
                invocation=invocation,
                generator=generator,
                suspension=suspension,
            )
        )
    finally:
        on_completion()
