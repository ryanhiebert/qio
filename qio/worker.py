from collections.abc import Awaitable
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
from .invocation import deserialize

INVOCATION_QUEUE_NAME = "qio"


def invocation_starter(
    bus: Bus,
    executor: Executor,
    concurrency: Concurrency,
    consumer: Consumer,
    continuations: Queue[SendContinuation | ThrowContinuation],
):
    while True:
        try:
            concurrency.reserve()
        except Done:
            break

        try:
            delivery_tag, body = next(consumer)
        except StopIteration:
            break

        invocation = deserialize(body)

        try:
            concurrency.start()
        except Done:
            break

        match invocation:
            case Invocation():
                executor.submit(
                    partial(
                        invocation_runner,
                        bus,
                        concurrency,
                        consumer,
                        continuations,
                        invocation,
                        delivery_tag,
                    )
                )
                bus.publish(
                    InvocationStarted(
                        invocation=invocation,
                    )
                )


def invocation_runner(
    bus: Bus,
    concurrency: Concurrency,
    consumer: Consumer,
    continuations: Queue[SendContinuation | ThrowContinuation],
    invocation: Invocation,
    delivery_tag: int,
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
        consumer.ack(delivery_tag)
        concurrency.stop()


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
                concurrency,
                continuation,
            )
        )


def continuation_runner(
    bus: Bus,
    concurrency: Concurrency,
    continuation: SendContinuation | ThrowContinuation,
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
        concurrency.stop()
