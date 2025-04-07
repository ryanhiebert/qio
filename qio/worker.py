from collections.abc import Awaitable
from collections.abc import Callable
from functools import partial
from queue import Queue
from queue import ShutDown
from typing import Any
from typing import cast

from pika import BlockingConnection

from .bus import Bus
from .concurrency import Concurrency
from .concurrency import Done
from .consumer import Consumer
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
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .invocation import serialize

INVOCATION_QUEUE_NAME = "qio"


def consume(
    consumer: Consumer,
    queue: Queue[tuple[int, Invocation] | SendContinuation | ThrowContinuation],
):
    """Consume the consumer and put them onto the queue."""
    # This needs to be run in a dedicated thread.
    for message in consumer:
        queue.put(message)


def starter(
    bus: Bus,
    executor: Executor,
    concurrency: Concurrency,
    consumer: Consumer,
    tasks: Queue[tuple[int, Invocation] | SendContinuation | ThrowContinuation],
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

        def on_complete(delivery_tag: int):
            consumer.ack(delivery_tag)
            concurrency.stop()

        match task:
            case delivery_tag, Invocation() as invocation:
                executor.submit(
                    partial(
                        invocation_runner,
                        bus,
                        consumer,
                        tasks,
                        invocation,
                        partial(on_complete, delivery_tag),
                    )
                )
                bus.publish(
                    InvocationStarted(
                        invocation=invocation,
                    )
                )
            case (SendContinuation() | ThrowContinuation()) as continuation:
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


def invocation_runner(
    bus: Bus,
    consumer: Consumer,
    continuations: Queue[tuple[int, Invocation] | SendContinuation | ThrowContinuation],
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


def continuer(
    events: Queue[InvocationErrored | InvocationSucceeded | LocalInvocationSuspended],
    bus: Bus,
    tasks: Queue[tuple[int, Invocation] | SendContinuation | ThrowContinuation],
):
    channel = BlockingConnection().channel()
    waiting: dict[str, Continuation] = {}

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
                if invocation.id in waiting:
                    continuation = waiting.pop(invocation.id)
                    bus.publish(
                        InvocationContinued(
                            invocation=invocation,
                            value=value,
                        )
                    )
                    bus.publish_local(
                        LocalInvocationContinued(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            value=value,
                        )
                    )
                    tasks.put(
                        SendContinuation(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            value=value,
                        )
                    )
            case InvocationErrored(
                invocation=invocation,
                exception=exception,
            ):
                if invocation.id in waiting:
                    continuation = waiting.pop(invocation.id)
                    bus.publish(
                        InvocationThrew(
                            invocation=invocation,
                            exception=exception,
                        )
                    )
                    bus.publish_local(
                        LocalInvocationThrew(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            exception=exception,
                        )
                    )
                    tasks.put(
                        ThrowContinuation(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            exception=exception,
                        )
                    )
            case LocalInvocationSuspended(
                invocation=invocation,
                generator=generator,
                suspension=suspension,
            ):
                bus.publish(InvocationSubmitted(invocation=suspension))
                channel.basic_publish(
                    exchange="",
                    routing_key=INVOCATION_QUEUE_NAME,
                    body=serialize(suspension),
                )
                bus.publish(InvocationEnqueued(invocation=suspension))
                waiting[suspension.id] = Continuation(
                    invocation=invocation,
                    generator=generator,
                )
