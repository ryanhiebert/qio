from __future__ import annotations

from collections.abc import Awaitable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from contextlib import suppress
from functools import partial
from queue import Queue
from queue import ShutDown
from time import sleep
from typing import Any
from typing import cast

from pika import BlockingConnection

from . import routine
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
from .invocation import deserialize
from .invocation import serialize

INVOCATION_QUEUE_NAME = "qio"


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


def continuer(
    events: Queue[InvocationErrored | InvocationSucceeded | LocalInvocationSuspended],
    bus: Bus,
    continuations: Queue[SendContinuation | ThrowContinuation],
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
                print(f"Invocation succeeded {invocation.id}")
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
                    continuations.put(
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
                    continuations.put(
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


def inspector(events: Queue[object], bus: Bus):
    while True:
        try:
            print(events.get())
        except ShutDown:
            break


def main():
    bus = Bus()
    invocation_concurrency = Concurrency(3)
    continuation_concurrency = Concurrency(3)
    continuations = Queue[SendContinuation | ThrowContinuation]()

    channel = BlockingConnection().channel()
    channel.queue_declare(INVOCATION_QUEUE_NAME)
    channel.queue_purge(INVOCATION_QUEUE_NAME)

    # The subscriptions need to happen before the producing actors start,
    # or else they may miss events that they need to see.
    continuer_events = bus.subscribe(
        {
            InvocationErrored,
            InvocationSucceeded,
            LocalInvocationSuspended,
        }
    )
    inspector_events = bus.subscribe({object})

    with Executor(name="qio") as executor:
        consumer = Consumer(
            queue=INVOCATION_QUEUE_NAME,
            prefetch=3,
        )
        try:
            actors = {
                invocation_starter: executor.submit(
                    lambda: invocation_starter(
                        bus,
                        executor,
                        invocation_concurrency,
                        consumer,
                        continuations,
                    )
                ),
                continuation_starter: executor.submit(
                    lambda: continuation_starter(
                        bus,
                        continuations,
                        executor,
                        continuation_concurrency,
                    )
                ),
                continuer: executor.submit(
                    lambda: continuer(continuer_events, bus, continuations)
                ),
                inspector: executor.submit(lambda: inspector(inspector_events, bus)),
            }

            # Publish initial records
            invocation1 = regular(0, 2)
            bus.publish(InvocationSubmitted(invocation=invocation1))
            channel.basic_publish(
                exchange="",
                routing_key=INVOCATION_QUEUE_NAME,
                body=serialize(invocation1),
            )
            bus.publish(InvocationEnqueued(invocation=invocation1))

            invocation2 = irregular()
            bus.publish(InvocationSubmitted(invocation=invocation2))
            channel.basic_publish(
                exchange="",
                routing_key=INVOCATION_QUEUE_NAME,
                body=serialize(invocation2),
            )
            bus.publish(InvocationEnqueued(invocation=invocation2))

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
            continuations.shutdown(immediate=True)
            consumer.shutdown()


if __name__ == "__main__":
    main()
