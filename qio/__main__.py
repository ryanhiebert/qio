from __future__ import annotations

import json
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
from .routine import Routine

INVOCATION_QUEUE_NAME = "qio"
ROUTINE_REGISTRY: dict[str, Routine] = {}


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


def serialize_invocation(invocation: Invocation, /) -> bytes:
    ROUTINE_REGISTRY.setdefault(invocation.routine.name, invocation.routine)
    assert ROUTINE_REGISTRY[invocation.routine.name] == invocation.routine
    return json.dumps(
        {
            "id": invocation.id,
            "routine": invocation.routine.name,
            "args": invocation.args,
            "kwargs": invocation.kwargs,
        }
    ).encode()


def deserialize_invocation(serialized: bytes, /) -> Invocation:
    data = json.loads(serialized.decode())
    return Invocation(
        id=data["id"],
        routine=ROUTINE_REGISTRY[data["routine"]],
        args=data["args"],
        kwargs=data["kwargs"],
    )


def invocation_queuer(bus: Bus):
    events = bus.subscribe({InvocationSubmitted})
    channel = BlockingConnection().channel()
    channel.queue_declare(INVOCATION_QUEUE_NAME)

    while True:
        try:
            event = events.get()
        except ShutDown:
            break

        match event:
            case InvocationSubmitted(invocation=invocation):
                channel.basic_publish(
                    exchange="",
                    routing_key=INVOCATION_QUEUE_NAME,
                    body=serialize_invocation(invocation),
                )
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


def invocation_starter(
    bus: Bus,
    executor: Executor,
    concurrency: Concurrency,
    consumer: Consumer,
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

        invocation = deserialize_invocation(body)

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
            bus.publish(
                InvocationContinued(
                    invocation=invocation,
                    generator=result.__await__(),
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
            continuation = tasks.get()
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
                waiting[suspension.id] = Continuation(
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
    invocation_concurrency = Concurrency(3)
    continuation_concurrency = Concurrency(3)
    continuation_tasks = Queue[SendContinuation | ThrowContinuation]()

    with BlockingConnection().channel() as channel:
        channel.queue_declare(INVOCATION_QUEUE_NAME)
        channel.queue_purge(INVOCATION_QUEUE_NAME)

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
                    )
                ),
                invocation_queuer: executor.submit(lambda: invocation_queuer(bus)),
                continuation_starter: executor.submit(
                    lambda: continuation_starter(
                        bus,
                        continuation_tasks,
                        executor,
                        continuation_concurrency,
                    )
                ),
                continuation_queuer: executor.submit(
                    lambda: continuation_queuer(bus, continuation_tasks)
                ),
                continuer: executor.submit(lambda: continuer(bus)),
                inspector: executor.submit(lambda: inspector(bus)),
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
            continuation_tasks.shutdown(immediate=True)
            consumer.shutdown()


if __name__ == "__main__":
    main()
