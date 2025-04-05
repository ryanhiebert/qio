from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from contextlib import suppress
from queue import Queue
from queue import ShutDown
from time import sleep

from pika import BlockingConnection
from typer import Typer

from . import routine
from .bus import Bus
from .concurrency import Concurrency
from .consumer import Consumer
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .executor import Executor
from .invocation import InvocationContinued
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .invocation import serialize
from .invocation import ROUTINE_REGISTRY
from .worker import continuation_starter
from .worker import invocation_starter

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


app = Typer()


@app.command()
def enqueue():
    ROUTINE_REGISTRY.setdefault(regular.name, regular)
    ROUTINE_REGISTRY.setdefault(raises.name, raises)
    ROUTINE_REGISTRY.setdefault(aregular.name, aregular)
    ROUTINE_REGISTRY.setdefault(irregular.name, irregular)

    bus = Bus()
    channel = BlockingConnection().channel()

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


@app.command()
def monitor():
    bus = Bus()
    inspector_events = bus.subscribe({object})
    
    try:
        inspector(inspector_events, bus)
    except KeyboardInterrupt:
        print("Shutting down gracefully.")
    finally:
        bus.shutdown()


@app.command()
def worker():
    ROUTINE_REGISTRY.setdefault(regular.name, regular)
    ROUTINE_REGISTRY.setdefault(raises.name, raises)
    ROUTINE_REGISTRY.setdefault(aregular.name, aregular)
    ROUTINE_REGISTRY.setdefault(irregular.name, irregular)

    bus = Bus()
    invocation_concurrency = Concurrency(3)
    continuation_concurrency = Concurrency(3)
    continuations = Queue[SendContinuation | ThrowContinuation]()

    # The subscriptions need to happen before the producing actors start,
    # or else they may miss events that they need to see.
    continuer_events = bus.subscribe(
        {
            InvocationErrored,
            InvocationSucceeded,
            LocalInvocationSuspended,
        }
    )

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
            }

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


@app.command()
def purge():
    channel = BlockingConnection().channel()
    channel.queue_declare(queue=INVOCATION_QUEUE_NAME)
    channel.queue_purge(queue=INVOCATION_QUEUE_NAME)
    print("Queue purged.")


if __name__ == "__main__":
    app()
