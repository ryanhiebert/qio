from contextlib import suppress
from time import sleep

from pika import BlockingConnection
from typer import Typer

from . import routine
from .bus import Bus
from .invocation import ROUTINE_REGISTRY
from .invocation import InvocationEnqueued
from .invocation import InvocationSubmitted
from .invocation import serialize
from .worker import Worker

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


ROUTINE_REGISTRY.setdefault(regular.name, regular)
ROUTINE_REGISTRY.setdefault(raises.name, raises)
ROUTINE_REGISTRY.setdefault(aregular.name, aregular)
ROUTINE_REGISTRY.setdefault(irregular.name, irregular)


app = Typer()


@app.command()
def enqueue():
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
    events = bus.subscribe({object})

    try:
        while True:
            print(events.get())
    except KeyboardInterrupt:
        print("Shutting down gracefully.")
    finally:
        bus.shutdown()


@app.command()
def worker():
    worker = Worker(concurrency=3)
    try:
        worker()
    except KeyboardInterrupt:
        print("Shutting down gracefully.")
        worker.stop()
    finally:
        worker.shutdown()


@app.command()
def purge():
    channel = BlockingConnection().channel()
    channel.queue_declare(queue=INVOCATION_QUEUE_NAME, durable=True)
    channel.queue_purge(queue=INVOCATION_QUEUE_NAME)
    print("Queue purged.")


if __name__ == "__main__":
    app()
