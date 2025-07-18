from contextlib import suppress
from time import sleep as time_sleep

from pika import BlockingConnection
from typer import Typer

from . import routine
from .invocation import INVOCATION_QUEUE_NAME
from .monitor import Monitor
from .qio import Qio
from .sleep import sleep
from .worker import Worker


@routine()
def regular(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        time_sleep(1)
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
    time_sleep(1)
    print("irregular sleep ended. Starting qio sleep.")
    await sleep(4)
    print("qio sleep ended")
    return await abstract(2, 5)


app = Typer()


@app.command()
def submit():
    qio = Qio()
    try:
        qio.submit(regular(0, 2))
        qio.submit(irregular())
    finally:
        qio.shutdown()


@app.command()
def monitor(raw: bool = False):
    if raw:
        qio = Qio()
        events = qio.subscribe({object})
        try:
            while True:
                print(events.get())
        except KeyboardInterrupt:
            print("Shutting down gracefully.")
        finally:
            qio.shutdown()
    else:
        Monitor().run()


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
