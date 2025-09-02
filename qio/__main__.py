from contextlib import suppress
from time import sleep as time_sleep

from pika import ConnectionParameters
from typer import Typer

from . import routine
from .gather import gather
from .monitor import Monitor
from .pika.broker import PikaBroker
from .pika.transport import PikaTransport
from .qio import Qio
from .sleep import sleep
from .worker import Worker


@routine(name="regular", queue="qio")
def regular(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        time_sleep(0.1)
    print(f"Instance {instance} completed")
    return f"Instance {instance} completed"


@routine(name="raises")
def raises():
    raise ValueError("This is a test exception")


@routine(name="aregular", queue="qio")
async def aregular(instance: int, iterations: int):
    return await regular(instance, iterations)


async def abstract(instance: int, iterations: int):
    # Works as long as the async call stack goes up to an
    # async def routine.
    with suppress(ValueError):
        await raises()
    return await aregular(instance, iterations)


@routine(name="irregular", queue="qio")
async def irregular():
    await regular(1, 2)
    print("irregular sleep started")
    time_sleep(0.1)
    print("irregular sleep ended. Starting qio sleep.")
    await sleep(0.4)
    print("qio sleep ended")
    await gather(regular(7, 2), sleep(0.5), abstract(8, 1))
    return await abstract(2, 5)


app = Typer()


@app.command()
def submit():
    connection_params = ConnectionParameters()
    qio = Qio(
        broker=PikaBroker(connection_params),
        transport=PikaTransport(connection_params),
        default_queue="qio",
    )
    try:
        qio.submit(irregular())
    finally:
        qio.shutdown()


@app.command()
def monitor(raw: bool = False):
    if raw:
        connection_params = ConnectionParameters()
        qio = Qio(
            broker=PikaBroker(connection_params),
            transport=PikaTransport(connection_params),
            default_queue="qio",
        )
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
    connection_params = ConnectionParameters()
    qio = Qio(
        broker=PikaBroker(connection_params),
        transport=PikaTransport(connection_params),
        default_queue="qio",
    )
    Worker(qio, queue="qio", concurrency=3)()


@app.command()
def purge():
    connection_params = ConnectionParameters()
    qio = Qio(
        broker=PikaBroker(connection_params),
        transport=PikaTransport(connection_params),
        default_queue="qio",
    )
    try:
        qio.purge(queue="qio")
    finally:
        qio.shutdown()


if __name__ == "__main__":
    app()
