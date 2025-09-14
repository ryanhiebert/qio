from contextlib import suppress
from time import sleep as time_sleep

from qio import routine

from .gather import gather
from .sleep import sleep


@routine(name="regular", queue="qio")
def regular(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        time_sleep(0.1)
    print(f"Instance {instance} completed")
    return f"Instance {instance} completed"


@routine(name="raises", queue="qio")
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
