from contextlib import suppress
from time import sleep

from queueio import routine
from queueio.gather import gather
from queueio.pause import pause


@routine(name="regular", queue="queueio")
def regular(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        sleep(0.1)
    print(f"Instance {instance} completed")
    return f"Instance {instance} completed"


@routine(name="raises", queue="queueio")
def raises():
    raise ValueError("This is a test exception")


@routine(name="aregular", queue="queueio")
async def aregular(instance: int, iterations: int):
    return await regular(instance, iterations)


async def abstract(instance: int, iterations: int):
    # Works as long as the async call stack goes up to an
    # async def routine.
    with suppress(ValueError):
        await raises()
    return await aregular(instance, iterations)


@routine(name="irregular", queue="queueio")
async def irregular():
    await regular(1, 2)
    print("irregular sleep started")
    sleep(0.1)
    print("irregular sleep ended. Starting queueio pause.")
    await pause(0.4)
    print("queueio pause ended")
    await gather(regular(7, 2), pause(0.5))
    return await abstract(2, 5)
