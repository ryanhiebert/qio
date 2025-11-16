from contextlib import suppress
from time import sleep as time_sleep

from queueio import QueueIO
from queueio import routine
from queueio.gather import gather
from queueio.sleep import sleep


@routine(name="regular", queue="queueio")
def regular(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        time_sleep(0.1)
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
    time_sleep(0.1)
    print("irregular sleep ended. Starting queueio sleep.")
    await sleep(0.4)
    print("queueio sleep ended")
    await gather(regular(7, 2), sleep(0.5), abstract(8, 1))
    return await abstract(2, 5)


if __name__ == "__main__":
    q = QueueIO()
    try:
        q.submit(irregular())
    finally:
        q.shutdown()
