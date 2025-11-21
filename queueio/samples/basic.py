# This file should exactly mirror the example in README.md

from time import sleep

from queueio import gather
from queueio import pause
from queueio import routine


@routine(name="blocking", queue="queueio")
def blocking():
    sleep(0.1)  # Regular blocking call


@routine(name="yielding", queue="queueio")
async def yielding(iterations: int):
    # Do them two at a time
    for _ in range(iterations // 2):
        await gather(blocking(), blocking())
        await pause(0.2)  # Release processing capacity
    if iterations % 2 == 1:
        await blocking()
