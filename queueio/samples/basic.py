# This file should exactly mirror the example in README.md

from time import sleep as time_sleep

from queueio import activate
from queueio import routine
from queueio.gather import gather
from queueio.sleep import sleep


@routine(name="blocking", queue="queueio")
def blocking():
    time_sleep(0.1)  # Regular blocking call


@routine(name="yielding", queue="queueio")
async def yielding(iterations: int):
    # Do them two at a time
    for _ in range(iterations // 2):
        await gather(blocking(), blocking())
        await sleep(0.2)  # Release processing capacity
    if iterations % 2 == 1:
        await blocking()


if __name__ == "__main__":
    with activate():
        yielding(7).start()
