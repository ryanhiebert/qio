from dataclasses import dataclass

from .suspension import Suspension


@dataclass(eq=False, kw_only=True)
class SleepSuspension(Suspension):
    interval: float


def sleep(interval: float, /):
    return SleepSuspension(interval=interval)
