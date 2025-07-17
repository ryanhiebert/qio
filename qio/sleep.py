from collections.abc import Generator
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Timer

from .suspendable import suspendable
from .suspension import Suspension


@dataclass(eq=False, kw_only=True)
class SleepSuspension(Suspension):
    interval: float

    def start(self):
        future = Future[None]()
        timer = Timer(self.interval, lambda: future.set_result(None))
        future.add_done_callback(lambda _: timer.cancel())
        timer.start()
        return future


@suspendable
def sleep(interval: float, /) -> Generator[SleepSuspension]:
    yield SleepSuspension(interval=interval)
