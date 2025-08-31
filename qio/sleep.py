from concurrent.futures import Future
from dataclasses import dataclass
from threading import Timer

from .suspending import suspending
from .suspension import Suspension


@dataclass(eq=False, kw_only=True)
class Sleep(Suspension):
    interval: float

    def start(self):
        future = Future[None]()
        timer = Timer(self.interval, lambda: future.set_result(None))
        future.add_done_callback(lambda _: timer.cancel())
        timer.start()
        return future


@suspending
def sleep(interval: float, /) -> Sleep:
    return Sleep(interval=interval)
