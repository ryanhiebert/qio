from concurrent.futures import Future
from dataclasses import dataclass
from threading import Timer

from .suspension import Suspension


@dataclass(eq=False, kw_only=True)
class Pause(Suspension[None]):
    interval: float

    def submit(self):
        future = Future[None]()
        timer = Timer(self.interval, lambda: future.set_result(None))
        future.add_done_callback(lambda _: timer.cancel())
        timer.start()
        return future


def pause(interval: float, /) -> Pause:
    return Pause(interval=interval)
