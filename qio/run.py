from collections.abc import Callable
from typing import cast

from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationErrored
from .invocation import InvocationSucceeded
from .producer import Producer


def run[R](invocation: Invocation[Callable[..., R]]) -> R:
    """Submit and wait for an invocation to complete."""
    bus = Bus()
    producer = Producer()
    completions = bus.subscribe({InvocationSucceeded, InvocationErrored})
    producer.submit(invocation)

    try:
        while True:
            match completions.get():
                case InvocationSucceeded() as event:
                    if event.invocation.id == invocation.id:
                        return cast(R, event.value)
                case InvocationErrored() as event:
                    if event.invocation.id == invocation.id:
                        raise event.exception
                case _:
                    pass
    finally:
        bus.shutdown()
