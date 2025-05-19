from collections.abc import Callable
from typing import cast

from .broker import Broker
from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded


def run[R](invocation: Invocation[Callable[..., R]]) -> R:
    """Submit and wait for an invocation to complete."""
    bus = Bus()
    broker = Broker()
    producer = broker.producer
    completions = bus.subscribe({InvocationSucceeded, InvocationErrored})
    bus.publish(
        InvocationSubmitted(
            invocation_id=invocation.id,
            routine=invocation.routine,
            args=invocation.args,
            kwargs=invocation.kwargs,
        )
    )
    producer.enqueue(invocation)

    try:
        while True:
            match completions.get():
                case InvocationSucceeded() as event:
                    if event.invocation_id == invocation.id:
                        return cast(R, event.value)
                case InvocationErrored() as event:
                    if event.invocation_id == invocation.id:
                        raise event.exception
                case _:
                    pass
    finally:
        bus.shutdown()
