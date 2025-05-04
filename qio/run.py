from collections.abc import Callable
from typing import cast

from .broker import Broker
from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded


def run[R](invocation: Invocation[Callable[..., R]]) -> R:
    """Submit and wait for an invocation to complete."""
    bus = Bus()
    broker = Broker()
    producer = broker.producer
    completions = bus.subscribe({InvocationSucceeded, InvocationErrored})
    bus.publish(InvocationSubmitted(invocation=invocation))
    producer.enqueue(invocation)
    bus.publish(InvocationEnqueued(invocation=invocation))

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
