from collections.abc import Callable
from collections.abc import Iterable
from queue import Queue
from typing import cast

from .broker import Broker
from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded


class Qio:
    """A unified interface for QIO operations."""

    def __init__(self):
        """Initialize the QIO interface."""
        self.__bus = Bus()
        self.__broker = Broker()

    def submit[R](self, invocation: Invocation[Callable[..., R]]) -> None:
        """Submit an invocation to be processed.

        This publishes the submission event and enqueues the invocation.
        """
        self.__bus.publish(InvocationSubmitted(invocation=invocation))
        self.__broker.producer.enqueue(invocation)
        self.__bus.publish(InvocationEnqueued(invocation=invocation))

    def subscribe(self, types: Iterable[type]) -> Queue:
        """Subscribe to events on the bus."""
        return self.__bus.subscribe(types)

    def run[R](self, invocation: Invocation[Callable[..., R]]) -> R:
        """Run an invocation and wait for its completion."""
        completions = self.__bus.subscribe({InvocationSucceeded, InvocationErrored})
        self.submit(invocation)

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
            self.__bus.unsubscribe(completions)

    def shutdown(self) -> None:
        """Shutdown all components."""
        self.__bus.shutdown()
