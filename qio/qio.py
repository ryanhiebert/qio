from collections.abc import Callable
from typing import cast

from .broker import Broker
from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded


class Qio:
    def __init__(self):
        self.bus = Bus()
        self.broker = Broker()

    def submit[R](self, invocation: Invocation[Callable[..., R]]):
        """Submit an invocation to be processed.

        This publishes the submission event and enqueues the invocation.
        """
        self.bus.publish(
            InvocationSubmitted(
                invocation_id=invocation.id,
                routine=invocation.routine,
                args=invocation.args,
                kwargs=invocation.kwargs,
            )
        )
        self.broker.producer.enqueue(invocation)

    def run[R](self, invocation: Invocation[Callable[..., R]]) -> R:
        """Run an invocation and wait for its completion."""
        completions = self.bus.subscribe({InvocationSucceeded, InvocationErrored})
        self.submit(invocation)

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
            self.bus.unsubscribe(completions)

    def shutdown(self):
        """Shut down all components."""
        self.bus.shutdown()
