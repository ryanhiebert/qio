from collections.abc import Callable
from typing import cast

from .broker import Broker
from .bus import Bus
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspension


class Qio:
    def __init__(self):
        self.bus = Bus()
        self.broker = Broker()

    def submit[R](self, suspension: InvocationSuspension):
        """Submit an InvocationSuspension to be processed.

        This publishes the submission event and enqueues the invocation.
        """
        self.bus.publish(
            InvocationSubmitted(
                invocation_id=suspension.invocation.id,
                routine=suspension.invocation.routine,
                args=suspension.invocation.args,
                kwargs=suspension.invocation.kwargs,
            )
        )
        self.broker.producer.enqueue(suspension.invocation)

    def run[R](self, suspension: InvocationSuspension[Callable[..., R]]) -> R:
        """Run an invocation and wait for its completion."""
        completions = self.bus.subscribe({InvocationSucceeded, InvocationErrored})
        self.submit(suspension)

        try:
            while True:
                match completions.get():
                    case InvocationSucceeded() as event:
                        if event.invocation_id == suspension.invocation.id:
                            return cast(R, event.value)
                    case InvocationErrored() as event:
                        if event.invocation_id == suspension.invocation.id:
                            raise event.exception
                    case _:
                        pass
        finally:
            self.bus.unsubscribe(completions)

    def shutdown(self):
        """Shut down all components."""
        self.bus.shutdown()
