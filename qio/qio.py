from collections.abc import Callable
from collections.abc import Iterable
from queue import Queue
from typing import cast

from .bus import Bus
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspension
from .invocation import serialize
from .pika.broker import PikaBroker
from .pika.transport import PikaTransport


class Qio:
    def __init__(self):
        self.bus = Bus(PikaTransport())
        self.broker = PikaBroker()

    def subscribe[T](self, types: Iterable[type[T]]) -> Queue[T]:
        return self.bus.subscribe(types)

    def unsubscribe(self, queue: Queue):
        return self.bus.unsubscribe(queue)

    def submit(self, suspension: InvocationSuspension):
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
        self.broker.enqueue(serialize(suspension.invocation))

    def run[R](self, suspension: InvocationSuspension[Callable[..., R]]) -> R:
        """Run an invocation and wait for its completion."""
        completions = self.subscribe({InvocationSucceeded, InvocationErrored})
        try:
            self.submit(suspension)
            while True:
                event = completions.get()
                match event:
                    case InvocationSucceeded() as event:
                        if event.invocation_id == suspension.invocation.id:
                            return cast(R, event.value)
                    case InvocationErrored() as event:
                        if event.invocation_id == suspension.invocation.id:
                            raise event.exception
        finally:
            self.unsubscribe(completions)

    def shutdown(self):
        """Shut down all components."""
        self.broker.shutdown()
        self.bus.shutdown()
