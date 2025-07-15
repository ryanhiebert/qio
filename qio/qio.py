from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from queue import Queue
from typing import Any
from typing import cast

from .broker import Message
from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationContinued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import InvocationSuspension
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .invocation import deserialize
from .invocation import serialize
from .pika.broker import PikaBroker
from .pika.transport import PikaTransport
from .suspension import Suspension


class Qio:
    def __init__(self):
        self.__bus = Bus(PikaTransport())
        self.__broker = PikaBroker()
        self.__invocations = dict[Invocation, Message]()

    def submit(self, suspension: InvocationSuspension):
        """Submit an InvocationSuspension to be processed.

        This publishes the submission event and enqueues the invocation.
        """
        self.__bus.publish(
            InvocationSubmitted(
                invocation_id=suspension.invocation.id,
                routine=suspension.invocation.routine,
                args=suspension.invocation.args,
                kwargs=suspension.invocation.kwargs,
            )
        )
        self.__broker.enqueue(serialize(suspension.invocation))

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

    def subscribe[T](self, types: Iterable[type[T]]) -> Queue[T]:
        return self.__bus.subscribe(types)

    def unsubscribe(self, queue: Queue):
        return self.__bus.unsubscribe(queue)

    def consume(self, *, prefetch: int) -> Generator[Invocation]:
        for message in self.__broker.consume(prefetch=prefetch):
            invocation = deserialize(message.body)
            self.__invocations[invocation] = message
            yield invocation

    def start(self, invocation: Invocation):
        """Signal that the invocation is starting."""
        self.__broker.start(self.__invocations[invocation])
        self.__bus.publish(InvocationStarted(invocation_id=invocation.id))

    def suspend(
        self,
        invocation: Invocation,
        generator: Generator,
        suspension: Suspension | None,
    ):
        """Signal that the invocation has suspended."""
        if suspension:
            self.__bus.publish(
                InvocationSuspended(invocation_id=invocation.id, suspension=suspension)
            )
            self.__bus.publish_local(
                LocalInvocationSuspended(
                    invocation_id=invocation.id,
                    invocation=invocation,
                    generator=generator,
                    suspension=suspension,
                )
            )
        self.__broker.suspend(self.__invocations[invocation])

    def resolve(self, invocation: Invocation, generator: Generator, value: Any):
        """Signal that a suspension has resolved to a value."""
        self.__bus.publish(
            InvocationContinued(invocation_id=invocation.id, value=value)
        )
        self.__bus.publish_local(
            LocalInvocationContinued(
                invocation_id=invocation.id, generator=generator, value=value
            )
        )

    def throw(self, invocation: Invocation, generator: Generator, exception: Exception):
        """Signal that a suspension has thrown an exception."""
        self.__bus.publish(
            InvocationThrew(invocation_id=invocation.id, exception=exception)
        )
        self.__bus.publish_local(
            LocalInvocationThrew(
                invocation_id=invocation.id, generator=generator, exception=exception
            )
        )

    def resume(self, invocation: Invocation):
        """Signal that the invocation is resuming."""
        self.__broker.resume(self.__invocations[invocation])
        self.__bus.publish(InvocationResumed(invocation_id=invocation.id))

    def succeed(self, invocation: Invocation, value: Any):
        """Signal that the invocation has succeeded."""
        self.__bus.publish(
            InvocationSucceeded(invocation_id=invocation.id, value=value)
        )
        self.__broker.complete(self.__invocations.pop(invocation))

    def error(self, invocation: Invocation, exception: Exception):
        """Signal that the invocation has errored."""
        self.__bus.publish(
            InvocationErrored(invocation_id=invocation.id, exception=exception)
        )
        self.__broker.complete(self.__invocations.pop(invocation))

    def shutdown(self):
        """Shut down all components."""
        self.__broker.shutdown()
        self.__bus.shutdown()
