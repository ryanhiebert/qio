from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterator
from typing import Any

from .broker import Broker
from .broker import Message
from .bus import Bus
from .invocation import Invocation
from .invocation import InvocationContinued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .suspension import Suspension


class Consumer(Iterator[Invocation]):
    def __init__(
        self,
        *,
        bus: Bus,
        broker: Broker,
        consumer: Iterator[Message],
        deserialize: Callable[[bytes], Invocation],
    ):
        self.__bus = bus
        self.__broker = broker
        self.__consumer = consumer
        self.__deserialize = deserialize
        self.__invocations = dict[Invocation, Message]()

    def __next__(self) -> Invocation:
        message = next(self.__consumer)
        invocation = self.__deserialize(message.body)
        self.__invocations[invocation] = message
        return invocation

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
            self.__bus.publish(InvocationSuspended(invocation_id=invocation.id))
            self.__bus.publish_local(
                LocalInvocationSuspended(
                    invocation_id=invocation.id,
                    suspension=suspension,
                    invocation=invocation,
                    generator=generator,
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
        self.__broker.unsuspend(self.__invocations[invocation])

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
        self.__broker.unsuspend(self.__invocations[invocation])

    def resume(self, invocation: Invocation):
        """Signal that the invocation is resuming."""
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
