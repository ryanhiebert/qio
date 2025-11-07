from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator
from typing import Any

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
from .message import Message
from .receiver import Receiver
from .stream import Stream
from .suspension import Suspension


class Consumer(Iterable[Invocation]):
    def __init__(
        self,
        *,
        stream: Stream,
        receiver: Receiver,
        deserialize: Callable[[bytes], Invocation],
    ):
        self.__stream = stream
        self.__receiver = receiver
        self.__deserialize = deserialize
        self.__invocations = dict[Invocation, Message]()

    def __iter__(self) -> Iterator[Invocation]:
        for message in self.__receiver:
            invocation = self.__deserialize(message.body)
            self.__invocations[invocation] = message
            yield invocation

    def start(self, invocation: Invocation):
        """Signal that the invocation is starting."""
        self.__stream.publish(InvocationStarted(id=invocation.id))

    def suspend(
        self,
        invocation: Invocation,
        generator: Generator,
        suspension: Suspension | None,
    ):
        """Signal that the invocation has suspended."""
        if suspension:
            self.__stream.publish(InvocationSuspended(id=invocation.id))
            self.__stream.publish_local(
                LocalInvocationSuspended(
                    id=invocation.id,
                    suspension=suspension,
                    invocation=invocation,
                    generator=generator,
                )
            )
        self.__receiver.pause(self.__invocations[invocation])

    def resolve(self, invocation: Invocation, generator: Generator, value: Any):
        """Signal that a suspension has resolved to a value."""
        self.__stream.publish(InvocationContinued(id=invocation.id, value=value))
        self.__stream.publish_local(
            LocalInvocationContinued(id=invocation.id, generator=generator, value=value)
        )
        self.__receiver.unpause(self.__invocations[invocation])

    def throw(self, invocation: Invocation, generator: Generator, exception: Exception):
        """Signal that a suspension has thrown an exception."""
        self.__stream.publish(InvocationThrew(id=invocation.id, exception=exception))
        self.__stream.publish_local(
            LocalInvocationThrew(
                id=invocation.id, generator=generator, exception=exception
            )
        )
        self.__receiver.unpause(self.__invocations[invocation])

    def resume(self, invocation: Invocation):
        """Signal that the invocation is resuming."""
        self.__stream.publish(InvocationResumed(id=invocation.id))

    def succeed(self, invocation: Invocation, value: Any):
        """Signal that the invocation has succeeded."""
        self.__stream.publish(InvocationSucceeded(id=invocation.id, value=value))
        self.__receiver.finish(self.__invocations.pop(invocation))

    def error(self, invocation: Invocation, exception: Exception):
        """Signal that the invocation has errored."""
        self.__stream.publish(InvocationErrored(id=invocation.id, exception=exception))
        self.__receiver.finish(self.__invocations.pop(invocation))
