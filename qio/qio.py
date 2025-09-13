from collections.abc import Generator
from collections.abc import Iterable
from concurrent.futures import Future
from contextlib import contextmanager
from typing import Any

from .broker import Broker
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
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .invocation import deserialize
from .invocation import serialize
from .queue import Queue
from .queue import ShutDown
from .queuespec import QueueSpec
from .registry import ROUTINE_REGISTRY
from .routine import Routine
from .suspension import Suspension
from .thread import Thread
from .transport import Transport


class Qio:
    def __init__(self, *, broker: Broker, transport: Transport):
        self.__bus = Bus(transport)
        self.__broker = broker
        self.__invocations = dict[Invocation, Message]()

    def run[R](self, invocation: Invocation[R], /) -> R:
        with self.invocation_handler():
            return invocation.start().result()

    def purge(self, *, queue: str):
        self.__broker.purge(queue=queue)

    def routine(self, routine_name: str, /) -> Routine:
        return ROUTINE_REGISTRY[routine_name]

    def subscribe[T](self, types: Iterable[type[T]]) -> Queue[T]:
        return self.__bus.subscribe(types)

    def unsubscribe(self, queue: Queue):
        return self.__bus.unsubscribe(queue)

    @contextmanager
    def invocation_handler(self) -> Generator[Future]:
        waiting: dict[str, Future] = {}
        events = self.subscribe({InvocationSucceeded, InvocationErrored})

        def resolver():
            while True:
                try:
                    event = events.get()
                except ShutDown:
                    break

                match event:
                    case InvocationSucceeded(invocation_id=invocation_id, value=value):
                        if invocation_id in waiting:
                            future = waiting.pop(invocation_id)
                            future.set_result(value)
                    case InvocationErrored(
                        invocation_id=invocation_id, exception=exception
                    ):
                        if invocation_id in waiting:
                            future = waiting.pop(invocation_id)
                            future.set_exception(exception)

        resolver_thread = Thread(target=resolver)
        resolver_thread.start()

        def handler(invocation: Invocation, /) -> Future:
            future = Future()
            waiting[invocation.id] = future
            self.submit(invocation)
            return future

        try:
            with Invocation.handler(handler):
                yield resolver_thread.future
        finally:
            self.unsubscribe(events)
            resolver_thread.join()

    def submit(self, invocation: Invocation, /):
        """Submit an invocation to be run in the background."""
        routine = self.routine(invocation.routine)
        self.__bus.publish(
            InvocationSubmitted(
                invocation_id=invocation.id,
                routine=invocation.routine,
                args=invocation.args,
                kwargs=invocation.kwargs,
            )
        )
        queue = routine.queue
        self.__broker.enqueue(serialize(invocation), queue=queue)

    def consume(self, queuespec: QueueSpec, /) -> Generator[Invocation]:
        for message in self.__broker.consume(queuespec):
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
