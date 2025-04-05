from queue import Queue
from queue import ShutDown

from pika import BlockingConnection

from .bus import Bus
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import InvocationContinued
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .invocation import serialize

INVOCATION_QUEUE_NAME = "qio"


def continuer(
    events: Queue[InvocationErrored | InvocationSucceeded | LocalInvocationSuspended],
    bus: Bus,
    continuations: Queue[SendContinuation | ThrowContinuation],
):
    channel = BlockingConnection().channel()
    waiting: dict[str, Continuation] = {}

    while True:
        try:
            event = events.get()
        except ShutDown:
            break

        match event:
            case InvocationSucceeded(
                invocation=invocation,
                value=value,
            ):
                if invocation.id in waiting:
                    continuation = waiting.pop(invocation.id)
                    bus.publish(
                        InvocationContinued(
                            invocation=invocation,
                            value=value,
                        )
                    )
                    bus.publish_local(
                        LocalInvocationContinued(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            value=value,
                        )
                    )
                    continuations.put(
                        SendContinuation(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            value=value,
                        )
                    )
            case InvocationErrored(
                invocation=invocation,
                exception=exception,
            ):
                if invocation.id in waiting:
                    continuation = waiting.pop(invocation.id)
                    bus.publish(
                        InvocationThrew(
                            invocation=invocation,
                            exception=exception,
                        )
                    )
                    bus.publish_local(
                        LocalInvocationThrew(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            exception=exception,
                        )
                    )
                    continuations.put(
                        ThrowContinuation(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            exception=exception,
                        )
                    )
            case LocalInvocationSuspended(
                invocation=invocation,
                generator=generator,
                suspension=suspension,
            ):
                bus.publish(InvocationSubmitted(invocation=suspension))
                channel.basic_publish(
                    exchange="",
                    routing_key=INVOCATION_QUEUE_NAME,
                    body=serialize(suspension),
                )
                bus.publish(InvocationEnqueued(invocation=suspension))
                waiting[suspension.id] = Continuation(
                    invocation=invocation,
                    generator=generator,
                )