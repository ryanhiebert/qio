from collections.abc import Awaitable
from contextlib import suppress
from queue import Queue
from queue import ShutDown
from threading import Thread

from .bus import Bus
from .consumer import Consumer
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation
from .invocation import InvocationContinued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .task import Task


class Runner:
    def __init__(self, bus: Bus, tasks: Queue[Task], consumer: Consumer):
        self.__bus = bus
        self.__tasks = tasks
        self.__consumer = consumer
        self.__thread = Thread(target=self.__run, daemon=True)
        self.__thread.start()

    def join(self):
        self.__thread.join()

    def __run(self):
        """Run tasks from the queue until shutdown."""
        while True:
            try:
                task = self.__tasks.get()
            except ShutDown:
                break

            try:
                self.__process_task(task)
            finally:
                self.__tasks.task_done()

    def __process_task(self, task: Task):
        """Process a single task from the queue."""
        match task:
            case delivery_tag, Invocation() as invocation:
                self.__bus.publish(InvocationStarted(invocation=invocation))
                try:
                    result = invocation.run()
                except Exception as exception:
                    self.__bus.publish(
                        InvocationErrored(invocation=invocation, exception=exception)
                    )
                else:
                    if isinstance(result, Awaitable):
                        generator = result.__await__()
                        self.__bus.publish(
                            InvocationContinued(invocation=invocation, value=None)
                        )
                        self.__bus.publish_local(
                            LocalInvocationContinued(
                                invocation=invocation,
                                generator=generator,
                                value=None,
                            )
                        )
                        with suppress(ShutDown):
                            self.__tasks.put(
                                SendContinuation(
                                    invocation=invocation,
                                    generator=generator,
                                    value=None,
                                )
                            )
                    else:
                        self.__bus.publish(
                            InvocationSucceeded(invocation=invocation, value=result)
                        )
                finally:
                    self.__consumer.ack(delivery_tag)

            case (SendContinuation() | ThrowContinuation()) as continuation:
                self.__bus.publish(
                    InvocationResumed(invocation=continuation.invocation)
                )
                match continuation:
                    case SendContinuation():
                        method = continuation.send
                    case ThrowContinuation():
                        method = continuation.throw

                try:
                    suspension = method()
                except StopIteration as stop:
                    self.__bus.publish(
                        InvocationSucceeded(
                            invocation=continuation.invocation,
                            value=stop.value,
                        )
                    )
                except Exception as exception:
                    self.__bus.publish(
                        InvocationErrored(
                            invocation=continuation.invocation,
                            exception=exception,
                        )
                    )
                else:
                    self.__bus.publish(
                        InvocationSuspended(
                            invocation=continuation.invocation,
                            suspension=suspension,
                        )
                    )
                    self.__bus.publish_local(
                        LocalInvocationSuspended(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            suspension=suspension,
                        )
                    )
