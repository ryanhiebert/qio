from collections.abc import Awaitable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from contextlib import suppress
from queue import Queue
from queue import ShutDown

from .bus import Bus
from .consumer import Consumer
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation
from .invocation import InvocationContinued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import InvocationSuspension
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .producer import Producer
from .task import Task
from .thread import Thread

INVOCATION_QUEUE_NAME = "qio"


class Worker:
    def __init__(self, *, concurrency: int):
        self.__bus = Bus()
        self.__tasks = Queue[Task]()
        self.__consumer = Consumer(queue=INVOCATION_QUEUE_NAME, prefetch=concurrency)
        self.__continuer_events = self.__bus.subscribe(
            {
                InvocationErrored,
                InvocationSucceeded,
                LocalInvocationSuspended,
            }
        )

        # Start threads event queues are created
        self.__runner_threads = [
            Thread(target=self.__runner, name=f"qio-runner-{i + 1}")
            for i in range(concurrency)
        ]
        self.__continuer_thread = Thread(target=self.__continuer, name="qio-continuer")
        self.__receiver_thread = Thread(target=self.__receiver, name="qio-receiver")

    def __call__(self):
        for thread in self.__runner_threads:
            thread.start()
        self.__continuer_thread.start()
        self.__receiver_thread.start()

        done, _ = wait(
            [self.__continuer_thread.future, self.__receiver_thread.future],
            return_when=FIRST_COMPLETED,
        )
        for future in done:
            future.result()
        print("Some actor finished unexpectedly.")
        print(
            {
                "continuer": self.__continuer_thread.future,
                "receiver": self.__receiver_thread.future,
            }
        )

    def __receiver(self):
        """Put messages from the consumer onto the queue.

        This actor is dedicated to reading the queue and keeping the
        consumer active.
        """
        for message in self.__consumer:
            self.__tasks.put(message)

    def __continuer(self):
        """Continue suspended invocations.

        This actor watches for completed or errored invocations that are
        blocking suspended invocations, and sends their continuations to the
        task queue to be resumed.
        """
        producer = Producer()
        waiting: dict[str, Continuation] = {}

        while True:
            try:
                event = self.__continuer_events.get()
            except ShutDown:
                break

            match event:
                case InvocationSucceeded(
                    invocation=invocation,
                    value=value,
                ):
                    if invocation.id in waiting:
                        continuation = waiting.pop(invocation.id)
                        self.__bus.publish(
                            InvocationContinued(
                                invocation=invocation,
                                value=value,
                            )
                        )
                        self.__bus.publish_local(
                            LocalInvocationContinued(
                                invocation=continuation.invocation,
                                generator=continuation.generator,
                                value=value,
                            )
                        )
                        self.__tasks.put(
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
                        self.__bus.publish(
                            InvocationThrew(
                                invocation=invocation,
                                exception=exception,
                            )
                        )
                        self.__bus.publish_local(
                            LocalInvocationThrew(
                                invocation=continuation.invocation,
                                generator=continuation.generator,
                                exception=exception,
                            )
                        )
                        self.__tasks.put(
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
                    if not isinstance(suspension, InvocationSuspension):
                        raise TypeError(
                            f"Expected InvocationSuspension, got {type(suspension)}"
                        )
                    producer.submit(suspension.invocation)
                    waiting[suspension.invocation.id] = Continuation(
                        invocation=invocation,
                        generator=generator,
                    )

    def __runner(self):
        """Run tasks from the queue.

        This actor pulls tasks from the queue and runs them, generating
        the appropriate events on the bus to notify other actors of the
        results.
        """
        while True:
            try:
                task = self.__tasks.get()
            except ShutDown:
                break

            try:
                match task:
                    case delivery_tag, Invocation() as invocation:
                        self.__run_invocation(delivery_tag, invocation)
                    case (SendContinuation() | ThrowContinuation()) as continuation:
                        self.__run_continuation(continuation)
            finally:
                self.__tasks.task_done()

    def __run_invocation(self, delivery_tag: int, invocation: Invocation):
        """Process an invocation task."""
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

    def __run_continuation(self, continuation: SendContinuation | ThrowContinuation):
        """Process a continuation task."""
        self.__bus.publish(InvocationResumed(invocation=continuation.invocation))
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

    def stop(self):
        self.__tasks.shutdown(immediate=True)
        # In the future, we should not shutdown immediately, we should
        # shut the queue down, then quickly drain the queue and handle
        # those tasks gracefully by requeueing them or nacking them.
        for thread in self.__runner_threads:
            thread.join()

    def shutdown(self):
        self.__tasks.shutdown(immediate=True)
        self.__bus.shutdown()
        self.__consumer.shutdown()
        self.__continuer_thread.join()
        self.__receiver_thread.join()
        for thread in self.__runner_threads:
            thread.join()
