from collections.abc import Awaitable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from contextlib import suppress
from functools import partial
from queue import Queue
from queue import ShutDown
from threading import Timer

from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation
from .invocation import InvocationErrored
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspension
from .invocation import LocalInvocationSuspended
from .qio import Qio
from .sleep import SleepSuspension
from .thread import Thread

INVOCATION_QUEUE_NAME = "qio"


class Worker:
    def __init__(self, *, concurrency: int):
        self.__qio = Qio()
        self.__tasks = Queue[Invocation | SendContinuation | ThrowContinuation]()
        self.__consumer = self.__qio.consume(prefetch=concurrency)
        self.__continuer_events = self.__qio.subscribe(
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
        self.__timers: dict[str, Timer] = {}

    def __call__(self):
        for thread in self.__runner_threads:
            thread.start()
        self.__continuer_thread.start()
        self.__receiver_thread.start()

        done, _ = wait(
            [self.__continuer_thread.future, self.__receiver_thread.future]
            + [thread.future for thread in self.__runner_threads],
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
        for invocation in self.__consumer:
            with suppress(ShutDown):
                self.__tasks.put(invocation)

    def __continuer(self):
        """Continue suspended invocations.

        This actor watches for completed or errored invocations that are
        blocking suspended invocations, and sends their continuations to the
        task queue to be resumed.
        """
        waiting: dict[str, Continuation] = {}

        while True:
            try:
                event = self.__continuer_events.get()
            except ShutDown:
                break

            match event:
                case InvocationSucceeded(
                    invocation_id=invocation_id,
                    value=value,
                ):
                    if invocation_id in waiting:
                        continuation = waiting.pop(invocation_id)
                        self.__qio.resolve(
                            continuation.invocation, continuation.generator, value
                        )
                        with suppress(ShutDown):
                            self.__tasks.put(
                                SendContinuation(
                                    invocation=continuation.invocation,
                                    generator=continuation.generator,
                                    value=value,
                                ),
                            )
                case InvocationErrored(
                    invocation_id=invocation_id,
                    exception=exception,
                ):
                    if invocation_id in waiting:
                        continuation = waiting.pop(invocation_id)
                        self.__qio.throw(
                            continuation.invocation, continuation.generator, exception
                        )
                        with suppress(ShutDown):
                            self.__tasks.put(
                                ThrowContinuation(
                                    invocation=continuation.invocation,
                                    generator=continuation.generator,
                                    exception=exception,
                                )
                            )
                case LocalInvocationSuspended(
                    invocation_id=invocation_id,
                    generator=generator,
                    suspension=suspension,
                    invocation=invocation,
                ):
                    match suspension:
                        case SleepSuspension():

                            def resume(
                                suspension_id,
                                task: SendContinuation,
                            ):
                                self.__timers.pop(suspension_id)
                                with suppress(ShutDown):
                                    self.__tasks.put(task)

                            self.__timers[suspension.id] = Timer(
                                suspension.interval,
                                partial(
                                    resume,
                                    suspension.id,
                                    SendContinuation(
                                        invocation=invocation,
                                        generator=generator,
                                        value=None,
                                    ),
                                ),
                            )
                            self.__timers[suspension.id].start()
                        case InvocationSuspension():
                            self.__qio.submit(suspension)
                            waiting[suspension.invocation.id] = Continuation(
                                invocation=invocation,
                                generator=generator,
                            )
                        case _:
                            raise ValueError(
                                f"Unexpected suspension type: {type(suspension)}"
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
                    case Invocation() as invocation:
                        self.__run_invocation(invocation)
                    case SendContinuation() | ThrowContinuation() as continuation:
                        self.__run_continuation(continuation)
            finally:
                self.__tasks.task_done()

    def __run_invocation(self, invocation: Invocation):
        """Process an invocation task."""
        self.__qio.start(invocation)
        try:
            result = invocation.run()
        except Exception as exception:
            self.__qio.error(invocation, exception)
        else:
            if isinstance(result, Awaitable):
                generator = result.__await__()
                self.__qio.suspend(invocation, generator, None)
                self.__qio.resolve(invocation, generator, None)
                with suppress(ShutDown):
                    self.__tasks.put(
                        SendContinuation(
                            invocation=invocation,
                            generator=generator,
                            value=None,
                        )
                    )
            else:
                self.__qio.succeed(invocation, result)

    def __run_continuation(self, continuation: SendContinuation | ThrowContinuation):
        """Process a continuation task."""
        self.__qio.resume(continuation.invocation)

        match continuation:
            case SendContinuation():
                method = continuation.send
            case ThrowContinuation():
                method = continuation.throw

        try:
            suspension = method()
        except StopIteration as stop:
            self.__qio.succeed(continuation.invocation, stop.value)
        except Exception as exception:
            self.__qio.error(continuation.invocation, exception)
        else:
            self.__qio.suspend(
                continuation.invocation, continuation.generator, suspension
            )

    def stop(self):
        self.__tasks.shutdown(immediate=True)
        for timer in self.__timers.values():
            timer.cancel()
        for thread in self.__runner_threads:
            thread.join()

    def shutdown(self):
        self.__tasks.shutdown(immediate=True)
        for timer in self.__timers.values():
            timer.cancel()
        self.__qio.shutdown()
        self.__continuer_thread.join()
        self.__receiver_thread.join()
        for thread in self.__runner_threads:
            thread.join()
