from collections.abc import Awaitable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import wait
from contextlib import suppress
from threading import Timer

from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation
from .invocation import LocalInvocationSuspended
from .qio import Qio
from .queue import Queue
from .queue import ShutDown
from .queuespec import QueueSpec
from .thread import Thread


class Worker:
    def __init__(self, qio: Qio, queuespec: QueueSpec):
        self.__qio = qio

        self.__tasks = Queue[Invocation | SendContinuation | ThrowContinuation]()
        self.__consumer = self.__qio.consume(queuespec)
        self.__continuer_events = self.__qio.subscribe({LocalInvocationSuspended})

        # Start threads event queues are created
        self.__runner_threads = [
            Thread(target=self.__runner, name=f"qio-runner-{i + 1}")
            for i in range(queuespec.concurrency)
        ]
        self.__continuer_thread = Thread(target=self.__continuer, name="qio-continuer")
        self.__receiver_thread = Thread(target=self.__receiver, name="qio-receiver")
        self.__timers: dict[str, Timer] = {}

    def __call__(self):
        with self.__qio.invocation_handler() as invocation_handler_future:
            try:
                for thread in self.__runner_threads:
                    thread.start()
                self.__continuer_thread.start()
                self.__receiver_thread.start()

                done, _ = wait(
                    [
                        invocation_handler_future,
                        self.__continuer_thread.future,
                        self.__receiver_thread.future,
                    ]
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
                        "invocation": invocation_handler_future,
                        "runners": [thread.future for thread in self.__runner_threads],
                    }
                )
            except KeyboardInterrupt:
                self.stop()
            finally:
                self.shutdown()

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

        waiting = dict[Future, Continuation]()
        new = Future[None]()

        def listener():
            nonlocal new

            while True:
                try:
                    event = self.__continuer_events.get()
                except BaseException as e:
                    new.set_exception(e)
                    return
                else:
                    # convert to future here to make results easier
                    waiting[event.suspension.start()] = Continuation(
                        invocation=event.invocation,
                        generator=event.generator,
                    )
                    # Replace ``new`` before setting the result
                    # to avoid short busy wait loops.
                    prior, new = new, Future[None]()
                    prior.set_result(None)

        listener_thread = Thread(target=listener)
        listener_thread.start()

        while True:
            # Reference ``new`` before ``waiting``, and save both
            # to temporary variables,to avoid subtle race conditions.
            wait_new, wait_waiting = new, set(waiting)
            wait(
                {listener_thread.future, wait_new} | set(wait_waiting),
                return_when=FIRST_COMPLETED,
            )

            if listener_thread.future.done():
                listener_thread.future.result()
                break

            for future in list(waiting):
                if not future.done() or future.cancelled():
                    continue

                continuation = waiting.pop(future)
                try:
                    value = future.result()
                except Exception as exception:
                    self.__consumer.throw(
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
                else:
                    self.__consumer.resolve(
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

            if wait_new.done():
                try:
                    wait_new.result()
                except ShutDown:
                    break

        listener_thread.join()

    def __runner(self):
        """Run tasks from the queue.

        This actor pulls tasks from the queue and runs them, writing the
        appropriate events to the stream to notify other actors of the results.
        """
        while True:
            try:
                task = self.__tasks.get()
            except ShutDown:
                break

            match task:
                case Invocation() as invocation:
                    self.__consumer.start(invocation)
                    self.__run_invocation(invocation)
                case SendContinuation() | ThrowContinuation() as continuation:
                    self.__consumer.resume(continuation.invocation)
                    self.__run_continuation(continuation)

    def __run_invocation(self, invocation: Invocation):
        """Process an invocation task."""
        routine = self.__qio.routine(invocation.routine)
        try:
            result = routine.fn(*invocation.args, **invocation.kwargs)
        except Exception as exception:
            self.__consumer.error(invocation, exception)
        else:
            if isinstance(result, Awaitable):
                generator = result.__await__()
                self.__run_continuation(
                    SendContinuation(
                        invocation=invocation,
                        generator=generator,
                        value=None,
                    )
                )
            else:
                self.__consumer.succeed(invocation, result)

    def __run_continuation(self, continuation: SendContinuation | ThrowContinuation):
        """Process a continuation task."""
        match continuation:
            case SendContinuation():
                method = continuation.send
            case ThrowContinuation():
                method = continuation.throw

        try:
            suspension = method()
        except StopIteration as stop:
            self.__consumer.succeed(continuation.invocation, stop.value)
        except Exception as exception:
            self.__consumer.error(continuation.invocation, exception)
        else:
            self.__consumer.suspend(
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
