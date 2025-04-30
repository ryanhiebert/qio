from collections.abc import Awaitable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from contextlib import suppress
from queue import Queue
from queue import ShutDown
from threading import Event
from threading import Thread

from .bus import Bus
from .consumer import Consumer
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .executor import Executor
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

INVOCATION_QUEUE_NAME = "qio"


def receiver(consumer: Consumer, queue: Queue[Task]):
    """Consume the consumer and put them onto the queue."""
    # This needs to be run in a dedicated thread.
    for message in consumer:
        queue.put(message)


def continuer(started: Event, bus: Bus, tasks: Queue[Task]):
    producer = Producer()
    events = bus.subscribe(
        {
            InvocationErrored,
            InvocationSucceeded,
            LocalInvocationSuspended,
        }
    )
    waiting: dict[str, Continuation] = {}
    started.set()

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
                    tasks.put(
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
                    tasks.put(
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


class Worker:
    def __init__(self, *, concurrency: int):
        self.__bus = Bus()
        self.__tasks = Queue[Task]()
        self.__consumer = Consumer(queue=INVOCATION_QUEUE_NAME, prefetch=concurrency)
        self.__executor = Executor(name="qio-worker")
        self.__threads = [
            Thread(target=self.__run, daemon=False) for _ in range(concurrency)
        ]
        for thread in self.__threads:
            thread.start()

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

    def __call__(self):
        continuer_started = Event()
        continuer_future = self.__executor.submit(
            lambda: continuer(continuer_started, self.__bus, self.__tasks)
        )
        continuer_started.wait()

        receive_future = self.__executor.submit(
            lambda: receiver(self.__consumer, self.__tasks)
        )

        futures = {
            "continuer": continuer_future,
            "receiver": receive_future,
        }

        # Shut down if any actor finishes
        done, _ = wait(futures.values(), return_when=FIRST_COMPLETED)
        for future in done:
            future.result()
        print("Some actor finished unexpectedly.")
        print(futures)

    def stop(self):
        self.__tasks.shutdown(immediate=True)
        # In the future, we should not shutdown immediately, we should
        # shut the queue down, then quickly drain the queue and handle
        # those tasks gracefully by requeueing them or nacking them.
        for thread in self.__threads:
            thread.join()

    def shutdown(self):
        self.__tasks.shutdown(immediate=True)
        self.__bus.shutdown()
        self.__consumer.shutdown()
        self.__executor.shutdown(wait=True)
