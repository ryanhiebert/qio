from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from queue import Queue
from queue import ShutDown
from threading import Event

from .bus import Bus
from .consumer import Consumer
from .continuation import Continuation
from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .executor import Executor
from .invocation import InvocationContinued
from .invocation import InvocationErrored
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspension
from .invocation import InvocationThrew
from .invocation import LocalInvocationContinued
from .invocation import LocalInvocationSuspended
from .invocation import LocalInvocationThrew
from .producer import Producer
from .runner import Runner
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
        self.__runners = [
            Runner(self.__bus, self.__tasks, self.__consumer)
            for _ in range(concurrency)
        ]

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
        print("tasks shut down")
        for runner in self.__runners:
            print("joining runner")
            runner.join()
        print("all runners joined")

    def shutdown(self):
        print("shutting down worker")
        # First shut down the tasks to stop new work from being processed
        print("shutting down tasks...")
        self.__tasks.shutdown(immediate=True)

        # Then shut down the bus to stop event processing
        print("shutting down bus...")
        self.__bus.shutdown()

        # Finally shut down the consumer and executor
        print("shutting down consumer...")
        self.__consumer.shutdown()
        print("shutting down executor...")
        self.__executor.shutdown(wait=True)
        print("worker shutdown complete")
