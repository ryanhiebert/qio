from __future__ import annotations

from collections.abc import Awaitable
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from pprint import pprint
from queue import SimpleQueue
from time import sleep
from typing import Any
from typing import cast

from . import routine
from .continuation import Continuation
from .invocation import Invocation


@routine()
def example(instance: int, iterations: int):
    for i in range(iterations):
        print(f"Iteration {instance} {i} started")
        sleep(1)
    return f"sleep_and_print {instance}"


@routine()
async def coordinate():
    print("Coordinator started")
    value = await example(0, 2)
    print(f"coordinate {value=}")
    return "coordinate"


class Waiting:
    def __init__(self):
        self.__waiting: dict[Continuation, set[Invocation]] = {}
        self.__waiting_on: dict[Invocation, set[Continuation]] = {}
        self.__queue = SimpleQueue[Continuation]()

    def empty(self):
        return self.__queue.empty()

    def get(self):
        return self.__queue.get()

    def wait(self, continuation: Continuation, invocations: set[Invocation]):
        self.__waiting[continuation] = invocations
        for invocation in invocations:
            self.__waiting_on.setdefault(invocation, set())
            self.__waiting_on[invocation].add(continuation)

    def start(self, invocation: Invocation, awaitable: Awaitable[Any]):
        self.__queue.put(
            Continuation(
                invocation=invocation,
                generator=awaitable.__await__(),
                value=None,
            ),
        )

    def complete(self, invocation: Invocation, value: Any):
        for continuation in self.__waiting_on.pop(invocation, set()):
            self.__waiting[continuation].remove(invocation)
            if not self.__waiting[continuation]:
                del self.__waiting[continuation]
                self.__queue.put(
                    Continuation(
                        invocation=continuation.invocation,
                        generator=continuation.generator,
                        value=value,
                    ),
                )


class Qio:
    def __init__(self):
        self.__queue = SimpleQueue[Invocation]()
    
    def enqueue(self, invocation: Invocation):
        self.__queue.put(invocation)

    def execute(self, *, threads: int = 3):
        queue = self.__queue
        results: dict[Invocation, Any] = {}
        running: dict[Future[Any], Invocation | Continuation] = {}
        waiting = Waiting()
    
        with ThreadPoolExecutor(max_workers=threads) as executor:
            try:
                while not queue.empty() or not waiting.empty() or running:
                    while len(running) < threads and not waiting.empty():
                        task = waiting.get()
                        future = executor.submit(task.send)
                        running[future] = task
    
                    while len(running) < threads and not queue.empty():
                        task = queue.get()
                        future = executor.submit(task.run)
                        running[future] = task
    
                    done, _ = wait(running, return_when=FIRST_COMPLETED)
                    for future in done:
                        task = running.pop(future)
                        if isinstance(task, Continuation):
                            try:
                                wait_for = future.result()
                                waiting.wait(task, {wait_for})
                                queue.put(wait_for)
                            except StopIteration as stop:
                                results[task.invocation] = stop.value
                                waiting.complete(task.invocation, stop.value)
                        else:
                            result = future.result()
                            if isinstance(result, Awaitable):
                                waiting.start(task, cast(Any, result))
                            else:
                                results[task] = result
                                waiting.complete(task, result)
            except KeyboardInterrupt:
                print("Shutting down gracefully.")
                executor.shutdown(wait=False, cancel_futures=True)
        pprint(results)


INVOCATIONS = [
    example(1, 2),
    coordinate(),
    coordinate(),
]


if __name__ == "__main__":
    qio  = Qio()
    for invocation in INVOCATIONS:
        qio.enqueue(invocation)
    qio.execute()
