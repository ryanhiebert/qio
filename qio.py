from __future__ import annotations
from typing import Any
from typing import Awaitable
from typing import Generator
from dataclasses import dataclass
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED
from time import sleep
from queue import SimpleQueue
from pprint import pprint
import string
import secrets


class Routine:
    def __init__(self, fn, *, name: str):
        self._fn = fn
        self.name = name

    def __call__(self, *args, **kwargs) -> Invocation:
        alphabet = string.ascii_lowercase + string.digits
        id = ''.join(secrets.choice(alphabet) for _ in range(10))
        return Invocation(id=id, routine=self, args=args, kwargs=kwargs)

    def __repr__(self):
        return f"<{type(self).__name__} {self.name!r}>"


def routine(*, name: str | None = None):
    """Decorate a function to make it a routine."""

    def create_routine(fn):
        return Routine(fn, name=name or f"{fn.__module__}.{fn.__qualname__}")

    return create_routine


@dataclass(eq=False)
class Invocation:
    id: str
    routine: Routine
    args: tuple[Any]
    kwargs: dict[str, Any]

    def run(self) -> Any:
        return self.routine._fn(*self.args, **self.kwargs)

    def __await__(self):
        yield self

    def __repr__(self):
        params_repr = ', '.join((*map(repr, self.args), *(f'{k}={v!r}' for k, v in self.kwargs.items())))
        return f"<{type(self).__name__} {self.id!r} {self.routine.name}({params_repr})>"


@dataclass(eq=False)
class Continuation:
    invocation: Invocation
    generator: Generator[Invocation, Any, Any]
    value: Any

    def send(self) -> Any:
        return self.generator.send(self.value)


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

    def wait(self, continuation: Continuation, invocations: set[Invocation]):
        self.__waiting[continuation] = invocations

    def complete(self, invocation: Invocation, value) -> set[Continuation]:
        continuations = set()
        for continuation, invocations in list(self.__waiting.items()):
            if invocation in invocations:
                invocations.remove(invocation)
                if not invocations:
                    del self.__waiting[continuation]
                    continuations.add(
                        Continuation(
                            invocation=continuation.invocation,
                            generator=continuation.generator,
                            value=value,
                        )
                    )
        return continuations


def execute(queue: SimpleQueue[Invocation | Continuation], *, threads: int = 3):
    results: dict[Invocation, Any] = {}
    running: dict[Future, Invocation | Continuation] = {}
    waiting = Waiting()

    with ThreadPoolExecutor(max_workers=threads) as executor:
        try:
            while not queue.empty() or running:
                while not queue.empty() and len(running) < threads:
                    task = queue.get()
                    if isinstance(task, Continuation):
                        future = executor.submit(task.send)
                        running[future] = task
                    else:
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
                            for continuation in waiting.complete(
                                task.invocation, stop.value
                            ):
                                queue.put(continuation)
                    else:
                        result = future.result()
                        if isinstance(result, Awaitable):
                            continuation = Continuation(
                                invocation=task,
                                generator=result.__await__(),
                                value=None,
                            )
                            queue.put(continuation)
                        else:
                            results[task] = result
                            for continuation in waiting.complete(task, result):
                                queue.put(continuation)
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
    queue = SimpleQueue()
    for invocation in INVOCATIONS:
        queue.put(invocation)
    execute(queue)
