from collections.abc import Iterable
from concurrent.futures import Future
from typing import Any
from typing import overload

from .suspension import Suspension


class Gather[T](Suspension[T]):
    def __init__(self, suspensions: Iterable[Suspension[Any]]):
        super().__init__()
        self.__suspensions = suspensions

    def submit(self) -> Future[T]:
        gathered = Future()
        futures = [suspension.submit() for suspension in self.__suspensions]

        # concurrent.futures.Future doesn't give us a way to be notified
        # when a future is running, so we can't reasonably determine when
        # something has begun running and is therefore uncancellable.
        # Rather than make gathered _always_ running and uncancellable,
        # we never set the future to running, so it will always be cancellable,
        # even if some futures are running already.

        def gathered_on_done(gathered):
            # Cancel all futures if the gathered future is cancelled
            if gathered.cancelled():
                for future in futures:
                    future.cancel()

        gathered.add_done_callback(gathered_on_done)

        def on_done(future):
            if all(future.done() for future in futures):
                results = []
                exceptions = []
                for future in futures:
                    try:
                        results.append(future.result(timeout=0))
                    except BaseException as exc:
                        exceptions.append(exc)
                if exceptions:
                    gathered.set_exception(
                        ExceptionGroup("Some gathered futures failed.", exceptions)
                    )
                else:
                    gathered.set_result(tuple(results))

        for f in futures:
            f.add_done_callback(on_done)

        return gathered


S = Suspension


@overload
def gather[T1](s: S[T1], /) -> Gather[tuple[T1]]: ...
@overload
def gather[T1, T2](s1: S[T1], s2: S[T2], /) -> Gather[tuple[T1, T2]]: ...
@overload
def gather[T1, T2, T3](
    s1: S[T1], s2: S[T2], s3: S[T3], /
) -> Gather[tuple[T1, T2, T3]]: ...
@overload
def gather[T1, T2, T3, T4](
    s1: S[T1], s2: S[T2], s3: S[T3], s4: S[T4], /
) -> Gather[tuple[T1, T2, T3, T4]]: ...
@overload
def gather[T1, T2, T3, T4, T5](
    s1: S[T1], s2: S[T2], s3: S[T3], s4: S[T4], s5: S[T5], /
) -> Gather[tuple[T1, T2, T3, T4, T5]]: ...


def gather(*suspensions: Suspension[Any]) -> Gather[Any]:
    return Gather(suspensions)
