from collections.abc import Awaitable
from collections.abc import Generator
from typing import Any

from .suspension import Suspension


class Suspendable[R](Awaitable[R], Suspension[R]):
    """A suspension that is awaitable.

    Many suspensions are more helpful if they are also awaitable,
    so that they can be used more flexibly with fewer abstractions.
    """

    def __await__(self) -> Generator[Suspension, Any, R]:
        return (yield self)
