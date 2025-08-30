from collections.abc import Awaitable
from typing import cast

from .suspendable import Suspendable
from .suspended import Suspended
from .suspension import Suspension


def suspend[R](suspendable: Awaitable[R]) -> Suspension[R]:
    """Use a thread to convert an awaitable into a suspension."""
    if isinstance(suspendable, Suspension):
        return suspendable  # Invocations are their own suspensions

    # Assume all awaitables are Suspendable to handle coroutines
    return Suspended(cast(Suspendable[R], suspendable))
