from collections.abc import Awaitable
from collections.abc import Generator
from typing import Any

from .suspension import Suspension


class Suspendable[R](Awaitable[R]):
    def __await__(self) -> Generator[Suspension, Any, R]: ...
