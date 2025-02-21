from __future__ import annotations

from collections.abc import Callable
from typing import Any


class Routine[T : Callable[..., Any] = Callable[..., Any]]:
    def __init__(self, fn: T, *, name: str):
        self.fn = fn
        self.name = name

    def __repr__(self):
        return f"<{type(self).__name__} {self.name!r}>"
