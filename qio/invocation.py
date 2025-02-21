from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .id import random_id
from .routine import Routine


@dataclass(eq=False, kw_only=True)
class Invocation:
    id: str = field(default_factory=random_id)
    routine: Routine
    args: tuple[Any]
    kwargs: dict[str, Any]

    def run(self) -> Any:
        return self.routine.fn(*self.args, **self.kwargs)

    def __await__(self):
        yield self

    def __repr__(self):
        params_repr = ", ".join(
            (*map(repr, self.args), *(f"{k}={v!r}" for k, v in self.kwargs.items())),
        )
        return f"<{type(self).__name__} {self.id!r} {self.routine.name}({params_repr})>"