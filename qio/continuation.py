from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .id import random_id
from .invocation import Invocation


@dataclass(eq=False, kw_only=True)
class Continuation:
    id: str = field(default_factory=random_id)
    invocation: Invocation
    generator: Generator[Invocation, Any, Any]
    value: Any

    def send(self) -> Any:
        return self.generator.send(self.value)