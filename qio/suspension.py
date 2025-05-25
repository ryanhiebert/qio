from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import cast

from .id import random_id


@dataclass(eq=False, kw_only=True)
class Suspension:
    """Base class for all suspension types in the system."""

    id: str = field(default_factory=random_id)

    def __await__(self) -> Any:
        return cast(Any, (yield self))
