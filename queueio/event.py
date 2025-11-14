from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime

from .id import random_id


@dataclass(eq=False, kw_only=True)
class Event:
    event_id: str = field(default_factory=random_id, repr=False)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(tz=UTC), repr=False
    )
    id: str
