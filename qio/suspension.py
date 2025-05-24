from dataclasses import dataclass
from dataclasses import field

from .id import random_id


@dataclass(eq=False, kw_only=True)
class Suspension:
    """Base class for all suspension types in the system."""
    
    id: str = field(default_factory=random_id)
