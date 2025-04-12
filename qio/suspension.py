from dataclasses import dataclass


@dataclass(eq=False, kw_only=True)
class Suspension:
    """Base class for all suspension types in the system."""
