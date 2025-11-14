from dataclasses import dataclass


@dataclass(eq=False, frozen=True)
class Message:
    """An encoded payload that can be delivered."""

    body: bytes
