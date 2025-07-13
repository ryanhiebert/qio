from dataclasses import dataclass


@dataclass(eq=False, frozen=True)
class Message:
    body: bytes
