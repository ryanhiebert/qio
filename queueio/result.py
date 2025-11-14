from dataclasses import dataclass


@dataclass
class Ok[T]:
    value: T


@dataclass
class Err[E: BaseException]:
    error: E


type Result[T, E: BaseException] = Ok[T] | Err[E]
