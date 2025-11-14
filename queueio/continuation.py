from collections.abc import Callable
from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from .id import random_id
from .invocation import Invocation
from .result import Err
from .result import Ok
from .result import Result


@dataclass(eq=False, kw_only=True)
class Continuation[T: Callable[..., Any] = Callable[..., Any]]:
    id: str = field(default_factory=random_id)
    invocation: Invocation
    generator: Generator[Invocation, Any, Any]
    result: Result[Any, BaseException]

    def resume(self) -> Any:
        match self.result:
            case Ok(value):
                return self.generator.send(value)
            case Err(exception):
                return self.generator.throw(exception)
