from collections.abc import Callable

from .invocation import Invocation


class Routine[**A, R]:
    def __init__(self, fn: Callable[A, R], *, name: str):
        self.fn = fn
        self.name = name

    def __repr__(self):
        return f"<{type(self).__name__} {self.name!r}>"

    def __call__(self, *args: A.args, **kwargs: A.kwargs) -> Invocation[R]:
        return Invocation(routine=self.name, args=args, kwargs=kwargs)
