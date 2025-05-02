from .continuation import SendContinuation
from .continuation import ThrowContinuation
from .invocation import Invocation

Task = Invocation | SendContinuation | ThrowContinuation
