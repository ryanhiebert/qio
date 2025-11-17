from contextlib import contextmanager

from .queueio import QueueIO as RealQueueIO
from .registry import routine as routine


@contextmanager
def activate():
    with RealQueueIO().activate():
        yield
