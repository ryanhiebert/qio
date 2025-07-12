from functools import cached_property

from .consumer import Consumer
from .producer import Producer


class Broker:
    """A broker enables producing and consuming messages on a queue."""

    @cached_property
    def producer(self):
        return Producer()

    def consumer(self, *, queue: str, prefetch: int) -> Consumer:
        return Consumer(queue=queue, prefetch=prefetch)
