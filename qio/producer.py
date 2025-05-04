from threading import Lock

from pika import BlockingConnection

from .bus import Bus
from .invocation import INVOCATION_QUEUE_NAME
from .invocation import Invocation
from .invocation import InvocationEnqueued
from .invocation import serialize


class Producer:
    def __init__(self, *, bus: Bus):
        self.__bus = bus
        self.__channel_lock = Lock()
        self.__channel = BlockingConnection().channel()

    def enqueue(self, invocation: Invocation):
        with self.__channel_lock:
            self.__channel.basic_publish(
                exchange="",
                routing_key=INVOCATION_QUEUE_NAME,
                body=serialize(invocation),
            )
        self.__bus.publish(InvocationEnqueued(invocation=invocation))
