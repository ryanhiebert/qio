from threading import Lock

from pika import BlockingConnection

from .bus import Bus
from .invocation import INVOCATION_QUEUE_NAME
from .invocation import Invocation
from .invocation import InvocationEnqueued
from .invocation import InvocationSubmitted
from .invocation import serialize


class Producer:
    def __init__(self):
        self.__bus = Bus()
        self.__channel_lock = Lock()
        self.__channel = BlockingConnection().channel()

    def submit(self, invocation: Invocation):
        self.__bus.publish(InvocationSubmitted(invocation=invocation))
        with self.__channel_lock:
            self.__channel.basic_publish(
                exchange="",
                routing_key=INVOCATION_QUEUE_NAME,
                body=serialize(invocation),
            )
        self.__bus.publish(InvocationEnqueued(invocation=invocation))
