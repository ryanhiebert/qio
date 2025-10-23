from threading import Lock

from pika import BlockingConnection
from pika import ConnectionParameters
from pika import URLParameters

from qio.broker import Broker
from qio.queuespec import QueueSpec

from .receiver import PikaReceiver


class PikaBroker(Broker):
    """A broker enables producing and consuming messages on a queue."""

    @classmethod
    def from_uri(cls, uri: str, /):
        """Create a broker instance from a URI."""
        amqp_uri = "amqp:" + uri.removeprefix("pika:")
        return cls(URLParameters(amqp_uri))

    def __init__(self, connection_params: ConnectionParameters | URLParameters):
        self.__connection_params = connection_params
        self.__producer_channel_lock = Lock()
        self.__producer_channel = BlockingConnection(self.__connection_params).channel()
        self.__shutdown_lock = Lock()
        self.__shutdown = False
        self.__receivers = set[PikaReceiver]()

    def enqueue(self, body: bytes, /, *, queue: str):
        with self.__producer_channel_lock:
            self.__producer_channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=body,
            )

    def purge(self, *, queue: str):
        with self.__producer_channel_lock:
            self.__producer_channel.queue_declare(queue=queue, durable=True)
            self.__producer_channel.queue_purge(queue=queue)

    def receive(self, queuespec: QueueSpec, /) -> PikaReceiver:
        if not queuespec.queues:
            raise ValueError("Must specify at least one queue")
        if len(queuespec.queues) != 1:
            raise ValueError("Only one queue is supported")

        receiver = PikaReceiver(
            connection_params=self.__connection_params,
            queue=queuespec.queues[0],
            prefetch=queuespec.concurrency,
        )
        self.__receivers.add(receiver)
        return receiver

    def shutdown(self):
        """Signal the final shutdown of the broker."""
        with self.__shutdown_lock:
            if self.__shutdown:
                return
            self.__shutdown = True
            for receiver in self.__receivers:
                receiver.shutdown()
