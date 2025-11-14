from collections.abc import Iterator
from threading import Lock
from threading import Thread
from typing import cast

from pika import BlockingConnection
from pika import ConnectionParameters
from pika import URLParameters

from queueio.journal import Journal

from ..queue import Queue
from ..queue import ShutDown


class PikaJournal(Journal):
    @classmethod
    def from_uri(cls, uri: str, /):
        """Create a journal instance from a URI."""
        amqp_uri = "amqp:" + uri.removeprefix("pika:")
        return cls(URLParameters(amqp_uri))

    def __init__(self, connection_params: ConnectionParameters | URLParameters):
        self.__connection_params = connection_params
        self.__subscriber = Queue[bytes]()
        self.__subscribe_connection = BlockingConnection(self.__connection_params)
        self.__subscribe_channel = self.__subscribe_connection.channel()
        self.__queue_name = cast(
            str, self.__subscribe_channel.queue_declare("", exclusive=True).method.queue
        )
        self.__shutdown_lock = Lock()
        self.__shutdown = False
        self.__subscribe_channel.queue_bind(
            self.__queue_name, "amq.topic", routing_key="#"
        )
        self.__subscribe_thread = Thread(
            target=self.__listen, name="queueio-journal-listener"
        )
        self.__subscribe_thread.start()

        self.__lock = Lock()
        self.__publish_channel = BlockingConnection(self.__connection_params).channel()

    def __listen(self):
        for _, _, body in self.__subscribe_channel.consume(
            self.__queue_name, auto_ack=True
        ):
            self.__subscriber.put(body)

    def subscribe(self) -> Iterator[bytes]:
        while True:
            try:
                yield self.__subscriber.get()
            except ShutDown:
                return

    def publish(self, message: bytes):
        with self.__lock:
            self.__publish_channel.basic_publish(
                exchange="amq.topic",
                routing_key="",
                body=message,
            )

    def shutdown(self):
        with self.__shutdown_lock:
            if self.__shutdown:
                return
            self.__shutdown = True

            lock = Lock()
            lock.acquire()

            def callback():
                self.__subscribe_channel.cancel()
                lock.release()

            self.__subscribe_connection.add_callback_threadsafe(callback)
            with lock:
                pass

            self.__subscribe_thread.join()
            self.__subscriber.shutdown()
