from collections.abc import Callable
from collections.abc import Iterable
from itertools import chain
from queue import Queue
from threading import Lock
from typing import Any
from typing import cast

import dill
from pika import BlockingConnection

from .executor import Executor


class Bus:
    def __init__(self):
        self.__subscriptions: dict[type, set[Queue[Any]]] = {}
        self.__channel_lock = Lock()
        self.__channel = BlockingConnection().channel()
        self.__listener_lock = Lock()
        self.__listener: Listener | None = None

    def subscribe[T](self, types: Iterable[type[T]]) -> Queue[T]:
        # Ensure that the listener is running
        with self.__listener_lock:
            if self.__listener is None:
                self.__listener = Listener(self.__remote_receive)

        queue = Queue[T]()
        for type in types:
            self.__subscriptions.setdefault(type, set()).add(queue)
        return queue

    def __distribute(self, event: Any):
        """Local-only distribution of events to subscribers."""
        subscribers = {
            subscription
            for type, subscriptions in self.__subscriptions.items()
            for subscription in subscriptions
            if isinstance(event, type)
        }
        for subscriber in subscribers:
            subscriber.put(event)

    def __remote_publish(self, event: Any):
        """Remote distribution of events to subscribers."""
        body = dill.dumps(event)
        with self.__channel_lock:
            self.__channel.basic_publish(
                exchange="amq.topic",
                routing_key="#",
                body=body,
            )

    def __remote_receive(self, body: bytes):
        """Receive and process remote events."""
        event = dill.loads(body)
        self.__distribute(event)

    def publish(self, event: Any):
        """Publish to all bus subscribers.
        
        Put on the backing event broker so that remote and local subscribers
        see the event. Requires that the event is serializable.
        """
        self.__remote_publish(event)
    
    def publish_local(self, event: Any):
        """Publish only to local subscribers.
        
        This is useful for events that have properties that are not serializable.
        """
        self.__distribute(event)

    def shutdown(self):
        with self.__listener_lock:
            if self.__listener:
                self.__listener.shutdown()
        for subscriber in set(chain.from_iterable(self.__subscriptions.values())):
            subscriber.shutdown()


class Listener:
    def __init__(self, receive: Callable[[bytes], None], /):
        self.running: bool = False
        self.__receive = receive
        self.__connection = BlockingConnection()
        self.__channel = self.__connection.channel()
        self.__queue = cast(
            str, self.__channel.queue_declare("", exclusive=True).method.queue
        )
        self.__channel.queue_bind(self.__queue, "amq.topic", routing_key="#")
        self.__executor = Executor(name="qio-bus-listener")
        self.__executor.submit(self.__listen)

    def shutdown(self):
        lock = Lock()
        lock.acquire()

        def callback():
            self.__channel.cancel()
            lock.release()

        self.__connection.add_callback_threadsafe(callback)
        with lock:
            return

    def __listen(self):
        self.running = True
        try:
            for _, _, body in self.__channel.consume(self.__queue, auto_ack=True):
                self.__receive(body)
        finally:
            self.running = False
