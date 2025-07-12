from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from itertools import chain
from queue import Queue
from queue import ShutDown
from threading import Lock
from threading import Thread
from typing import Any
from typing import cast

import dill
from pika import BlockingConnection


class BusTransport:
    def __init__(self):
        self.__subscriber = Queue[bytes]()
        self.__subscribe_connection = BlockingConnection()
        self.__subscribe_channel = self.__subscribe_connection.channel()
        self.__queue_name = cast(
            str, self.__subscribe_channel.queue_declare("", exclusive=True).method.queue
        )
        self.__subscribe_channel.queue_bind(self.__queue_name, "amq.topic", routing_key="#")
        self.__subscribe_thread = Thread(target=self.__listen, name="qio-bus-transport-listener")
        self.__subscribe_thread.start()

        self.__lock = Lock()
        self.__publish_channel = BlockingConnection().channel()

    def __listen(self):
        for _, _, body in self.__subscribe_channel.consume(self.__queue_name, auto_ack=True):
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


class Bus:
    def __init__(self):
        self.__transport = BusTransport()
        self.__subscriptions: dict[type, set[Queue[Any]]] = {}
        self.__listener = Thread(target=self.__listen, name='qio-bus-listener')
        self.__listener.start()
    
    def __listen(self):
        for message in self.__transport.subscribe():
            self.__remote_receive(message)

    def subscribe[T](self, types: Iterable[type[T]]) -> Queue[T]:
        queue = Queue[T]()
        for type in types:
            self.__subscriptions.setdefault(type, set()).add(queue)
        return queue

    def unsubscribe(self, queue: Queue) -> None:
        """Unsubscribe a queue from all event types."""
        for type, subscriptions in list(self.__subscriptions.items()):
            subscriptions.discard(queue)
            if not subscriptions:
                del self.__subscriptions[type]

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
        self.__transport.publish(dill.dumps(event))

    def __remote_receive(self, body: bytes):
        """Receive and process remote events."""
        event = dill.loads(body)
        self.__distribute(event)

    def publish(self, event: Any):
        """Publish an event to all subscribers of the bus transport.

        Put on the bus transport so that remote and local subscribers
        see the event. Requires that the event is serializable.
        """
        self.__remote_publish(event)

    def publish_local(self, event: Any):
        """Publish only to subscribers of this Bus instance.

        This is useful for events that have properties that are not serializable.
        """
        self.__distribute(event)

    def shutdown(self):
        self.__transport.shutdown()
        self.__listener.join()
        for subscriber in set(chain.from_iterable(self.__subscriptions.values())):
            subscriber.shutdown()

