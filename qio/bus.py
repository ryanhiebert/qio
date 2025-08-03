from collections.abc import Iterable
from itertools import chain
from queue import Queue
from threading import Thread
from typing import Any

import dill

from .transport import Transport


class Bus:
    def __init__(self, transport: Transport):
        self.__transport = transport
        self.__subscriptions: dict[type, set[Queue[Any]]] = {}
        self.__listener = Thread(target=self.__listen, name="qio-bus-listener")
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
        queue.shutdown(immediate=True)

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
