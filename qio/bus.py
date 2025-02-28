from collections.abc import Iterable
from itertools import chain
from queue import Queue
from typing import Any


class Bus:
    def __init__(self):
        self.__subscriptions: dict[type, set[Queue[Any]]] = {}

    def subscribe[T](self, type: type[T] | Iterable[type[T]]) -> Queue[T]:
        queue = Queue[T]()
        types = type if isinstance(type, Iterable) else {type}
        for type in types:
            self.__subscriptions.setdefault(type, set()).add(queue)
        return queue

    def publish(self, event: Any):
        subscribers = {
            subscription
            for type, subscriptions in self.__subscriptions.items()
            for subscription in subscriptions
            if isinstance(event, type)
        }
        for subscriber in subscribers:
            subscriber.put(event)

    def shutdown(self):
        for subscriber in set(chain.from_iterable(self.__subscriptions.values())):
            subscriber.shutdown()
