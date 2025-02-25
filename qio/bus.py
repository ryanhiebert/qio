from collections.abc import Iterable
from queue import SimpleQueue
from typing import Any


class Bus:
    def __init__(self):
        self.__subscriptions: dict[type, set[SimpleQueue[Any]]] = {}

    def subscribe[T](self, type: type[T] | Iterable[type[T]]) -> SimpleQueue[T]:
        queue = SimpleQueue[T]()
        types = type if isinstance(type, Iterable) else {type}
        for type in types:
            self.__subscriptions.setdefault(type, set()).add(queue)
        return queue

    def publish(self, event: Any):
        print(event)
        subscribers = {
            subscription
            for type, subscriptions in self.__subscriptions.items()
            for subscription in subscriptions
            if isinstance(event, type)
        }
        for subscriber in subscribers:
            subscriber.put(event)
