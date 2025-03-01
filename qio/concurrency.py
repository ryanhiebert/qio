from threading import BoundedSemaphore


class Done(Exception):
    """The concurrency semaphore is shut down."""


class Reservation:
    """An object representing a reserved resource."""


class Concurrency:
    def __init__(self, limit: int, /):
        self._limit = limit
        self._reserved = BoundedSemaphore(limit)
        self._started = BoundedSemaphore(limit)
        self._done = False

    def reserve(self):
        if self._done:
            raise Done
        reserved = self._reserved.acquire()
        if self._done:
            self._reserved.release()
            raise Done
        return reserved

    def start(self):
        if self._done:
            raise Done
        started = self._started.acquire()
        if self._done:
            self._started.release()
            raise Done
        return started

    def stop(self):
        self._reserved.release()
        self._started.release()

    def shutdown(self, *, wait: bool = False):
        if self._done:
            raise Exception("Already shut down.")

        self._done = True
        for _ in range(self._limit if wait else 0):
            self._started.acquire()
        for _ in range(self._limit if wait else 0):
            self._started.release()
