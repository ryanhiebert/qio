from threading import BoundedSemaphore
from threading import Lock


class Done(Exception):
    """The concurrency semaphore is shut down."""


class Concurrency:
    def __init__(self, limit: int, /):
        self._limit = limit
        self._reserved = BoundedSemaphore(limit)
        self._started = BoundedSemaphore(limit)
        self._done = False
        self._waitlock = Lock()

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

    def shutdown(self, *, wait: bool):
        self._done = True
        if not wait:
            return

        with self._waitlock:
            # Only one thread can wait for all concurrency at a time.
            for _ in range(self._limit):
                self._started.acquire()
        for _ in range(self._limit):
            self._started.release()
