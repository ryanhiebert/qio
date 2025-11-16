import importlib
import os
import tomllib
from collections.abc import Generator
from collections.abc import Iterable
from concurrent.futures import Future
from contextlib import contextmanager
from pathlib import Path

from .broker import Broker
from .consumer import Consumer
from .invocation import Invocation
from .invocation import InvocationCompleted
from .invocation import InvocationSubmitted
from .invocation import deserialize
from .invocation import serialize
from .journal import Journal
from .message import Message
from .queue import Queue
from .queue import ShutDown
from .queuespec import QueueSpec
from .registry import ROUTINE_REGISTRY
from .result import Err
from .result import Ok
from .routine import Routine
from .stream import Stream
from .thread import Thread


class QueueIO:
    def __init__(
        self,
        *,
        broker: Broker | None = None,
        journal: Journal | None = None,
    ):
        self.__broker = broker or self.__default_broker()
        self.__stream = Stream(journal or self.__default_journal())
        self.__invocations = dict[Invocation, Message]()
        self.__register_routines()

    def __pyproject(self) -> Path | None:
        for path in [cwd := Path.cwd(), *cwd.parents]:
            candidate = path / "pyproject.toml"
            if candidate.is_file():
                return candidate
        return None

    def __config(self) -> dict:
        if pyproject := self.__pyproject():
            with pyproject.open("rb") as f:
                config = tomllib.load(f)
            return config.get("tool", {}).get("queueio", {})
        return {}

    def __default_broker(self) -> Broker:
        broker_uri = os.environ.get("QUEUEIO_BROKER")
        if not broker_uri:
            config = self.__config()
            broker_uri = config.get("broker")
            if not broker_uri:
                raise ValueError(
                    "No broker URI configured. Set QUEUEIO_BROKER env var "
                    "or add 'broker' to [tool.queueio] in pyproject.toml"
                )

        if not broker_uri.startswith("amqp:"):
            raise ValueError(f"URI scheme must be 'amqp:', got: {broker_uri}")

        from .pika.broker import PikaBroker

        return PikaBroker.from_uri(broker_uri)

    def __default_journal(self) -> Journal:
        journal_uri = os.environ.get("QUEUEIO_JOURNAL")
        if not journal_uri:
            config = self.__config()
            journal_uri = config.get("journal")
            if not journal_uri:
                raise ValueError(
                    "No journal URI configured. Set QUEUEIO_JOURNAL env var "
                    "or add 'journal' to [tool.queueio] in pyproject.toml"
                )

        if not journal_uri.startswith("amqp:"):
            raise ValueError(f"URI scheme must be 'amqp:', got: {journal_uri}")

        from .pika.journal import PikaJournal

        return PikaJournal.from_uri(journal_uri)

    def __register_routines(self):
        """Load routine modules from pyproject.toml."""
        config = self.__config()
        modules = config.get("register", [])

        for module_name in modules:
            importlib.import_module(module_name)

    def run[R](self, invocation: Invocation[R], /) -> R:
        with self.invocation_handler():
            return invocation.start().result()

    def purge(self, *, queue: str):
        self.__broker.purge(queue=queue)

    def routine(self, routine_name: str, /) -> Routine:
        return ROUTINE_REGISTRY[routine_name]

    def routines(self) -> list[Routine]:
        """Return all registered routines."""
        return list(ROUTINE_REGISTRY.values())

    def subscribe[T](self, types: Iterable[type[T]]) -> Queue[T]:
        return self.__stream.subscribe(types)

    def unsubscribe(self, queue: Queue):
        return self.__stream.unsubscribe(queue)

    @contextmanager
    def invocation_handler(self) -> Generator[Future]:
        waiting: dict[str, Future] = {}
        events = self.subscribe({InvocationCompleted})

        def resolver():
            while True:
                try:
                    event = events.get()
                except ShutDown:
                    break

                match event:
                    case InvocationCompleted(id=invocation_id, result=Ok(value)):
                        if invocation_id in waiting:
                            future = waiting.pop(invocation_id)
                            future.set_result(value)
                    case InvocationCompleted(id=invocation_id, result=Err(exception)):
                        if invocation_id in waiting:
                            future = waiting.pop(invocation_id)
                            future.set_exception(exception)

        resolver_thread = Thread(target=resolver)
        resolver_thread.start()

        def handler(invocation: Invocation, /) -> Future:
            future = Future()
            waiting[invocation.id] = future
            self.submit(invocation)
            return future

        try:
            with Invocation.handler(handler):
                yield resolver_thread.future
        finally:
            self.unsubscribe(events)
            resolver_thread.join()

    def submit(self, invocation: Invocation, /):
        """Submit an invocation to be run in the background."""
        routine = self.routine(invocation.routine)
        self.__stream.publish(
            InvocationSubmitted(
                id=invocation.id,
                routine=invocation.routine,
                args=invocation.args,
                kwargs=invocation.kwargs,
            )
        )
        queue = routine.queue
        self.__broker.enqueue(serialize(invocation), queue=queue)

    def consume(self, queuespec: QueueSpec, /) -> Consumer:
        return Consumer(
            stream=self.__stream,
            receiver=self.__broker.receive(queuespec),
            deserialize=deserialize,
        )

    def shutdown(self):
        """Shut down all components."""
        self.__broker.shutdown()
        self.__stream.shutdown()
