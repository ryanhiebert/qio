from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from concurrent.futures import Future
from contextlib import suppress
from queue import Queue
from queue import ShutDown
from threading import Event
from threading import Thread
from typing import Any
from typing import AnyStr
from typing import cast

from pika import SelectConnection
from pika import frame
from pika import spec
from pika.channel import Channel
from pika.connection import Parameters
from pika.exceptions import ConnectionClosedByClient


class ThreadsafeConnection:
    def __init__(self, connection_params: Parameters):
        opened = Future()
        self.__closed = Future()
        self.__connection = SelectConnection(
            connection_params,
            lambda _: opened.set_result(None),
            lambda _, exc: opened.set_exception(cast(BaseException, exc)),
            lambda _, exc: self.__closed.set_exception(exc),
        )
        self.__thread = Thread(target=self.__connection.ioloop.start)
        self.__thread.start()
        opened.result()

    def __wait[T](self, fn: Callable[[], T]):
        # TODO: Throw connection exceptions on all waiters
        event = Event()
        self.__connection.ioloop.add_callback_threadsafe(
            lambda: event.set() if fn() else event.set()
        )
        event.wait()

    def channel(self, channel_number: int | None = None) -> ThreadsafeChannel:
        future = Future[Channel]()
        self.__wait(
            lambda: self.__connection.channel(
                channel_number=channel_number,
                on_open_callback=future.set_result,
            )
        )
        return ThreadsafeChannel(self.__wait, future.result())

    def close(self, reply_code: int = 200, reply_text: str = "Normal shutdown"):
        self.__wait(
            lambda: self.__connection.close(
                reply_code=reply_code,
                reply_text=reply_text,
            )
        )
        with suppress(ConnectionClosedByClient):
            self.__closed.result()
        self.__connection.ioloop.stop()
        self.__thread.join()


class ThreadsafeChannel:
    def __init__(
        self,
        wait: Callable[[Callable[[], Any]], None],
        channel: Channel,
    ):
        self.__wait = wait
        self.__channel = channel
        self.__messages = Queue[
            tuple[spec.Basic.Deliver, spec.BasicProperties, bytes]
        ]()
        self.__channel.add_on_close_callback(
            lambda c, e: self.__messages.shutdown(immediate=True)
        )

    def declare_queue(
        self,
        queue: str,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Mapping[str, Any] | None = None,
    ) -> frame.Method[spec.Queue.DeclareOk]:
        future: Future[frame.Method[spec.Queue.DeclareOk]] = Future()
        self.__wait(
            lambda: self.__channel.queue_declare(
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments,
                callback=future.set_result,
            )
        )
        return future.result()

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: AnyStr,
        properties: spec.BasicProperties | None = None,
        mandatory: bool = False,
    ):
        self.__wait(
            lambda: self.__channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=mandatory,
            )
        )

    def purge(self, queue: str) -> frame.Method[spec.Queue.PurgeOk]:
        future: Future[frame.Method[spec.Queue.PurgeOk]] = Future()
        self.__wait(
            lambda: self.__channel.queue_purge(
                queue=queue,
                callback=future.set_result,
            )
        )

        return future.result()

    def consume(
        self,
        queue: str,
        auto_ack: bool = False,
        exclusive: bool = False,
        consumer_tag: str | None = None,
        arguments: Mapping[str, Any] | None = None,
    ) -> frame.Method[spec.Basic.ConsumeOk]:
        future: Future[frame.Method[spec.Basic.ConsumeOk]] = Future()

        self.__wait(
            lambda: self.__channel.basic_consume(
                queue=queue,
                on_message_callback=lambda _, m, p, b: self.__messages.put((m, p, b)),
                auto_ack=auto_ack,
                exclusive=exclusive,
                consumer_tag=consumer_tag,
                arguments=arguments,
                callback=future.set_result,
            )
        )
        return future.result()

    def cancel(self, consumer_tag: str) -> frame.Method[spec.Basic.CancelOk]:
        future: Future[frame.Method[spec.Basic.CancelOk]] = Future()
        self.__wait(
            lambda: self.__channel.basic_cancel(
                consumer_tag=consumer_tag,
                callback=lambda r: future.set_result(r),
            )
        )
        return future.result()

    def qos(
        self,
        prefetch_size: int = 0,
        prefetch_count: int = 0,
        global_qos: bool = False,
    ) -> frame.Method[spec.Basic.QosOk]:
        future: Future[frame.Method[spec.Basic.QosOk]] = Future()
        self.__wait(
            lambda: self.__channel.basic_qos(
                prefetch_size=prefetch_size,
                prefetch_count=prefetch_count,
                global_qos=global_qos,
                callback=future.set_result,
            )
        )
        return future.result()

    def ack(self, delivery_tag: int = 0, multiple: bool = False):
        self.__wait(
            lambda: self.__channel.basic_ack(
                delivery_tag=delivery_tag,
                multiple=multiple,
            )
        )

    def messages(
        self,
    ) -> Iterator[tuple[spec.Basic.Deliver, spec.BasicProperties, bytes]]:
        while True:
            try:
                yield self.__messages.get()
            except ShutDown:
                return
