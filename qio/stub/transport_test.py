import itertools
import threading
import time

import pytest

from .transport import StubTransport


@pytest.mark.timeout(2)
def test_stub_transport_multiple_messages():
    """Verify that multiple messages can be published and received in order."""
    transport = StubTransport()

    messages = [b"message1", b"message2", b"message3"]
    for msg in messages:
        transport.publish(msg)

    subscriber = transport.subscribe()
    received_messages = []

    received_messages = list(itertools.islice(subscriber, len(messages)))

    assert len(received_messages) == len(messages)
    assert received_messages == messages

    transport.shutdown()


@pytest.mark.timeout(2)
def test_stub_transport_shutdown():
    """Verify that shutdown stops subscribers and they receive all pending messages."""
    transport = StubTransport()

    transport.publish(b"test message 1")
    transport.publish(b"test message 2")

    received_messages = []

    def subscriber_thread():
        for message in transport.subscribe():
            received_messages.append(message)

    thread = threading.Thread(target=subscriber_thread)
    thread.start()

    time.sleep(0.1)
    transport.shutdown()

    thread.join(timeout=2.0)
    assert received_messages == [b"test message 1", b"test message 2"]


@pytest.mark.timeout(2)
def test_stub_transport_concurrent_publishing():
    """Verify that multiple threads can publish messages concurrently without issues."""
    transport = StubTransport()
    received_messages = []

    def publisher(start_num: int, count: int):
        for i in range(count):
            message = f"message_{start_num + i}".encode()
            transport.publish(message)

    def subscriber():
        for message in transport.subscribe():
            received_messages.append(message)

    subscriber_thread = threading.Thread(target=subscriber)
    subscriber_thread.start()

    threads = []
    messages_per_thread = 5
    num_threads = 3

    for i in range(num_threads):
        thread = threading.Thread(
            target=publisher, args=(i * messages_per_thread, messages_per_thread)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    time.sleep(0.2)

    expected_total = num_threads * messages_per_thread
    assert len(received_messages) == expected_total

    transport.shutdown()
    subscriber_thread.join(timeout=1.0)
