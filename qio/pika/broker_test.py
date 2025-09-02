import threading

import pytest
from pika import ConnectionParameters

from .broker import PikaBroker


@pytest.mark.timeout(2)
def test_prefetch_limits_message_consumption():
    """Verify that prefetch parameter limits message consumption."""
    broker = PikaBroker(ConnectionParameters())
    broker.purge(queue="test-queue")

    # Enqueue more messages than prefetch limit
    for i in range(3):
        broker.enqueue(f"message{i}".encode(), queue="test-queue")

    consumed_messages = []
    prefetch_limit = 2

    def consume_messages():
        consumer = broker.consume(queue="test-queue", prefetch=prefetch_limit)
        for message in consumer:
            consumed_messages.append(message)
            # Don't call start() - this should block after prefetch_limit messages

    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

    # Wait for consumer to reach prefetch limit or timeout
    thread.join(timeout=1.0)

    # Should have consumed exactly prefetch_limit messages and be blocked
    assert len(consumed_messages) == prefetch_limit
    assert thread.is_alive()  # Thread should still be alive (blocked)

    broker.shutdown()


@pytest.mark.timeout(2)
def test_suspend_resume_affects_prefetch_capacity():
    """Verify suspending messages frees capacity and resuming reduces it."""
    broker = PikaBroker(ConnectionParameters())
    broker.purge(queue="test-queue")

    # Enqueue messages
    for i in range(3):
        broker.enqueue(f"msg{i}".encode(), queue="test-queue")

    consumed_messages = []
    prefetch_limit = 2

    def consume_messages():
        consumer = broker.consume(queue="test-queue", prefetch=prefetch_limit)
        for message in consumer:
            consumed_messages.append(message)
            broker.start(message)

    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

    # Wait for initial consumption up to prefetch limit
    thread.join(timeout=0.1)
    assert len(consumed_messages) == prefetch_limit
    assert thread.is_alive()  # Should be blocked

    # Suspend first message - should free capacity for third message
    broker.suspend(consumed_messages[0])
    thread.join(timeout=0.1)
    assert len(consumed_messages) == 3

    # Resume first message - should consume capacity again
    broker.resume(consumed_messages[0])

    broker.shutdown()


@pytest.mark.timeout(2)
def test_complete_message_frees_prefetch_capacity():
    """Verify completing messages frees up capacity."""
    broker = PikaBroker(ConnectionParameters())
    broker.purge(queue="test-queue")

    # Enqueue more messages than prefetch limit
    for i in range(4):
        broker.enqueue(f"test{i}".encode(), queue="test-queue")

    consumed_messages = []
    prefetch_limit = 2

    def consume_messages():
        consumer = broker.consume(queue="test-queue", prefetch=prefetch_limit)
        for message in consumer:
            consumed_messages.append(message)
            broker.start(message)

    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

    # Initial consumption up to prefetch limit
    thread.join(timeout=0.1)
    assert len(consumed_messages) == prefetch_limit
    assert thread.is_alive()  # Should be blocked

    # Complete first message - should allow consuming third message
    broker.complete(consumed_messages[0])
    thread.join(timeout=0.1)
    assert len(consumed_messages) == 3

    # Complete second message - should allow consuming fourth message
    broker.complete(consumed_messages[1])
    thread.join(timeout=0.1)
    assert len(consumed_messages) == 4

    broker.shutdown()


@pytest.mark.timeout(2)
def test_multiple_consumers_independent_prefetch_limits():
    """Verify multiple consumers operate with independent prefetch limits."""
    broker = PikaBroker(ConnectionParameters())
    broker.purge(queue="test-queue")

    # Enqueue enough messages for both consumers
    for i in range(5):
        broker.enqueue(f"msg{i}".encode(), queue="test-queue")

    consumer1_messages = []
    consumer2_messages = []

    def consume_with_limit_2():
        consumer = broker.consume(queue="test-queue", prefetch=2)
        for message in consumer:
            consumer1_messages.append(message)
            broker.start(message)

    def consume_with_limit_3():
        consumer = broker.consume(queue="test-queue", prefetch=3)
        for message in consumer:
            consumer2_messages.append(message)
            broker.start(message)

    thread1 = threading.Thread(target=consume_with_limit_2, daemon=True)
    thread2 = threading.Thread(target=consume_with_limit_3, daemon=True)

    thread1.start()
    thread2.start()

    # Allow both consumers to consume up to their limits
    thread1.join(timeout=0.1)
    thread2.join(timeout=0.1)

    # Both consumers should respect their prefetch limits and be blocked
    total_consumed = len(consumer1_messages) + len(consumer2_messages)
    assert total_consumed == 5  # All messages consumed (2+3=5)
    assert thread1.is_alive()  # Both should be blocked
    assert thread2.is_alive()

    broker.shutdown()
