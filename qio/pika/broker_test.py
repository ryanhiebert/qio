import threading

import pytest
from pika import ConnectionParameters

from qio.queuespec import QueueSpec

from .broker import PikaBroker


def test_pika_broker_from_uri():
    """Test PikaBroker.from_uri with localhost URI creates broker successfully."""
    broker = PikaBroker.from_uri("pika://localhost:5672")
    assert isinstance(broker, PikaBroker)
    broker.shutdown()


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
        queuespec = QueueSpec(queues=["test-queue"], concurrency=prefetch_limit)
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumed_messages.append(message)
            # Don't call start() - this should block after prefetch_limit messages

    thread = threading.Thread(target=consume_messages)
    thread.start()

    # Wait for consumer to reach prefetch limit or timeout
    thread.join(timeout=1.0)

    # Should have consumed exactly prefetch_limit messages and be blocked
    assert len(consumed_messages) == prefetch_limit
    assert thread.is_alive()  # Thread should still be alive (blocked)

    broker.shutdown()
    thread.join(timeout=1.0)  # Clean up thread


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
        queuespec = QueueSpec(queues=["test-queue"], concurrency=prefetch_limit)
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumed_messages.append(message)
            broker.start(message)

    thread = threading.Thread(target=consume_messages)
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
    thread.join(timeout=1.0)  # Clean up thread


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
        queuespec = QueueSpec(queues=["test-queue"], concurrency=prefetch_limit)
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumed_messages.append(message)
            broker.start(message)

    thread = threading.Thread(target=consume_messages)
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
    thread.join(timeout=1.0)  # Clean up thread


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
        queuespec = QueueSpec(queues=["test-queue"], concurrency=2)
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumer1_messages.append(message)
            broker.start(message)

    def consume_with_limit_3():
        queuespec = QueueSpec(queues=["test-queue"], concurrency=3)
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumer2_messages.append(message)
            broker.start(message)

    thread1 = threading.Thread(target=consume_with_limit_2)
    thread2 = threading.Thread(target=consume_with_limit_3)

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
    thread1.join(timeout=1.0)  # Clean up threads
    thread2.join(timeout=1.0)


def test_consume_rejects_empty_queues():
    """Verify broker rejects QueueSpec with no queues."""
    broker = PikaBroker(ConnectionParameters())
    queuespec = QueueSpec(queues=[], concurrency=2)

    with pytest.raises(ValueError, match="QueueSpec must have at least one queue"):
        list(broker.consume(queuespec))

    broker.shutdown()


def test_consume_rejects_multiple_queues():
    """Verify broker rejects QueueSpec with multiple queues."""
    broker = PikaBroker(ConnectionParameters())
    queuespec = QueueSpec(queues=["queue1", "queue2"], concurrency=2)

    with pytest.raises(ValueError, match="Only one queue is supported"):
        list(broker.consume(queuespec))

    broker.shutdown()
