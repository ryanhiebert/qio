import threading

import pytest

from qio.queuespec import QueueSpec

from .broker import StubBroker


def test_prefetch_limits_message_consumption():
    """Verify that prefetch parameter limits message consumption."""
    broker = StubBroker()
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


def test_suspend_resume_affects_prefetch_capacity():
    """Verify suspending messages frees capacity and resuming reduces it."""
    broker = StubBroker()
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


def test_complete_message_frees_prefetch_capacity():
    """Verify completing messages frees up capacity."""
    broker = StubBroker()
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


def test_multiple_consumers_independent_prefetch_limits():
    """Verify multiple consumers operate with independent prefetch limits."""
    broker = StubBroker()
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
    broker = StubBroker()
    queuespec = QueueSpec(queues=[], concurrency=2)

    with pytest.raises(ValueError, match="QueueSpec must have at least one queue"):
        list(broker.consume(queuespec))

    broker.shutdown()


def test_consume_supports_multiple_queues():
    """Verify broker can consume from multiple queues."""
    broker = StubBroker()
    broker.purge(queue="queue1")
    broker.purge(queue="queue2")

    # Enqueue messages to different queues
    broker.enqueue(b"msg1", queue="queue1")
    broker.enqueue(b"msg2", queue="queue2")
    broker.enqueue(b"msg3", queue="queue1")

    queuespec = QueueSpec(queues=["queue1", "queue2"], concurrency=3)
    consumed_messages = []

    def consume_messages():
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumed_messages.append(message.body)
            broker.start(message)
            if len(consumed_messages) >= 3:
                break

    thread = threading.Thread(target=consume_messages)
    thread.start()
    thread.join(timeout=1.0)

    # Should consume all 3 messages from both queues
    assert len(consumed_messages) == 3
    assert b"msg1" in consumed_messages
    assert b"msg2" in consumed_messages
    assert b"msg3" in consumed_messages

    broker.shutdown()
    thread.join(timeout=1.0)  # Clean up thread


def test_multiple_queues_with_mixed_empty_and_filled():
    """Verify broker handles multiple queues with some empty, some with messages."""
    broker = StubBroker()
    broker.purge(queue="empty1")
    broker.purge(queue="filled")
    broker.purge(queue="empty2")
    broker.purge(queue="also_filled")

    # Add messages to some queues but not others
    broker.enqueue(b"message_a", queue="filled")
    broker.enqueue(b"message_b", queue="also_filled")
    broker.enqueue(b"message_c", queue="filled")

    consumed_messages = []
    queuespec = QueueSpec(
        queues=["empty1", "filled", "empty2", "also_filled"], concurrency=3
    )

    def consume_messages():
        consumer = broker.consume(queuespec)
        for message in consumer:
            consumed_messages.append(message.body)
            broker.start(message)
            if len(consumed_messages) >= 3:
                break

    thread = threading.Thread(target=consume_messages)
    thread.start()
    thread.join(timeout=1.0)

    # Should consume all 3 messages from the non-empty queues
    assert len(consumed_messages) == 3
    expected_messages = {b"message_a", b"message_b", b"message_c"}
    assert set(consumed_messages) == expected_messages

    broker.shutdown()
    thread.join(timeout=1.0)  # Clean up thread


def test_duplicate_queue_names_get_additional_priority():
    """Verify that listing a queue multiple times gives it proportional priority.

    This tests an unintuitive but intentional behavior: when a queue appears
    multiple times in the queuespec, it gets proportionally higher selection
    probability in the round-robin selection.
    """
    broker = StubBroker()
    broker.purge(queue="priority_queue")
    broker.purge(queue="normal_queue")

    # Enqueue equal numbers of messages to both queues
    for i in range(20):
        broker.enqueue(f"priority_{i}".encode(), queue="priority_queue")
        broker.enqueue(f"normal_{i}".encode(), queue="normal_queue")

    consumed_messages = []
    selection_order = []  # Track the order messages were consumed

    # List priority_queue 3 times, normal_queue once
    # This gives priority_queue 3 out of 4 chances to be selected
    queuespec = QueueSpec(
        queues=["priority_queue", "normal_queue", "priority_queue", "priority_queue"],
        concurrency=40,
    )

    def consume_messages():
        consumer = broker.consume(queuespec)
        for message in consumer:
            msg_text = message.body.decode()
            consumed_messages.append(msg_text)
            if msg_text.startswith("priority_"):
                selection_order.append("P")
            else:
                selection_order.append("N")
            broker.start(message)
            if len(consumed_messages) >= 40:
                break

    thread = threading.Thread(target=consume_messages)
    thread.start()
    thread.join(timeout=2.0)

    # Should consume all 40 messages
    assert len(consumed_messages) == 40

    priority_count = selection_order.count("P")
    normal_count = selection_order.count("N")

    # Both queues should be fully consumed
    assert priority_count == 20
    assert normal_count == 20

    # The key behavioral test: Due to priority_queue being listed 3 times vs
    # normal_queue 1 time, we should see proportional selection following a
    # pattern close to 3:1 ratio when both queues have messages available.

    selection_pattern = "".join(selection_order)

    # Analyze the pattern while both queues have messages (first ~26 selections)
    # After that, only one queue has messages left
    pattern_with_both = selection_order[:26]  # Analyze first 26 when both active
    p_count_early = pattern_with_both.count("P")

    # With 3:1 weighting, expect roughly 3/4 of selections to be P when both have
    # messages. Allow some variance but verify the proportional behavior
    actual_p_ratio = p_count_early / len(pattern_with_both)

    # The unintuitive behavior: priority_queue gets selected proportionally more
    # often (roughly 3:1 ratio) due to being listed 3 times vs 1 time
    assert actual_p_ratio > 0.6, (
        f"Expected priority queue to be selected ~75% of time due to 3:1 weighting, "
        f"but got {actual_p_ratio:.2%}. Pattern: {selection_pattern}"
    )

    # Count transitions to verify proper interleaving (not just clustering)
    transitions = sum(
        1
        for i in range(1, len(selection_order))
        if selection_order[i] != selection_order[i - 1]
    )

    # Should have reasonable interleaving, not just two big blocks
    assert transitions >= 8, (
        f"Expected reasonable interleaving between queues, but only {transitions} "
        f"transitions in pattern: {selection_pattern}"
    )

    broker.shutdown()
    thread.join(timeout=1.0)  # Clean up thread


def test_empty_queue_cycling_fairness():
    """Test that empty queues don't cause unfair cycling in round-robin selection.

    Edge case: With queues [A, B, C] where A is empty, incorrect cycling
    (always advance by 1) gives B extra turns: A->B (B wins), B->C (C wins),
    C->A (A empty, B wins), resulting in B getting 2/3 instead of 1/2.

    Correct cycling should advance based on which queue actually provided
    the message, not just cycle by a fixed amount.
    """
    broker = StubBroker()
    broker.purge(queue="empty")
    broker.purge(queue="queue1")
    broker.purge(queue="queue2")

    # Only queue1 and queue2 have messages, empty is always empty
    for i in range(12):
        broker.enqueue(f"msg1_{i}".encode(), queue="queue1")
        broker.enqueue(f"msg2_{i}".encode(), queue="queue2")

    consumed_messages = []
    selection_order = []

    # Put empty queue in middle to test the cycling bug
    queuespec = QueueSpec(queues=["queue1", "empty", "queue2"], concurrency=24)

    def consume_messages():
        consumer = broker.consume(queuespec)
        for message in consumer:
            msg_text = message.body.decode()
            consumed_messages.append(msg_text)
            if "msg1_" in msg_text:
                selection_order.append("1")
            else:
                selection_order.append("2")
            broker.start(message)
            if len(consumed_messages) >= 24:
                break

    thread = threading.Thread(target=consume_messages)
    thread.start()
    thread.join(timeout=2.0)

    assert len(consumed_messages) == 24

    queue1_count = selection_order.count("1")
    queue2_count = selection_order.count("2")

    # Both queues should be fully consumed
    assert queue1_count == 12
    assert queue2_count == 12

    # Key test: In the first portion when both queues have messages,
    # they should split roughly equally (not 2:1 due to cycling bug)
    first_12 = selection_order[:12]
    q1_early = first_12.count("1")
    q2_early = first_12.count("2")

    # With correct cycling, should be close to 6/6
    # With broken cycling, would be closer to 8/4
    assert abs(q1_early - q2_early) <= 2, (
        f"Expected roughly equal split, got {q1_early}:{q2_early} "
        f"in first 12. Pattern: {''.join(first_12)}"
    )

    broker.shutdown()
    thread.join(timeout=1.0)
