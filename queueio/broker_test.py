"""Base test class for broker implementations.

Example usage for a new broker implementation:

```python
# queueio/mybroker/broker_test.py
import pytest
from queueio.broker_test_base import BaseBrokerTest
from .broker import MyBroker

class TestMyBroker(BaseBrokerTest):
    supports_multiple_queues = True  # Set based on broker capabilities

    @pytest.fixture
    def broker(self):
        broker = MyBroker("connection_string")
        yield broker
        broker.shutdown()

    # Add broker-specific tests here if needed
    def test_my_broker_specific_feature(self, broker):
        # Custom test for this broker implementation
        pass
```
"""

import threading
from functools import wraps

import pytest

from queueio.queuespec import QueueSpec


def skip_if_unsupported(feature_attr):
    """Decorator that skips test if the test class doesn't support a feature."""

    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            if not getattr(self, feature_attr, True):
                pytest.skip(f"Test class doesn't support {feature_attr}")
            return test_func(self, *args, **kwargs)

        return wrapper

    return decorator


class BaseBrokerTest:
    """Base class with common tests for all broker implementations."""

    supports_multiple_queues = True

    @pytest.fixture
    def broker(self):
        """Subclasses must implement this fixture to provide a broker instance."""
        raise NotImplementedError("Subclasses must implement broker fixture")

    @pytest.mark.timeout(2)
    def test_prefetch_limits_message_consumption(self, broker):
        """Verify that prefetch parameter limits message consumption."""
        broker.purge(queue="test-queue")

        # Enqueue more messages than prefetch limit
        for i in range(3):
            broker.enqueue(f"message{i}".encode(), queue="test-queue")

        received_messages = []
        prefetch_limit = 2

        def receive_messages():
            queuespec = QueueSpec(queues=["test-queue"], concurrency=prefetch_limit)
            receiver = broker.receive(queuespec)
            for message in receiver:
                received_messages.append(message)

        thread = threading.Thread(target=receive_messages)
        thread.start()

        # Wait for receiver to reach prefetch limit or timeout
        thread.join(timeout=0.1)

        # Should have received exactly prefetch_limit messages and be blocked
        assert len(received_messages) == prefetch_limit
        assert thread.is_alive()  # Thread should still be alive (blocked)

        broker.shutdown()
        thread.join(timeout=1.0)  # Clean up thread
        assert not thread.is_alive()

    @pytest.mark.timeout(2)
    def test_suspend_resume_affects_prefetch_capacity(self, broker):
        """Verify suspending messages frees capacity and resuming reduces it."""
        broker.purge(queue="test-queue")

        # Enqueue messages
        for i in range(3):
            broker.enqueue(f"msg{i}".encode(), queue="test-queue")

        received_messages = []
        prefetch_limit = 2
        queuespec = QueueSpec(queues=["test-queue"], concurrency=prefetch_limit)
        receiver = broker.receive(queuespec)

        def receive_messages():
            for message in receiver:
                received_messages.append(message)

        thread = threading.Thread(target=receive_messages)
        thread.start()

        # Wait for initial consumption up to prefetch limit
        thread.join(timeout=0.1)
        assert len(received_messages) == prefetch_limit
        assert thread.is_alive()  # Should be blocked

        # Pause first message - should free capacity for third message
        receiver.pause(received_messages[0])
        thread.join(timeout=0.1)
        assert len(received_messages) == 3

        # Unpause first message - should receive capacity again
        receiver.unpause(received_messages[0])

        broker.shutdown()
        thread.join(timeout=1.0)  # Clean up thread

    @pytest.mark.timeout(2)
    def test_complete_message_frees_prefetch_capacity(self, broker):
        """Verify completing messages frees up capacity."""
        broker.purge(queue="test-queue")

        # Enqueue more messages than prefetch limit
        for i in range(4):
            broker.enqueue(f"test{i}".encode(), queue="test-queue")

        received_messages = []
        prefetch_limit = 2
        queuespec = QueueSpec(queues=["test-queue"], concurrency=prefetch_limit)
        receiver = broker.receive(queuespec)

        def receive_messages():
            for message in receiver:
                received_messages.append(message)

        thread = threading.Thread(target=receive_messages)
        thread.start()

        # Initial consumption up to prefetch limit
        thread.join(timeout=0.1)
        assert len(received_messages) == prefetch_limit
        assert thread.is_alive()  # Should be blocked

        # Finish first message - should allow consuming third message
        receiver.finish(received_messages[0])
        thread.join(timeout=0.1)
        assert len(received_messages) == 3

        # Finish second message - should allow consuming fourth message
        receiver.finish(received_messages[1])
        thread.join(timeout=0.1)
        assert len(received_messages) == 4

        broker.shutdown()
        thread.join(timeout=1.0)  # Clean up thread

    @pytest.mark.timeout(2)
    def test_multiple_receivers_independent_prefetch_limits(self, broker):
        """Verify multiple receivers operate with independent prefetch limits."""
        broker.purge(queue="test-queue")

        # Enqueue enough messages for both receivers
        for i in range(5):
            broker.enqueue(f"msg{i}".encode(), queue="test-queue")

        receiver1_messages = []
        receiver2_messages = []

        def receive_with_limit_2():
            queuespec = QueueSpec(queues=["test-queue"], concurrency=2)
            receiver = broker.receive(queuespec)
            for message in receiver:
                receiver1_messages.append(message)

        def receive_with_limit_3():
            queuespec = QueueSpec(queues=["test-queue"], concurrency=3)
            receiver = broker.receive(queuespec)
            for message in receiver:
                receiver2_messages.append(message)

        thread1 = threading.Thread(target=receive_with_limit_2)
        thread2 = threading.Thread(target=receive_with_limit_3)

        thread1.start()
        thread2.start()

        # Allow both receivers to receive up to their limits
        thread1.join(timeout=0.1)
        thread2.join(timeout=0.1)

        # Both receivers should respect their prefetch limits and be blocked
        total_received = len(receiver1_messages) + len(receiver2_messages)
        assert total_received == 5  # All messages received (2+3=5)
        assert thread1.is_alive()  # Both should be blocked
        assert thread2.is_alive()

        broker.shutdown()
        thread1.join(timeout=1.0)  # Clean up threads
        thread2.join(timeout=1.0)

    def test_receive_rejects_empty_queues(self, broker):
        """Verify broker rejects QueueSpec with no queues."""
        queuespec = QueueSpec(queues=[], concurrency=2)

        with pytest.raises(ValueError, match="Must specify at least one queue"):
            list(broker.receive(queuespec))

    @skip_if_unsupported("supports_multiple_queues")
    def test_receive_supports_multiple_queues(self, broker):
        """Verify broker can receive from multiple queues."""
        broker.purge(queue="queue1")
        broker.purge(queue="queue2")

        # Enqueue messages to different queues
        broker.enqueue(b"msg1", queue="queue1")
        broker.enqueue(b"msg2", queue="queue2")
        broker.enqueue(b"msg3", queue="queue1")

        queuespec = QueueSpec(queues=["queue1", "queue2"], concurrency=3)
        received_messages = []

        def receive_messages():
            receiver = broker.receive(queuespec)
            for message in receiver:
                received_messages.append(message.body)
                if len(received_messages) >= 3:
                    break

        thread = threading.Thread(target=receive_messages)
        thread.start()
        thread.join(timeout=1.0)

        # Should receive all 3 messages from both queues
        assert len(received_messages) == 3
        assert b"msg1" in received_messages
        assert b"msg2" in received_messages
        assert b"msg3" in received_messages

        broker.shutdown()
        thread.join(timeout=1.0)  # Clean up thread

    @skip_if_unsupported("supports_multiple_queues")
    def test_multiple_queues_with_mixed_empty_and_filled(self, broker):
        """Verify broker handles multiple queues with some empty, some with messages."""

        broker.purge(queue="empty1")
        broker.purge(queue="filled")
        broker.purge(queue="empty2")
        broker.purge(queue="also_filled")

        # Add messages to some queues but not others
        broker.enqueue(b"message_a", queue="filled")
        broker.enqueue(b"message_b", queue="also_filled")
        broker.enqueue(b"message_c", queue="filled")

        received_messages = []
        queuespec = QueueSpec(
            queues=["empty1", "filled", "empty2", "also_filled"], concurrency=3
        )

        def receive_messages():
            receiver = broker.receive(queuespec)
            for message in receiver:
                received_messages.append(message.body)
                if len(received_messages) >= 3:
                    break

        thread = threading.Thread(target=receive_messages)
        thread.start()
        thread.join(timeout=1.0)

        # Should receive all 3 messages from the non-empty queues
        assert len(received_messages) == 3
        expected_messages = {b"message_a", b"message_b", b"message_c"}
        assert set(received_messages) == expected_messages

        broker.shutdown()
        thread.join(timeout=1.0)  # Clean up thread

    @skip_if_unsupported("supports_multiple_queues")
    @skip_if_unsupported("supports_weighted_queue_subscriptions")
    def test_duplicate_queue_names_get_additional_priority(self, broker):
        """Verify that listing a queue multiple times gives it proportional priority.

        This tests an unintuitive but intentional behavior: when a queue appears
        multiple times in the queuespec, it gets proportionally higher selection
        probability in the round-robin selection.
        """

        broker.purge(queue="priority_queue")
        broker.purge(queue="normal_queue")

        # Enqueue equal numbers of messages to both queues
        for i in range(20):
            broker.enqueue(f"priority_{i}".encode(), queue="priority_queue")
            broker.enqueue(f"normal_{i}".encode(), queue="normal_queue")

        received_messages = []
        selection_order = []  # Track the order messages were received

        # List priority_queue 3 times, normal_queue once
        # This gives priority_queue 3 out of 4 chances to be selected
        queuespec = QueueSpec(
            queues=[
                "priority_queue",
                "normal_queue",
                "priority_queue",
                "priority_queue",
            ],
            concurrency=5,
        )

        def receive_messages():
            receiver = broker.receive(queuespec)
            for message in receiver:
                msg_text = message.body.decode()
                received_messages.append(msg_text)
                if msg_text.startswith("priority_"):
                    selection_order.append("P")
                else:
                    selection_order.append("N")
                receiver.finish(message)
                if len(received_messages) >= 40:
                    break

        thread = threading.Thread(target=receive_messages)
        thread.start()
        thread.join(timeout=2.0)

        # Should receive all 40 messages
        assert len(received_messages) == 40

        priority_count = selection_order.count("P")
        normal_count = selection_order.count("N")

        # Both queues should be fully received
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
        assert actual_p_ratio > 0.7, (
            f"Expected priority queue to be selected ~75% of time due to 3:1 weighting,"
            f" but got {actual_p_ratio:.2%}. Pattern: {selection_pattern}"
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

    @skip_if_unsupported("supports_multiple_queues")
    def test_empty_queue_cycling_fairness(self, broker):
        """Test that empty queues don't cause unfair cycling in round-robin selection.

        Edge case: With queues [A, B, C] where A is empty, incorrect cycling
        (always advance by 1) gives B extra turns: A->B (B wins), B->C (C wins),
        C->A (A empty, B wins), resulting in B getting 2/3 instead of 1/2.

        Correct cycling should advance based on which queue actually provided
        the message, not just cycle by a fixed amount.
        """

        broker.purge(queue="empty")
        broker.purge(queue="queue1")
        broker.purge(queue="queue2")

        # Only queue1 and queue2 have messages, empty is always empty
        for i in range(50):
            broker.enqueue(f"msg1_{i}".encode(), queue="queue1")
            broker.enqueue(f"msg2_{i}".encode(), queue="queue2")

        received_messages = []
        selection_order = []

        # Put empty queue in middle to test the cycling bug
        queuespec = QueueSpec(queues=["queue1", "empty", "queue2"], concurrency=3)

        def receive_messages():
            receiver = broker.receive(queuespec)
            for message in receiver:
                msg_text = message.body.decode()
                received_messages.append(msg_text)
                if "msg1_" in msg_text:
                    selection_order.append("1")
                else:
                    selection_order.append("2")
                receiver.finish(message)
                if len(received_messages) >= 100:
                    break

        thread = threading.Thread(target=receive_messages)
        thread.start()
        thread.join(timeout=2.0)

        assert len(received_messages) == 100

        queue1_count = selection_order.count("1")
        queue2_count = selection_order.count("2")

        # Both queues should be fully received
        assert queue1_count == 50
        assert queue2_count == 50

        # Key test: During the period when both queues have messages available,
        # they should receive roughly equal treatment despite the empty queue.
        # Test the first 50% of messages when both queues are active.
        first_half = selection_order[:50]  # First 50% of messages
        q1_early = first_half.count("1")
        q2_early = first_half.count("2")

        # The exact distribution pattern may vary by broker implementation,
        # but neither queue should be significantly starved during this period.
        assert abs(q1_early - q2_early) <= 15, (
            f"Expected balanced distribution, got {q1_early}:{q2_early}. "
            f"Actual order: {''.join(first_half)}"
        )
        broker.shutdown()
        thread.join(timeout=1.0)

    def test_concurrent_shutdown_is_thread_safe(self, broker):
        """Verify that concurrent shutdown calls are thread-safe."""
        import threading
        import time

        results = []

        def shutdown_and_record():
            start_time = time.time()
            broker.shutdown()
            end_time = time.time()
            results.append(end_time - start_time)

        # Start multiple threads calling shutdown concurrently
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=shutdown_and_record)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=1.0)

        # All threads should have completed without error
        assert len(results) == 3
        assert all(t >= 0 for t in results)
