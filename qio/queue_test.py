import time

import pytest

from qio.queue import Collision
from qio.queue import Queue
from qio.queue import ShutDown
from qio.queue import SwapQueue
from qio.queue import SyncQueue
from qio.select import select
from qio.thread import Thread


class TestQueue:
    def test_backpressure_and_coordination(self):
        """Test backpressure and coordination."""
        queue = Queue[str](maxsize=2)

        # Fill the queue to capacity
        def initial_putter(value: str):
            index, result = select([queue.put.select(value)])
            return result

        def initial_getter():
            index, result = select([queue.get.select()])
            return result

        # Fill up the queue first
        t1 = Thread(target=initial_putter, args=("item1",))
        t2 = Thread(target=initial_putter, args=("item2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Both should succeed since queue has capacity
        assert t1.future.result() is None
        assert t2.future.result() is None

        # Now queue is full (maxsize=2). Try to put another item - should block
        def blocked_putter():
            index, result = select([queue.put.select("item3")])
            return result

        blocked_thread = Thread(target=blocked_putter)
        blocked_thread.start()

        # Give it time to start waiting
        time.sleep(0.1)

        # Putter should be blocked, so thread shouldn't be done yet
        assert blocked_thread.is_alive()

        # Now get one item - this should unblock the putter
        getter_thread = Thread(target=initial_getter)
        getter_thread.start()

        # Wait for both operations to complete
        blocked_thread.join()
        getter_thread.join()

        # The getter should have received one of the original items
        gotten_item = getter_thread.future.result()
        assert gotten_item in ["item1", "item2"]

        # The blocked putter should have succeeded
        assert blocked_thread.future.result() is None

        # Verify the queue now contains the remaining original item + new item
        final_get1 = Thread(target=initial_getter)
        final_get2 = Thread(target=initial_getter)
        final_get1.start()
        final_get2.start()
        final_get1.join()
        final_get2.join()

        final_items = {final_get1.future.result(), final_get2.future.result()}
        expected_items = {"item1", "item2", "item3"}
        # One item was already gotten, so we should have the other two
        assert final_items.union({gotten_item}) == expected_items

    def test_unbounded_queue_behavior(self):
        """Test unbounded queue behavior."""
        unbounded = Queue[str](maxsize=0)

        def put_many_unbounded():
            results = []
            for i in range(10):
                index, result = select([unbounded.put.select(f"item{i}")])
                results.append(result)
            return results

        # Should be able to put many items without blocking
        putter_thread = Thread(target=put_many_unbounded)
        putter_thread.start()
        putter_thread.join()

        # All puts should succeed immediately
        results = putter_thread.future.result()
        assert all(r is None for r in results)

        # Verify all items are in the queue by getting them
        def get_all():
            items = []
            for _ in range(10):
                index, result = select([unbounded.get.select()])
                items.append(result)
            return items

        getter_thread = Thread(target=get_all)
        getter_thread.start()
        getter_thread.join()

        items = getter_thread.future.result()
        expected = [f"item{i}" for i in range(10)]
        assert items == expected

    def test_concurrent_multi_producer_consumer(self):
        """Test concurrent multi-producer multi-consumer."""
        queue = Queue[str](maxsize=5)

        # We'll have 3 producers each putting 4 items, and 4 consumers
        # each getting 3 items Total: 12 items produced, 12 items consumed

        producer_count = 3
        items_per_producer = 4
        consumer_count = 4
        items_per_consumer = 3

        def producer(producer_id: int):
            results = []
            for i in range(items_per_producer):
                item = f"producer{producer_id}_item{i}"
                index, result = select([queue.put.select(item)])
                results.append((item, result))
            return results

        def consumer(consumer_id: int):
            items = []
            for _ in range(items_per_consumer):
                index, result = select([queue.get.select()])
                items.append(result)
            return items

        # Start all producers
        producer_threads = []
        for p_id in range(producer_count):
            thread = Thread(target=producer, args=(p_id,))
            producer_threads.append(thread)
            thread.start()

        # Start all consumers with slight delay to ensure some
        # producers start first
        time.sleep(0.05)
        consumer_threads = []
        for c_id in range(consumer_count):
            thread = Thread(target=consumer, args=(c_id,))
            consumer_threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in producer_threads + consumer_threads:
            thread.join()

        # Collect all produced items
        all_produced = []
        for thread in producer_threads:
            producer_results = thread.future.result()
            for item, result in producer_results:
                # All puts should succeed (return None)
                assert result is None
                all_produced.append(item)

        # Collect all consumed items
        all_consumed = []
        for thread in consumer_threads:
            consumer_items = thread.future.result()
            all_consumed.extend(consumer_items)

        # Verify no data loss - all produced items should be consumed
        assert len(all_produced) == producer_count * items_per_producer
        assert len(all_consumed) == consumer_count * items_per_consumer
        assert len(all_produced) == len(all_consumed)  # 12 items each

        # Verify all items are accounted for (no duplicates or missing items)
        assert set(all_produced) == set(all_consumed)

        # Verify each item appears exactly once in consumed items
        assert len(set(all_consumed)) == len(all_consumed)

        # Verify the expected items were produced
        expected_items = set()
        for p_id in range(producer_count):
            for i in range(items_per_producer):
                expected_items.add(f"producer{p_id}_item{i}")

        assert set(all_produced) == expected_items

    def test_immediate_shutdown_behavior(self):
        """Test immediate shutdown behavior - all operations fail immediately."""
        queue = Queue[str](maxsize=1)

        # Fill the queue to capacity
        select([queue.put.select("item1")])

        def blocked_putter():
            index, result = select([queue.put.select("blocked")])
            return result

        def blocked_getter():
            index, result = select([queue.get.select()])
            return result

        def put_after_shutdown():
            index, result = select([queue.put.select("after")])
            return result

        def get_after_shutdown():
            index, result = select([queue.get.select()])
            return result

        # Start blocked putter (queue is full)
        blocked_put_thread = Thread(target=blocked_putter)
        blocked_put_thread.start()

        # Give it time to start waiting
        time.sleep(0.1)
        assert blocked_put_thread.is_alive()  # Should be blocked

        # Shutdown immediately
        queue.shutdown(immediate=True)

        # Start operations after shutdown
        after_put_thread = Thread(target=put_after_shutdown)
        after_get_thread = Thread(target=get_after_shutdown)
        blocked_get_thread = Thread(target=blocked_getter)

        after_put_thread.start()
        after_get_thread.start()
        blocked_get_thread.start()

        # Wait for all threads
        blocked_put_thread.join()
        after_put_thread.join()
        after_get_thread.join()
        blocked_get_thread.join()

        # All operations should have raised ShutDown exceptions
        with pytest.raises(ShutDown):
            blocked_put_thread.future.result()

        with pytest.raises(ShutDown):
            after_put_thread.future.result()

        with pytest.raises(ShutDown):
            after_get_thread.future.result()

        with pytest.raises(ShutDown):
            blocked_get_thread.future.result()

    def test_non_immediate_shutdown_behavior(self):
        """Non-immediate shutdown

        Pending putters fail, getters can drain queue.
        """
        queue = Queue[str](maxsize=1)

        # Add item to queue
        select([queue.put.select("item1")])

        def blocked_putter():
            index, result = select([queue.put.select("blocked")])
            return result

        def put_after_shutdown():
            index, result = select([queue.put.select("after")])
            return result

        # Start blocked putter (queue is full)
        blocked_put_thread = Thread(target=blocked_putter)
        blocked_put_thread.start()

        # Give it time to start waiting
        time.sleep(0.1)
        assert blocked_put_thread.is_alive()  # Should be blocked

        # Shutdown non-immediately (default behavior)
        queue.shutdown(immediate=False)

        # Start operations after shutdown
        after_put_thread = Thread(target=put_after_shutdown)
        after_put_thread.start()

        # Wait for threads
        blocked_put_thread.join()
        after_put_thread.join()

        # Putters should fail immediately
        with pytest.raises(ShutDown):
            blocked_put_thread.future.result()

        with pytest.raises(ShutDown):
            after_put_thread.future.result()

        # We can still get the existing item
        index, result = select([queue.get.select()])
        assert result == "item1"

    def test_non_immediate_vs_immediate_shutdown_difference(self):
        """Test key difference: non-immediate allows draining, immediate doesn't."""
        # Test non-immediate: allows draining existing items
        queue1 = Queue[str](maxsize=2)
        select([queue1.put.select("item1")])
        select([queue1.put.select("item2")])

        queue1.shutdown(immediate=False)

        # Should be able to get existing items
        index, result1 = select([queue1.get.select()])
        index, result2 = select([queue1.get.select()])
        assert {result1, result2} == {"item1", "item2"}

        # Test immediate: blocks all operations immediately
        queue2 = Queue[str](maxsize=2)
        select([queue2.put.select("item1")])
        select([queue2.put.select("item2")])

        queue2.shutdown(immediate=True)

        def try_get():
            index, result = select([queue2.get.select()])
            return result

        get_thread = Thread(target=try_get)
        get_thread.start()
        get_thread.join()

        # Should fail even though items exist
        with pytest.raises(ShutDown):
            get_thread.future.result()

    def test_non_immediate_shutdown_empty_queue(self):
        """Empty queue shuts down immediately."""
        queue = Queue[str](maxsize=2)

        def put_after_shutdown():
            index, result = select([queue.put.select("test")])
            return result

        def get_after_shutdown():
            index, result = select([queue.get.select()])
            return result

        # Shutdown non-immediately on empty queue (should behave like immediate)
        queue.shutdown(immediate=False)

        # Start operations after shutdown
        put_thread = Thread(target=put_after_shutdown)
        get_thread = Thread(target=get_after_shutdown)
        put_thread.start()
        get_thread.start()

        # Wait for threads
        put_thread.join()
        get_thread.join()

        # Both operations should fail since queue was empty at shutdown
        with pytest.raises(ShutDown):
            put_thread.future.result()

        with pytest.raises(ShutDown):
            get_thread.future.result()

    def test_shutdown_default_behavior(self):
        """Test that shutdown() without parameters defaults to non-immediate."""
        queue = Queue[str](maxsize=1)

        # Fill queue to capacity
        select([queue.put.select("item1")])

        def blocked_putter():
            index, result = select([queue.put.select("blocked")])
            return result

        # Start blocked putter (queue is full)
        put_thread = Thread(target=blocked_putter)
        put_thread.start()

        # Give it time to start waiting
        time.sleep(0.1)
        assert put_thread.is_alive()  # Should be blocked

        # Default shutdown (should be non-immediate)
        queue.shutdown()

        put_thread.join()

        # Putter should fail
        with pytest.raises(ShutDown):
            put_thread.future.result()

        # But we should still be able to get the existing item
        index, result = select([queue.get.select()])
        assert result == "item1"


class TestSwapQueue:
    def test_basic_swap(self):
        """Basic atomic value swap."""
        queue = SwapQueue[str, int]()

        def left_side():
            index, result = select([queue.left.select("hello")])
            return result

        def right_side():
            time.sleep(0.1)  # Ensure left side waits first
            index, result = select([queue.right.select(42)])
            return result

        t1 = Thread(target=left_side)
        t2 = Thread(target=right_side)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Verify the swap worked
        assert t1.future.result() == 42
        assert t2.future.result() == "hello"

    def test_queue_shutdown(self):
        """Shutdown cancels pending and blocks new operations."""

        queue = SwapQueue[str, int]()

        def left_before_shutdown():
            index, result = select([queue.left.select("before")])
            return result

        def left_after_shutdown():
            index, result = select([queue.left.select("after_left")])
            return result

        def right_after_shutdown():
            index, result = select([queue.right.select(99)])
            return result

        # Start first thread before shutdown
        t1 = Thread(target=left_before_shutdown)
        t1.start()

        # Give it a moment to start waiting
        time.sleep(0.1)

        # Shutdown the queue
        queue.shutdown()

        # Start threads after shutdown
        t2 = Thread(target=left_after_shutdown)
        t3 = Thread(target=right_after_shutdown)
        t2.start()
        t3.start()

        # Wait for all threads to complete
        t1.join()
        t2.join()
        t3.join()

        # All operations should have raised ShutDown exceptions
        with pytest.raises(ShutDown):
            t1.future.result()

        with pytest.raises(ShutDown):
            t2.future.result()

        with pytest.raises(ShutDown):
            t3.future.result()

    def test_concurrent_swap_handling(self):
        """Concurrent swaps with thread safety."""
        queue = SwapQueue[str, int]()

        # We'll have 3 left-side threads and 3 right-side threads
        left_values = ["hello", "world", "test"]
        right_values = [1, 2, 3]

        def left_side(value: str):
            index, result = select([queue.left.select(value)])
            return result

        def right_side(value: int):
            index, result = select([queue.right.select(value)])
            return result

        # Start all threads
        left_threads = [Thread(target=left_side, args=(val,)) for val in left_values]
        right_threads = [Thread(target=right_side, args=(val,)) for val in right_values]

        # Start left threads first
        for t in left_threads:
            t.start()

        # Small delay, then start right threads
        time.sleep(0.1)
        for t in right_threads:
            t.start()

        # Wait for all to complete
        for t in left_threads + right_threads:
            t.join()

        # Collect all results
        left_results = [t.future.result() for t in left_threads]
        right_results = [t.future.result() for t in right_threads]

        # Verify that each left thread got one of the right values
        # and each right thread got one of the left values
        assert sorted(left_results) == sorted(right_values)
        assert sorted(right_results) == sorted(left_values)

        # Verify no duplicates - each value should appear exactly once
        assert len(set(left_results)) == len(left_results)
        assert len(set(right_results)) == len(right_results)

    def test_abandoned_guard_cleanup(self):
        """Abandoned selectors don't block future operations."""
        queue1 = SwapQueue[str, int]()
        queue2 = SwapQueue[str, int]()

        def multi_queue_left():
            # This will select on multiple queues - only one will succeed
            index, result = select(
                [
                    queue1.left.select("hello"),
                    queue2.left.select("world"),
                ]
            )
            return index, result

        def complete_queue1():
            # This completes the swap on queue1
            index, result = select([queue1.right.select(42)])
            return result

        # Start the multi-queue selector first
        multi_thread = Thread(target=multi_queue_left)
        multi_thread.start()

        # Let it start waiting
        time.sleep(0.1)

        # Complete the swap on queue1 - this should abandon the queue2 selector
        queue1_thread = Thread(target=complete_queue1)
        queue1_thread.start()

        # Wait for both to complete
        multi_thread.join()
        queue1_thread.join()

        # Verify the multi-queue selector completed on queue1 (index 0)
        multi_index, multi_result = multi_thread.future.result()
        queue1_result = queue1_thread.future.result()

        assert multi_index == 0  # First queue in the list
        assert multi_result == 42
        assert queue1_result == "hello"

        # Now test that queue2 still works despite having an abandoned selector
        def use_queue2():
            index, result = select([queue2.left.select("test")])
            return result

        def complete_queue2():
            index, result = select([queue2.right.select(99)])
            return result

        # Start operations on queue2
        queue2_left = Thread(target=use_queue2)
        queue2_right = Thread(target=complete_queue2)

        queue2_left.start()
        time.sleep(0.05)
        queue2_right.start()

        queue2_left.join()
        queue2_right.join()

        # Verify queue2 operations completed successfully
        assert queue2_left.future.result() == 99
        assert queue2_right.future.result() == "test"

    def test_self_swap_edge_case(self):
        """Self-swap on same queue raises SelfSwap exception."""
        queue = SwapQueue[str, str]()

        def self_swap():
            # Try to select on both sides of the same queue
            index, result = select(
                [
                    queue.left.select("hello"),
                    queue.right.select("world"),
                ]
            )
            return index, result

        thread = Thread(target=self_swap)
        thread.start()
        thread.join()

        # Should raise Collision exception
        with pytest.raises(Collision):
            thread.future.result()

        # Test with reversed order too
        def self_swap_reversed():
            index, result = select(
                [
                    queue.right.select("world"),
                    queue.left.select("hello"),
                ]
            )
            return index, result

        thread2 = Thread(target=self_swap_reversed)
        thread2.start()
        thread2.join()

        with pytest.raises(Collision):
            thread2.future.result()


class TestSyncQueue:
    def test_basic_put_get(self):
        """Basic put and get operation."""
        queue = SyncQueue[str]()

        def putter():
            index, result = select([queue.put.select("hello")])
            return result

        def getter():
            time.sleep(0.1)  # Ensure putter waits first
            index, result = select([queue.get.select()])
            return result

        t1 = Thread(target=putter)
        t2 = Thread(target=getter)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Verify the put/get worked
        assert t1.future.result() is None  # put_select returns None
        assert t2.future.result() == "hello"

    def test_sync_queue_shutdown(self):
        """Shutdown delegates to underlying SwapQueue."""
        queue = SyncQueue[str]()

        def put_before_shutdown():
            index, result = select([queue.put.select("before")])
            return result

        # Start thread before shutdown
        t1 = Thread(target=put_before_shutdown)
        t1.start()

        # Give it a moment to start waiting
        time.sleep(0.1)

        # Shutdown the queue
        queue.shutdown()

        # Wait for thread to complete
        t1.join()

        # Should have raised ShutDown exception
        with pytest.raises(ShutDown):
            t1.future.result()

    def test_wrapper_methods_match_swap_queue(self):
        """Verify SyncQueue methods are proper wrappers around SwapQueue."""
        queue = SyncQueue[int]()

        # The methods should return callables (same as SwapQueue returns)
        put_callable = queue.put.select(42)
        get_callable = queue.get.select()

        # These should be callable functions
        assert callable(put_callable)
        assert callable(get_callable)

    def test_type_safety(self):
        """Verify type consistency in SyncQueue operations."""
        queue = SyncQueue[str]()

        def putter():
            # Should accept str and return None
            index, result = select([queue.put.select("test")])
            return result

        def getter():
            # Should return str
            index, result = select([queue.get.select()])
            return result

        put_thread = Thread(target=putter)
        get_thread = Thread(target=getter)

        put_thread.start()
        time.sleep(0.05)
        get_thread.start()

        put_thread.join()
        get_thread.join()

        # Verify types are as expected
        put_result = put_thread.future.result()
        get_result = get_thread.future.result()

        assert put_result is None
        assert isinstance(get_result, str)
        assert get_result == "test"
