"""Base test class for journal implementations.

Example usage for a new journal implementation:

```python
# qio/myjournal/journal_test.py
import pytest
from qio.journal_test import BaseJournalTest
from .journal import MyJournal

class TestMyJournal(BaseJournalTest):

    @pytest.fixture
    def journal(self):
        journal = MyJournal("connection_string")
        yield journal
        journal.shutdown()

    # Add journal-specific tests here if needed
    def test_my_journal_specific_feature(self, journal):
        # Custom test for this journal implementation
        pass
```
"""

import itertools
import threading
import time
from functools import wraps

import pytest


def skip_if_unsupported(feature_attr):
    """Decorator that skips test if the test class doesn't support a feature."""

    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            if not getattr(self, feature_attr, True):
                pytest.skip(f"Journal doesn't support {feature_attr}")
            return test_func(self, *args, **kwargs)

        return wrapper

    return decorator


class BaseJournalTest:
    """Base class with common tests for all journal implementations."""

    @pytest.fixture
    def journal(self):
        """Subclasses must implement this fixture to provide a journal instance."""
        raise NotImplementedError("Subclasses must implement journal fixture")

    @pytest.mark.timeout(2)
    def test_multiple_messages(self, journal):
        """Verify that multiple messages can be published and received in order."""
        messages = [b"message1", b"message2", b"message3"]
        for msg in messages:
            journal.publish(msg)

        subscriber = journal.subscribe()
        received_messages = list(itertools.islice(subscriber, len(messages)))

        assert len(received_messages) == len(messages)
        assert received_messages == messages

        journal.shutdown()

    @pytest.mark.timeout(2)
    def test_shutdown(self, journal):
        """Verify that shutdown stops subscribers and they receive pending messages."""
        journal.publish(b"test message 1")
        journal.publish(b"test message 2")

        received_messages = []

        def subscriber_thread():
            for message in journal.subscribe():
                received_messages.append(message)

        thread = threading.Thread(target=subscriber_thread)
        thread.start()

        time.sleep(0.1)
        journal.shutdown()

        thread.join(timeout=2.0)
        assert received_messages == [b"test message 1", b"test message 2"]

    @pytest.mark.timeout(2)
    def test_concurrent_publishing(self, journal):
        """Verify that multiple threads can publish messages concurrently."""
        received_messages = []

        def publisher(start_num: int, count: int):
            for i in range(count):
                message = f"message_{start_num + i}".encode()
                journal.publish(message)

        def subscriber():
            for message in journal.subscribe():
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

        journal.shutdown()
        subscriber_thread.join(timeout=1.0)

    def test_concurrent_shutdown_is_thread_safe(self, journal):
        """Verify that concurrent shutdown calls are thread-safe."""
        results = []

        def shutdown_and_record():
            start_time = time.time()
            journal.shutdown()
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
