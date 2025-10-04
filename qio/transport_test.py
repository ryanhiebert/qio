"""Base test class for transport implementations.

Example usage for a new transport implementation:

```python
# qio/mytransport/transport_test.py
import pytest
from qio.transport_test import BaseTransportTest
from .transport import MyTransport

class TestMyTransport(BaseTransportTest):
    supports_from_uri = True  # Set based on transport capabilities

    @pytest.fixture
    def transport(self):
        transport = MyTransport("connection_string")
        yield transport
        transport.shutdown()

    # Add transport-specific tests here if needed
    def test_my_transport_specific_feature(self, transport):
        # Custom test for this transport implementation
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
                pytest.skip(f"Transport doesn't support {feature_attr}")
            return test_func(self, *args, **kwargs)

        return wrapper

    return decorator


class BaseTransportTest:
    """Base class with common tests for all transport implementations."""

    supports_from_uri = True

    @pytest.fixture
    def transport(self):
        """Subclasses must implement this fixture to provide a transport instance."""
        raise NotImplementedError("Subclasses must implement transport fixture")

    @pytest.mark.timeout(2)
    def test_multiple_messages(self, transport):
        """Verify that multiple messages can be published and received in order."""
        messages = [b"message1", b"message2", b"message3"]
        for msg in messages:
            transport.publish(msg)

        subscriber = transport.subscribe()
        received_messages = list(itertools.islice(subscriber, len(messages)))

        assert len(received_messages) == len(messages)
        assert received_messages == messages

        transport.shutdown()

    @pytest.mark.timeout(2)
    def test_shutdown(self, transport):
        """Verify that shutdown stops subscribers and they receive pending messages."""
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
    def test_concurrent_publishing(self, transport):
        """Verify that multiple threads can publish messages concurrently."""
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

    def test_concurrent_shutdown_is_thread_safe(self, transport):
        """Verify that concurrent shutdown calls are thread-safe."""
        results = []

        def shutdown_and_record():
            start_time = time.time()
            transport.shutdown()
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
