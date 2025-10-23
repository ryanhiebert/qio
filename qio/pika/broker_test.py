import pytest
from pika import ConnectionParameters

from qio.broker_test import BaseBrokerTest
from qio.queuespec import QueueSpec

from .broker import PikaBroker


class TestPikaBroker(BaseBrokerTest):
    supports_multiple_queues = False

    @pytest.fixture
    def broker(self):
        broker = PikaBroker(ConnectionParameters())
        yield broker
        broker.shutdown()

    def test_pika_broker_from_uri(self):
        """Test PikaBroker.from_uri with localhost URI creates broker successfully."""
        broker = PikaBroker.from_uri("pika://localhost:5672")
        assert isinstance(broker, PikaBroker)
        broker.shutdown()

    def test_receive_rejects_multiple_queues(self, broker):
        """Verify broker rejects QueueSpec with multiple queues."""
        queuespec = QueueSpec(queues=["queue1", "queue2"], concurrency=2)

        with pytest.raises(ValueError, match="Only one queue is supported"):
            list(broker.receive(queuespec))
