import pytest
from pika import ConnectionParameters

from queueio.broker_test import BaseBrokerTest

from .broker import PikaBroker


class TestPikaBroker(BaseBrokerTest):
    supports_multiple_queues = True
    supports_weighted_queue_subscriptions = False

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
