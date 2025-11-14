import pytest

from queueio.broker_test import BaseBrokerTest

from .broker import StubBroker


class TestStubBroker(BaseBrokerTest):
    supports_multiple_queues = True
    supports_weighted_queue_subscriptions = True

    @pytest.fixture
    def broker(self):
        broker = StubBroker()
        yield broker
        broker.shutdown()

    def test_stub_broker_from_uri(self):
        """Test StubBroker.from_uri creates broker successfully."""
        broker = StubBroker.from_uri("stub://test")
        assert isinstance(broker, StubBroker)
        broker.shutdown()
