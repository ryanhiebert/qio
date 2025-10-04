import pytest

from qio.broker_test import BaseBrokerTest

from .broker import StubBroker


class TestStubBroker(BaseBrokerTest):
    supports_multiple_queues = True

    @pytest.fixture
    def broker(self):
        broker = StubBroker()
        yield broker
        broker.shutdown()
