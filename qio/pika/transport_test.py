import pytest
from pika import ConnectionParameters

from qio.transport_test import BaseTransportTest

from .transport import PikaTransport


class TestPikaTransport(BaseTransportTest):
    @pytest.fixture
    def transport(self):
        transport = PikaTransport(ConnectionParameters())
        yield transport
        transport.shutdown()

    def test_pika_transport_from_uri(self):
        """Test PikaTransport.from_uri creates transport successfully."""
        transport = PikaTransport.from_uri("pika://localhost:5672")
        assert isinstance(transport, PikaTransport)
        transport.shutdown()
