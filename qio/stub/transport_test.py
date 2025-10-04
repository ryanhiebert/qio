import pytest

from qio.transport_test import BaseTransportTest

from .transport import StubTransport


class TestStubTransport(BaseTransportTest):
    @pytest.fixture
    def transport(self):
        transport = StubTransport()
        yield transport
        transport.shutdown()

    def test_stub_transport_from_uri(self):
        """Test StubTransport.from_uri creates transport successfully."""
        transport = StubTransport.from_uri("stub://test")
        assert isinstance(transport, StubTransport)
        transport.shutdown()
