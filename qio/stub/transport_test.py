import pytest

from qio.transport_test import BaseTransportTest

from .transport import StubTransport


class TestStubTransport(BaseTransportTest):
    supports_from_uri = True

    @pytest.fixture
    def transport(self):
        transport = StubTransport()
        yield transport
        transport.shutdown()
