from qio.qio import Qio
from qio.stub.broker import StubBroker
from qio.stub.transport import StubTransport


def test_qio_with_custom_broker_and_transport():
    """Qio custom broker and transport implementations."""
    broker = StubBroker()
    transport = StubTransport()
    qio = Qio(broker=broker, transport=transport)

    try:
        # Test purge (uses broker)
        qio.purge(queue="qio")

        # Test subscriptions (uses transport)
        events = qio.subscribe({object})
        qio.unsubscribe(events)

    finally:
        qio.shutdown()


def test_different_qio_instances_are_independent():
    """Qio instances are independent."""
    # Create two Qio instances with different stub implementations
    broker1 = StubBroker()
    transport1 = StubTransport()
    qio1 = Qio(broker=broker1, transport=transport1)

    broker2 = StubBroker()
    transport2 = StubTransport()
    qio2 = Qio(broker=broker2, transport=transport2)

    try:
        # Both should work independently
        qio1.purge(queue="qio")
        qio2.purge(queue="qio")

        # Test that they can have independent subscriptions
        events1 = qio1.subscribe({object})
        events2 = qio2.subscribe({object})

        qio1.unsubscribe(events1)
        qio2.unsubscribe(events2)

    finally:
        qio1.shutdown()
        qio2.shutdown()
