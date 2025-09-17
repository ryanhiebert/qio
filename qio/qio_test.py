import pytest

from .qio import Qio
from .registry import ROUTINE_REGISTRY
from .stub.broker import StubBroker
from .stub.transport import StubTransport


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


def test_qio_loads_default_configuration(tmp_path):
    """Qio loads default broker and transport when none provided."""
    import os

    # Create config with default settings
    config_dir = tmp_path / "default_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-defaults"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        qio = Qio()
        try:
            # Should be able to perform basic operations
            qio.purge(queue="test")
            events = qio.subscribe({object})
            qio.unsubscribe(events)
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_allows_independent_broker_transport_override(tmp_path):
    """Qio allows independent override of broker or transport."""
    import os

    # Create config for default broker/transport
    config_dir = tmp_path / "override_test"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-override"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        stub_broker = StubBroker()
        stub_transport = StubTransport()

        # Override just broker, transport should use default
        qio1 = Qio(broker=stub_broker)

        # Override just transport, broker should use default
        qio2 = Qio(transport=stub_transport)

        try:
            # Both should work
            qio1.purge(queue="test")
            qio2.purge(queue="test")
        finally:
            qio1.shutdown()
            qio2.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_routines_method():
    """Qio.routines() returns registered routines."""
    from .routine import Routine

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Manually add a test routine
        def test_function():
            pass

        test_routine = Routine(test_function, name="test_routine", queue="test_queue")
        ROUTINE_REGISTRY["test_routine"] = test_routine

        qio = Qio(broker=StubBroker(), transport=StubTransport())

        try:
            routines = qio.routines()

            # Should have our test routine
            assert len(routines) == 1
            assert routines[0].name == "test_routine"
            assert routines[0].queue == "test_queue"
        finally:
            qio.shutdown()
    finally:
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_valid_config(tmp_path):
    """Qio works with a valid pyproject.toml configuration."""
    import os

    # Create valid config
    config_dir = tmp_path / "valid_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project"
        version = "0.1.0"

        [tool.qio]
        register = ["qio.sample"]
        broker = "pika://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    try:
        qio = Qio()
        try:
            # Should load configuration successfully
            qio.purge(queue="test")
            routines = qio.routines()
            routine_names = {routine.name for routine in routines}
            assert "regular" in routine_names
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)


def test_qio_with_no_config(tmp_path):
    """Qio works with pyproject.toml that has no [tool.qio] section."""
    import os

    # Create config with no qio section
    config_dir = tmp_path / "no_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-no-config"
        version = "0.1.0"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        qio = Qio()
        try:
            # Should use defaults and work
            qio.purge(queue="test")
            routines = qio.routines()
            # Should have no routines since no register config
            assert len(routines) == 0
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_no_pyproject_file(tmp_path):
    """Qio works when no pyproject.toml exists."""
    import os

    # Create empty directory with no pyproject.toml
    config_dir = tmp_path / "empty"
    config_dir.mkdir()

    # Change to empty directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        qio = Qio()
        try:
            # Should use defaults and work
            qio.purge(queue="test")
            routines = qio.routines()
            # Should have no routines since no config
            assert len(routines) == 0
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_config(tmp_path):
    """Qio fails with unknown broker/transport types."""
    import os

    # Create config with unknown broker/transport
    config_dir = tmp_path / "invalid_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid"
        version = "0.1.0"

        [tool.qio]
        broker = "unknown://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for unknown broker URI scheme
        with pytest.raises(
            ValueError,
            match="URI scheme must be 'pika:', got: unknown://localhost:5672",
        ):
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_transport_config(tmp_path):
    """Qio fails with unknown transport type."""
    import os

    # Create config with unknown transport
    config_dir = tmp_path / "invalid_transport_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-transport"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "unknown://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for unknown transport URI scheme
        with pytest.raises(
            ValueError,
            match="URI scheme must be 'pika:', got: unknown://localhost:5672",
        ):
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_uri_broker_config(tmp_path):
    """Qio works with URI-based broker configuration."""
    import os

    # Create config with broker URI
    config_dir = tmp_path / "uri_broker_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-uri-broker"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        qio = Qio()
        try:
            # Should work with URI configuration
            qio.purge(queue="test")
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_uri_transport_config(tmp_path):
    """Qio works with URI-based transport configuration."""
    import os

    # Create config with transport URI
    config_dir = tmp_path / "uri_transport_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-uri-transport"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        qio = Qio()
        try:
            # Should work with URI configuration
            qio.purge(queue="test")
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_both_uri_configs(tmp_path):
    """Qio works with both broker and transport as URIs."""
    import os

    # Create config with both URIs
    config_dir = tmp_path / "both_uri_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-both-uri"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        qio = Qio()
        try:
            # Should work with both URI configurations
            qio.purge(queue="test")
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_broker_uri_scheme(tmp_path):
    """Qio fails with invalid URI scheme for broker."""
    import os

    # Create config with invalid broker URI scheme
    config_dir = tmp_path / "invalid_broker_uri"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-broker-uri"
        version = "0.1.0"

        [tool.qio]
        broker = "redis://localhost:6379"
        transport = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for invalid URI scheme
        with pytest.raises(
            ValueError, match="URI scheme must be 'pika:', got: redis://localhost:6379"
        ):
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_transport_uri_scheme(tmp_path):
    """Qio fails with invalid URI scheme for transport."""
    import os

    # Create config with invalid transport URI scheme
    config_dir = tmp_path / "invalid_transport_uri"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-transport-uri"
        version = "0.1.0"

        [tool.qio]
        broker = "pika://localhost:5672"
        transport = "redis://localhost:6379"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for invalid URI scheme
        with pytest.raises(
            ValueError, match="URI scheme must be 'pika:', got: redis://localhost:6379"
        ):
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)
