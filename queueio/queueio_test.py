import os

import pytest

from queueio.stub.journal import StubJournal

from .queueio import QueueIO
from .registry import ROUTINE_REGISTRY
from .stub.broker import StubBroker


def test_queueio_with_custom_broker_and_journal():
    """QueueIO custom broker and journal implementations."""
    broker = StubBroker()
    journal = StubJournal()
    queueio = QueueIO(broker=broker, journal=journal)

    try:
        # Test purge (uses broker)
        queueio.purge(queue="queueio")

        # Test subscriptions (uses journal)
        events = queueio.subscribe({object})
        queueio.unsubscribe(events)
    finally:
        queueio.shutdown()


def test_different_queueio_instances_are_independent():
    """QueueIO instances are independent."""
    # Create two QueueIO instances with different stub implementations
    broker1 = StubBroker()
    journal1 = StubJournal()
    queueio1 = QueueIO(broker=broker1, journal=journal1)

    broker2 = StubBroker()
    journal2 = StubJournal()
    queueio2 = QueueIO(broker=broker2, journal=journal2)

    try:
        # Both should work independently
        queueio1.purge(queue="queueio")
        queueio2.purge(queue="queueio")

        # Test that they can have independent subscriptions
        events1 = queueio1.subscribe({object})
        events2 = queueio2.subscribe({object})

        queueio1.unsubscribe(events1)
        queueio2.unsubscribe(events2)

    finally:
        queueio1.shutdown()
        queueio2.shutdown()


def test_queueio_loads_configuration_from_pyproject(tmp_path):
    """QueueIO loads broker and journal configuration from pyproject.toml."""

    # Create config with default settings
    config_dir = tmp_path / "default_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-defaults"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        queueio = QueueIO()
        try:
            # Should be able to perform basic operations
            queueio.purge(queue="test")
            events = queueio.subscribe({object})
            queueio.unsubscribe(events)
        finally:
            queueio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_allows_independent_broker_journal_override(tmp_path):
    """QueueIO allows independent override of broker or journal."""

    # Create config for default broker/journal
    config_dir = tmp_path / "override_test"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-override"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "pika://localhost:5672"
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
        stub_journal = StubJournal()

        # Override just broker, journal should use default
        queueio1 = QueueIO(broker=stub_broker)

        # Override just journal, broker should use default
        queueio2 = QueueIO(journal=stub_journal)

        try:
            # Both should work
            queueio1.purge(queue="test")
            queueio2.purge(queue="test")
        finally:
            queueio1.shutdown()
            queueio2.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_routines_method():
    """QueueIO.routines() returns registered routines."""
    from .routine import Routine

    queueio = QueueIO(broker=StubBroker(), journal=StubJournal())

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Manually add a test routine
        def test_function():
            pass

        test_routine = Routine(test_function, name="test_routine", queue="test_queue")
        ROUTINE_REGISTRY["test_routine"] = test_routine

        try:
            routines = queueio.routines()

            # Should have our test routine
            assert len(routines) == 1
            assert routines[0].name == "test_routine"
            assert routines[0].queue == "test_queue"
        finally:
            queueio.shutdown()
    finally:
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_valid_config(tmp_path):
    """QueueIO works with a valid pyproject.toml configuration."""

    # Create valid config
    config_dir = tmp_path / "valid_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project"
        version = "0.1.0"

        [tool.queueio]
        register = ["queueio.sample"]
        broker = "pika://localhost:5672"
        journal = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    try:
        queueio = QueueIO()
        try:
            # Should load configuration successfully
            queueio.purge(queue="test")
            routines = queueio.routines()
            routine_names = {routine.name for routine in routines}
            assert "regular" in routine_names
        finally:
            queueio.shutdown()
    finally:
        os.chdir(original_cwd)


def test_queueio_with_invalid_config(tmp_path):
    """QueueIO fails with unknown broker/journal types."""

    # Create config with unknown broker/journal
    config_dir = tmp_path / "invalid_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid"
        version = "0.1.0"

        [tool.queueio]
        broker = "unknown://localhost:5672"
        journal = "pika://localhost:5672"
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
            QueueIO()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_invalid_journal_config(tmp_path):
    """QueueIO fails with unknown journal type."""

    # Create config with unknown journal
    config_dir = tmp_path / "invalid_journal_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-journal"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "unknown://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for unknown journal URI scheme
        with pytest.raises(
            ValueError,
            match="URI scheme must be 'pika:', got: unknown://localhost:5672",
        ):
            QueueIO()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_uri_broker_config(tmp_path):
    """QueueIO works with URI-based broker configuration."""

    # Create config with broker URI
    config_dir = tmp_path / "uri_broker_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-uri-broker"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        queueio = QueueIO()
        try:
            # Should work with URI configuration
            queueio.purge(queue="test")
        finally:
            queueio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_uri_journal_config(tmp_path):
    """QueueIO works with URI-based journal configuration."""

    # Create config with journal URI
    config_dir = tmp_path / "uri_journal_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-uri-journal"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        queueio = QueueIO()
        try:
            # Should work with URI configuration
            queueio.purge(queue="test")
        finally:
            queueio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_both_uri_configs(tmp_path):
    """QueueIO works with both broker and journal as URIs."""

    # Create config with both URIs
    config_dir = tmp_path / "both_uri_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-both-uri"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "pika://localhost:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        queueio = QueueIO()
        try:
            # Should work with both URI configurations
            queueio.purge(queue="test")
        finally:
            queueio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_invalid_broker_uri_scheme(tmp_path):
    """QueueIO fails with invalid URI scheme for broker."""

    # Create config with invalid broker URI scheme
    config_dir = tmp_path / "invalid_broker_uri"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-broker-uri"
        version = "0.1.0"

        [tool.queueio]
        broker = "redis://localhost:6379"
        journal = "pika://localhost:5672"
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
            QueueIO()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_both_environment_variables(tmp_path, monkeypatch):
    """QueueIO prefers both environment variables over config."""

    # Create config with different broker/journal
    config_dir = tmp_path / "env_both_test"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-env-both"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://config:5672"
        journal = "pika://config:5672"
        """
    config_file.write_text(config_content)

    # Change to config directory
    original_cwd = os.getcwd()
    os.chdir(config_dir)

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    # Set both environment variables to use localhost (which works)
    monkeypatch.setenv("QUEUEIO_BROKER", "pika://localhost:5672")
    monkeypatch.setenv("QUEUEIO_JOURNAL", "pika://localhost:5672")

    try:
        queueio = QueueIO()
        try:
            # Should work with environment variables taking precedence
            queueio.purge(queue="test")
        finally:
            queueio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_invalid_environment_broker(monkeypatch):
    """QueueIO fails with invalid QUEUEIO_BROKER environment variable."""
    # Set invalid environment variable
    monkeypatch.setenv("QUEUEIO_BROKER", "redis://invalid:6379")

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for invalid URI scheme
        with pytest.raises(
            ValueError, match="URI scheme must be 'pika:', got: redis://invalid:6379"
        ):
            QueueIO()
    finally:
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_invalid_environment_journal(monkeypatch):
    """QueueIO fails with invalid QUEUEIO_JOURNAL environment variable."""
    # Set invalid environment variable
    monkeypatch.setenv("QUEUEIO_JOURNAL", "redis://invalid:6379")

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for invalid URI scheme
        with pytest.raises(
            ValueError, match="URI scheme must be 'pika:', got: redis://invalid:6379"
        ):
            QueueIO()
    finally:
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_queueio_with_invalid_journal_uri_scheme(tmp_path):
    """QueueIO fails with invalid URI scheme for journal."""

    # Create config with invalid journal URI scheme
    config_dir = tmp_path / "invalid_journal_uri"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-journal-uri"
        version = "0.1.0"

        [tool.queueio]
        broker = "pika://localhost:5672"
        journal = "redis://localhost:6379"
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
            QueueIO()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)
