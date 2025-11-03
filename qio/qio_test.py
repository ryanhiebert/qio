import os

import pytest

from qio.stub.journal import StubJournal

from .qio import Qio
from .registry import ROUTINE_REGISTRY
from .stub.broker import StubBroker


def test_qio_with_custom_broker_and_journal():
    """Qio custom broker and journal implementations."""
    broker = StubBroker()
    journal = StubJournal()
    qio = Qio(broker=broker, journal=journal)

    try:
        # Test purge (uses broker)
        qio.purge(queue="qio")

        # Test subscriptions (uses journal)
        events = qio.subscribe({object})
        qio.unsubscribe(events)

    finally:
        qio.shutdown()


def test_different_qio_instances_are_independent():
    """Qio instances are independent."""
    # Create two Qio instances with different stub implementations
    broker1 = StubBroker()
    journal1 = StubJournal()
    qio1 = Qio(broker=broker1, journal=journal1)

    broker2 = StubBroker()
    journal2 = StubJournal()
    qio2 = Qio(broker=broker2, journal=journal2)

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


def test_qio_loads_configuration_from_pyproject(tmp_path):
    """Qio loads broker and journal configuration from pyproject.toml."""

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


def test_qio_allows_independent_broker_journal_override(tmp_path):
    """Qio allows independent override of broker or journal."""

    # Create config for default broker/journal
    config_dir = tmp_path / "override_test"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-override"
        version = "0.1.0"

        [tool.qio]
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
        qio1 = Qio(broker=stub_broker)

        # Override just journal, broker should use default
        qio2 = Qio(journal=stub_journal)

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

    qio = Qio(broker=StubBroker(), journal=StubJournal())

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
        journal = "pika://localhost:5672"
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


def test_qio_with_invalid_config(tmp_path):
    """Qio fails with unknown broker/journal types."""

    # Create config with unknown broker/journal
    config_dir = tmp_path / "invalid_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid"
        version = "0.1.0"

        [tool.qio]
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
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_journal_config(tmp_path):
    """Qio fails with unknown journal type."""

    # Create config with unknown journal
    config_dir = tmp_path / "invalid_journal_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-journal"
        version = "0.1.0"

        [tool.qio]
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
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_uri_broker_config(tmp_path):
    """Qio works with URI-based broker configuration."""

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


def test_qio_with_uri_journal_config(tmp_path):
    """Qio works with URI-based journal configuration."""

    # Create config with journal URI
    config_dir = tmp_path / "uri_journal_config"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-uri-journal"
        version = "0.1.0"

        [tool.qio]
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
    """Qio works with both broker and journal as URIs."""

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
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_both_environment_variables(tmp_path, monkeypatch):
    """Qio prefers both environment variables over config."""

    # Create config with different broker/journal
    config_dir = tmp_path / "env_both_test"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-env-both"
        version = "0.1.0"

        [tool.qio]
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
    monkeypatch.setenv("QIO_BROKER", "pika://localhost:5672")
    monkeypatch.setenv("QIO_JOURNAL", "pika://localhost:5672")

    try:
        qio = Qio()
        try:
            # Should work with environment variables taking precedence
            qio.purge(queue="test")
        finally:
            qio.shutdown()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_environment_broker(monkeypatch):
    """Qio fails with invalid QIO_BROKER environment variable."""
    # Set invalid environment variable
    monkeypatch.setenv("QIO_BROKER", "redis://invalid:6379")

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for invalid URI scheme
        with pytest.raises(
            ValueError, match="URI scheme must be 'pika:', got: redis://invalid:6379"
        ):
            Qio()
    finally:
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_environment_journal(monkeypatch):
    """Qio fails with invalid QIO_JOURNAL environment variable."""
    # Set invalid environment variable
    monkeypatch.setenv("QIO_JOURNAL", "redis://invalid:6379")

    # Clear registry to isolate test
    original_registry = dict(ROUTINE_REGISTRY)
    ROUTINE_REGISTRY.clear()

    try:
        # Should raise ValueError for invalid URI scheme
        with pytest.raises(
            ValueError, match="URI scheme must be 'pika:', got: redis://invalid:6379"
        ):
            Qio()
    finally:
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)


def test_qio_with_invalid_journal_uri_scheme(tmp_path):
    """Qio fails with invalid URI scheme for journal."""

    # Create config with invalid journal URI scheme
    config_dir = tmp_path / "invalid_journal_uri"
    config_dir.mkdir()
    config_file = config_dir / "pyproject.toml"
    config_content = """
        [project]
        name = "test-project-invalid-journal-uri"
        version = "0.1.0"

        [tool.qio]
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
            Qio()
    finally:
        os.chdir(original_cwd)
        # Restore registry
        ROUTINE_REGISTRY.clear()
        ROUTINE_REGISTRY.update(original_registry)
