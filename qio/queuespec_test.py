import pytest

from .queuespec import QueueSpec


def test_single_queue_parsing():
    spec = QueueSpec.parse("production=10")
    assert spec.queues == ["production"]
    assert spec.concurrency == 10


def test_multi_queue_parsing():
    spec = QueueSpec.parse("high,medium,low=5")
    assert spec.queues == ["high", "medium", "low"]
    assert spec.concurrency == 5


def test_whitespace_handling():
    spec = QueueSpec.parse("  api , background  = 8 ")
    assert spec.queues == ["api", "background"]
    assert spec.concurrency == 8


def test_empty_spec_error():
    with pytest.raises(ValueError, match="Queue spec cannot be empty"):
        QueueSpec.parse("")


def test_missing_equals_error():
    with pytest.raises(ValueError, match="Invalid queue spec"):
        QueueSpec.parse("production")


def test_empty_concurrency_error():
    with pytest.raises(ValueError, match="Concurrency must be a positive integer"):
        QueueSpec.parse("queue=")


def test_invalid_concurrency_error():
    with pytest.raises(ValueError, match="Concurrency must be a positive integer"):
        QueueSpec.parse("queue=abc")


def test_zero_concurrency_error():
    with pytest.raises(ValueError, match="Concurrency must be a positive integer"):
        QueueSpec.parse("queue=0")


def test_negative_concurrency_error():
    with pytest.raises(ValueError, match="Concurrency must be a positive integer"):
        QueueSpec.parse("queue=-5")


def test_empty_queue_names_error():
    with pytest.raises(ValueError, match="No valid queue names found"):
        QueueSpec.parse("=10")


def test_comma_only_error():
    with pytest.raises(ValueError, match="Invalid queue spec"):
        QueueSpec.parse(",")


def test_empty_queue_names_in_list():
    spec = QueueSpec.parse("queue1,,queue2=5")
    assert spec.queues == ["queue1", "queue2"]
    assert spec.concurrency == 5
