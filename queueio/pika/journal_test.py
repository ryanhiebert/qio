import pytest
from pika import ConnectionParameters

from queueio.journal_test import BaseJournalTest

from .journal import PikaJournal


class TestPikaJournal(BaseJournalTest):
    @pytest.fixture
    def journal(self):
        journal = PikaJournal(ConnectionParameters())
        yield journal
        journal.shutdown()

    def test_pika_journal_from_uri(self):
        """Test PikaJournal.from_uri creates journal successfully."""
        journal = PikaJournal.from_uri("amqp://localhost:5672")
        assert isinstance(journal, PikaJournal)
        journal.shutdown()
