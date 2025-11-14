import pytest

from queueio.journal_test import BaseJournalTest

from .journal import StubJournal


class TestStubJournal(BaseJournalTest):
    @pytest.fixture
    def journal(self):
        journal = StubJournal()
        yield journal
        journal.shutdown()

    def test_stub_journal_from_uri(self):
        """Test StubJournal.from_uri creates journal successfully."""
        journal = StubJournal.from_uri("stub://test")
        assert isinstance(journal, StubJournal)
        journal.shutdown()
