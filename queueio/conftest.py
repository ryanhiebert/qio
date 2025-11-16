import os
import threading
from threading import Thread
from time import sleep

import pytest


def pytest_sessionstart(session):
    """Ensure the test suite always exits."""
    timeout = float(session.config.getini("timeout")) + 5
    Thread(target=lambda: sleep(timeout) or os._exit(1), daemon=True).start()


@pytest.fixture(autouse=True)
def check_thread_cleanup():
    """Ensure that threads are not left running."""
    initial_threads = set(threading.enumerate())
    yield
    final_threads = {t for t in threading.enumerate() if t.is_alive()}
    new_threads = final_threads - initial_threads

    if new_threads:
        thread_info = []
        for thread in new_threads:
            daemon = "daemon" if thread.daemon else "non-daemon"
            thread_info.append(f"  - {thread.name} ({daemon})")

        pytest.fail(
            f"Test left {len(new_threads)} thread(s) running:\n"
            + "\n".join(thread_info)
        )
