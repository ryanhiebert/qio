import subprocess
import sys

import pytest

from qio import Qio
from qio.__main__ import irregular
from qio.invocation import InvocationSucceeded


@pytest.mark.timeout(10)
def test_integration():
    # Prefers a clean environment and queue
    qio = Qio()

    try:
        qio.purge()
        events = qio.subscribe({InvocationSucceeded})
        invocation = irregular()
        qio.submit(invocation)

        # 1. Start worker process in the background
        worker = subprocess.Popen(
            [sys.executable, "-m", "qio", "worker"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            while event := events.get():
                if event.invocation_id == invocation.id:
                    break
        finally:
            if worker.poll() is None:  # Process is still running
                worker.terminate()
                try:
                    worker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't terminate gracefully
                    worker.kill()
                    worker.wait()

    finally:
        # Always clean up the qio instance
        qio.shutdown()
