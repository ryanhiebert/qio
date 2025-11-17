import subprocess
import sys

import pytest

from queueio import QueueIO
from queueio.invocation import Invocation

from .expanded import irregular


@pytest.mark.timeout(10)
def test_integration():
    # Prefers a clean environment and queue
    queueio = QueueIO()

    try:
        queueio.purge(queue="queueio")
        events = queueio.subscribe({Invocation.Completed})
        invocation = irregular()
        queueio.submit(invocation)

        # 1. Start worker process in the background
        proc = subprocess.Popen(
            [sys.executable, "-m", "queueio", "worker", "queueio=1"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            while event := events.get():
                if event.id == invocation.id:
                    break
        finally:
            if proc.poll() is None:  # Process is still running
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't terminate gracefully
                    proc.kill()
                    proc.wait()

    finally:
        # Always clean up the queueio instance
        queueio.shutdown()
