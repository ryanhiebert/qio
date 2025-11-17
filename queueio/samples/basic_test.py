import subprocess
import sys

import pytest

from queueio import QueueIO
from queueio.invocation import Invocation

from .basic import yielding


@pytest.mark.timeout(10)
def test_integration():
    queueio = QueueIO()

    try:
        queueio.purge(queue="queueio")
        events = queueio.subscribe({Invocation.Completed})
        invocation = yielding(7)
        queueio.submit(invocation)

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
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    finally:
        queueio.shutdown()
