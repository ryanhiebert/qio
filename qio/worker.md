# Worker

The `Worker` class processes invocations from message queues with specified concurrency.

## Usage

```python
from qio.qio import Qio
from qio.queuespec import QueueSpec
from qio.worker import Worker

# Create queue specification
queuespec = QueueSpec.parse("production=5")

# Create worker
qio = Qio(broker=broker, journal=journal)
worker = Worker(qio, queuespec)

# Run worker (blocks until stopped)
worker()
```

## Concurrency

The worker spawns multiple runner threads based on the concurrency setting:
- Each thread processes invocations independently
- Prefetch limit matches concurrency to prevent overloading
- Threads coordinate through internal task queues

## Lifecycle

1. **Initialization**: Validates single queue, creates consumer and threads
2. **Running**: Starts all threads and monitors for completion
3. **Shutdown**: Stops gracefully on `KeyboardInterrupt` or thread failure

## CLI Integration

```bash
python -m qio worker production=10
```

The CLI uses `QueueSpec.parse()` to convert string arguments into `QueueSpec` objects.
