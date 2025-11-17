# Worker

The `Worker` class processes invocations from message queues with specified concurrency.

## Usage

```python
from queueio.queueio import QueueIO
from queueio.queuespec import QueueSpec
from queueio.worker import Worker

# Create queue specification
queuespec = QueueSpec.parse("production=5")

# Create worker
queueio = QueueIO()
worker = Worker(queueio, queuespec)

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
python -m queueio worker production=10
```

The CLI uses `QueueSpec.parse()` to convert string arguments into `QueueSpec` objects.
