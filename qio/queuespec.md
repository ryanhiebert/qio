# QueueSpec

The `QueueSpec` class represents a parsed queue configuration for workers.

## Usage

```python
from qio.queuespec import QueueSpec

# Single queue
spec = QueueSpec.parse("production=10")
print(spec.queues)      # ['production']
print(spec.concurrency) # 10

# Multiple queues
spec = QueueSpec.parse("high,medium,low=5")
print(spec.queues)      # ['high', 'medium', 'low']
print(spec.concurrency) # 5
```

## Format

Queue specifications use the format: `queue[,queue2,...]=concurrency`

## Examples

| Input               | Queues                      | Concurrency |
|---------------------|-----------------------------|-------------|
| `production=10`     | `['production']`            | `10`        |
| `api,background=5`  | `['api', 'background']`     | `5`         |
| `high,medium,low=2` | `['high', 'medium', 'low']` | `2`         |
