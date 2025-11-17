# QueueIO Class Configuration

The `QueueIO` class requires configuration of a pika connection through environment variables or config file settings. Environment variables always take precedence over config file settings.

## Priority Order

1. **Environment Variables** (highest priority)
2. **Configuration File** (pyproject.toml)

## Environment Variables

Set this environment variable to configure the `QueueIO` class:

- `QUEUEIO_PIKA` - Pika connection URI

## Configuration File

Add configuration to your `pyproject.toml`:

```toml
[tool.queueio]
pika = "amqp://localhost:5672"
```

## URI Format

Currently only `amqp://` URIs are supported:

```
amqp://hostname:port
```

Examples:
- `amqp://localhost:5672`
- `amqp://rabbitmq.example.com:5672`
