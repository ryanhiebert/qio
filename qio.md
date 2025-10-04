# Qio Class Configuration

The `Qio` class supports configuration of broker and transport connections through multiple methods with a clear priority order. You can mix environment variables and config file settings - environment variables always take precedence over config file settings, which take precedence over the default values (`pika://localhost:5672`).

## Priority Order

1. **Environment Variables** (highest priority)
2. **Configuration File** (pyproject.toml)
3. **Default Values** (`pika://localhost:5672`)

## Environment Variables

Set these environment variables to configure the `Qio` class:

- `QIO_BROKER` - Broker connection URI
- `QIO_TRANSPORT` - Transport connection URI

## Configuration File

Add configuration to your `pyproject.toml`:

```toml
[tool.qio]
broker = "pika://localhost:5672"
transport = "pika://localhost:5672"
```

## URI Format

Currently only `pika://` URIs are supported:

```
pika://hostname:port
```

Examples:
- `pika://localhost:5672`
- `pika://rabbitmq.example.com:5672`
