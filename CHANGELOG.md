Changelog
=========

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com).

[0.2.2] - 2025-11-23
--------------------

### Fixed

- Use the queueio logo from GitHub so it will show on PyPI.

[0.2.1] - 2025-11-22
--------------------

### Fixed

- Include the queueio logo in the distribution so it will show on PyPI.

[0.2.0] - 2025-11-22
--------------------

### Acknowledgement

Thank you to Nick Anderegg for allowing me to use the queueio name for this project.

### Added

- `routine` decorator to declare sync or async functions as background routines.
- `activate` context manager to activate the queueio system.
- `pause` to coordinate a pause of a routine to queueio.
- `gather` to run multiple routines concurrently and gather the results.
- `Routine.submit()` method to submit a routine invocation to the queue.
- Configuration in the `tool.queueio` section of `pyproject.toml`.
  - `pika` configures the pika library to connect to the AMQP broker.
  - `register` configures the modules that declare routines.
- `QUEUEIO_PIKA` environment variable
  to override the `pika` configuration in `pyproject.toml`.
- `queueio sync` command to synchronize queues to the broker.
- `queueio run` command to run the queueio worker.
- The queuespec syntax to `queue run` to consume multiple queues with shared capacity.
- `queueio monitor` command to monitor activity in the queueio system.

[0.2.2]: https://github.com/ryanhiebert/queueio/compare/tag/0.2.1...tag/0.2.2
[0.2.1]: https://github.com/ryanhiebert/queueio/compare/tag/0.2.0...tag/0.2.1
[0.2.0]: https://github.com/ryanhiebert/queueio/releases/tag/0.2.0
