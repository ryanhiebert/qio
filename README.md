![queueio](logo.svg)

Python background queues with an async twist
============================================

Use async functions to manage complex background task workflows,
and keep using synchronous functions for everything else.

Getting Started
---------------

Install `queueio`:

```sh
pip install queueio
```

Create your routines:

```python
# basic.py
from time import sleep

from queueio import activate
from queueio import routine
from queueio.gather import gather
from queueio.pause import pause


@routine(name="blocking", queue="queueio")
def blocking():
    sleep(0.1)  # Regular blocking call


@routine(name="yielding", queue="queueio")
async def yielding(iterations: int):
    # Do them two at a time
    for _ in range(iterations // 2):
        await gather(blocking(), blocking())
        await pause(0.2)  # Release processing capacity
    if iterations % 2 == 1:
        await blocking()


if __name__ == "__main__":
    with activate():
        yielding(7).start()

```

Add the configuration to your `pyproject.toml`:

```toml
[tool.queueio]
# Configure RabbitMQ
pika = "amqp://guest:guest@localhost:5672/"
# Register the modules that the worker should load to find your routines
register = ["basic"]
```

The pika configuration can be overridden with an environment variable
to allow a project to be deployed in multiple environments.

```sh
QUEUEIO_PIKA='amqp://guest:guest@localhost:5672/'
```

Run your script to submit the routine to run on a worker:

```sh
python basic.py
```

Then run the worker to process submitted routines:

```sh
queueio worker queueio=3
```

Monitor the status of active routine invocations:

```sh
queueio monitor
```

Stability
---------

This is a new project.
The design of the public API is under active development and will change.
Release notes will provide clear upgrade instructions,
but backward compatibility and deprecation warnings
will not generally be implemented.
