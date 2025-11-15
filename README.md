![queueio](logo.svg)

Queues with an async twist
==========================

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
# sample.py
from queueio import QueueIO
from queueio import routine
from queueio.gather import gather
from queueio.sleep import sleep
from time import sleep as time_sleep


@routine(name="blocking", queue="queueio")
def blocking():
    pass

@routine(name="yielding", queue="queueio")
async def yielding(iterations: int):
    pass


if __name__ == "__main__":
    QueueIO().submit(yielding())  
```

Add the configuration to your `pyproject.toml`:

```toml
[tool.queueio]
# Configure RabbitMQ
broker = 'pika://guest:guest@localhost:5672/'
journal = 'pika://guest:guest@localhost:5672/'
# Register the modules that the worker should load to find your routines
register = ["sample"]
```

The broker and journal can be configured with environment variables
to allow a project to be deployed in multiple environments.

```sh
QUEUEIO_BROKER='amqp://guest:guest@localhost:5672/'
QUEUEIO_JOURNAL='amqp://guest:guest@localhost:5672/'
```

Run your script to submit the routine to run on a worker:

```sh
python sample.py
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
The design of the public API is under active development and subject to change.
Release notes will provide clear upgrade instructions,
but backward compatibility and deprecation warnings
will not generally be implemented.