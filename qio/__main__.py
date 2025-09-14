import importlib
import tomllib
from pathlib import Path
from typing import Annotated

from pika import ConnectionParameters
from typer import Argument
from typer import Typer

from .monitor import Monitor
from .pika.broker import PikaBroker
from .pika.transport import PikaTransport
from .qio import Qio
from .queuespec import QueueSpec
from .registry import ROUTINE_REGISTRY
from .worker import Worker


def locate() -> Path | None:
    """Locate the pyproject.toml file."""
    for path in [cwd := Path.cwd(), *cwd.parents]:
        candidate = path / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None


def register():
    """Load routine modules from pyproject.toml."""
    if pyproject := locate():
        try:
            with pyproject.open("rb") as f:
                config = tomllib.load(f)
        except (OSError, tomllib.TOMLDecodeError):
            return

        modules = config.get("tool", {}).get("qio", {}).get("register", [])

        for module_name in modules:
            importlib.import_module(module_name)


app = Typer()


@app.command()
def show():
    """Show all registered routines."""
    register()
    if not ROUTINE_REGISTRY:
        print("No routines registered.")
        return

    # Calculate column widths
    name_width = max(len("Name"), max(len(name) for name in ROUTINE_REGISTRY))
    function_paths = []
    for routine in ROUTINE_REGISTRY.values():
        module = routine.fn.__module__
        qualname = routine.fn.__qualname__
        function_paths.append(f"{module}.{qualname}")
    path_width = max(len("Path"), max(len(path) for path in function_paths))

    print(f"{'Name':<{name_width}} | {'Path':<{path_width}}")
    print(f"{'-' * name_width}-+-{'-' * path_width}")
    for name, path in zip(ROUTINE_REGISTRY.keys(), function_paths, strict=False):
        print(f"{name:<{name_width}} | {path:<{path_width}}")


@app.command()
def monitor(raw: bool = False):
    """Monitor qio events.

    Shows a live view of qio activity. Use --raw for detailed event output.
    """
    if raw:
        connection_params = ConnectionParameters()
        qio = Qio(
            broker=PikaBroker(connection_params),
            transport=PikaTransport(connection_params),
        )
        events = qio.subscribe({object})
        try:
            while True:
                print(events.get())
        except KeyboardInterrupt:
            print("Shutting down gracefully.")
        finally:
            qio.shutdown()
    else:
        Monitor().run()


@app.command()
def worker(
    queuespec: Annotated[
        QueueSpec,
        Argument(
            parser=QueueSpec.parse,
            help="Queue configuration in format 'queue=concurrency'. "
            "Examples: 'production=10', 'api,background=5'",
            metavar="QUEUE[,QUEUE2,...]=CONCURRENCY",
        ),
    ],
):
    """Start a worker to process from a queue.

    The worker will process invocations from the specified queue,
    as many at a time as specified by the concurrency.
    """
    register()
    connection_params = ConnectionParameters()
    qio = Qio(
        broker=PikaBroker(connection_params),
        transport=PikaTransport(connection_params),
    )
    Worker(qio, queuespec)()


@app.command()
def purge(
    queues: Annotated[
        str,
        Argument(
            help="Comma-separated list of queues to purge. "
            "Examples: 'qio', 'production,background'",
            metavar="QUEUE[,QUEUE2,...]",
        ),
    ],
):
    """Purge all messages from some queues.

    This will remove all pending messages from the given queues.
    Use with caution as this operation cannot be undone.
    """
    connection_params = ConnectionParameters()
    qio = Qio(
        broker=PikaBroker(connection_params),
        transport=PikaTransport(connection_params),
    )
    try:
        queue_list = [q.strip() for q in queues.split(",") if q.strip()]
        if not queue_list:
            print("Error: No valid queue names provided")
            return

        for queue in queue_list:
            print(f"Purging queue: {queue}")
            qio.purge(queue=queue)

        print(f"Successfully purged {len(queue_list)} queue(s)")
    finally:
        qio.shutdown()


if __name__ == "__main__":
    app()
