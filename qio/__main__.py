from typing import Annotated

from typer import Argument
from typer import Typer

from .monitor import Monitor
from .qio import Qio
from .queuespec import QueueSpec
from .worker import Worker

app = Typer()


@app.command()
def routines():
    """Show all registered routines."""
    qio = Qio()
    try:
        routines = qio.routines()

        if not routines:
            print("No routines registered.")
            return

        # Calculate column widths
        name_width = max(len("Name"), max(len(routine.name) for routine in routines))
        function_paths = []
        for routine in routines:
            module = routine.fn.__module__
            qualname = routine.fn.__qualname__
            function_paths.append(f"{module}.{qualname}")
        path_width = max(len("Path"), max(len(path) for path in function_paths))

        print(f"{'Name':<{name_width}} | {'Path':<{path_width}}")
        print(f"{'-' * name_width}-+-{'-' * path_width}")
        for routine, path in zip(routines, function_paths, strict=False):
            print(f"{routine.name:<{name_width}} | {path:<{path_width}}")
    finally:
        qio.shutdown()


@app.command()
def monitor(raw: bool = False):
    """Monitor qio events.

    Shows a live view of qio activity. Use --raw for detailed event output.
    """
    if raw:
        qio = Qio()
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
    qio = Qio()
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
    qio = Qio()
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
