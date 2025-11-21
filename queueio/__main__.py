from typing import Annotated

from typer import Argument
from typer import Typer

from .monitor import Monitor
from .queueio import QueueIO
from .queuespec import QueueSpec
from .worker import Worker

app = Typer()

routine_app = Typer()
queue_app = Typer()

app.add_typer(
    routine_app,
    name="routine",
    help="A function to coordinate background execution.",
    rich_help_panel="Entities",
)
app.add_typer(
    queue_app,
    name="queue",
    help="An ordered collection of work items to process.",
    rich_help_panel="Entities",
)


@routine_app.command("list")
def routine_list():
    """Show all registered routines."""
    queueio = QueueIO()
    try:
        routines = queueio.routines()

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
        queueio.shutdown()


@app.command(rich_help_panel="Commands")
def monitor(raw: bool = False):
    """Monitor queueio events.

    Show a live view of queueio activity. Use --raw for detailed event output.
    """
    if raw:
        queueio = QueueIO()
        events = queueio.subscribe({object})
        try:
            while True:
                print(events.get())
        except KeyboardInterrupt:
            print("Shutting down gracefully.")
        finally:
            queueio.shutdown()
    else:
        Monitor().run()


@app.command(rich_help_panel="Commands")
def run(
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
    """Run a worker to process from a queue.

    The worker will process invocations from the specified queue,
    as many at a time as specified by the concurrency.
    """
    queueio = QueueIO()
    Worker(queueio, queuespec)()


@app.command(rich_help_panel="Commands")
def sync():
    """Sync known queues to the broker."""
    queueio = QueueIO()
    try:
        routines = queueio.routines()

        if not routines:
            print("No routines registered.")
            return

        queues = sorted({routine.queue for routine in routines})

        print(f"Syncing queues for {len(routines)} routine(s):")
        for queue in queues:
            print(f"  Ensuring queue exists: {queue}")
            queueio.create(queue=queue)

        print(f"Successfully synced {len(queues)} queue(s)")
    finally:
        queueio.shutdown()


@queue_app.command("purge")
def queue_purge(
    queues: Annotated[
        str,
        Argument(
            help="Comma-separated list of queues to purge. "
            "Examples: 'queueio', 'production,background'",
            metavar="QUEUE[,QUEUE2,...]",
        ),
    ],
):
    """Purge all messages from some queues.

    This will remove all pending messages from the given queues.
    Use with caution as this operation cannot be undone.
    """
    queueio = QueueIO()
    try:
        queue_list = [q.strip() for q in queues.split(",") if q.strip()]
        if not queue_list:
            print("Error: No valid queue names provided")
            return

        for queue in queue_list:
            print(f"Purging queue: {queue}")
            queueio.purge(queue=queue)

        print(f"Successfully purged {len(queue_list)} queue(s)")
    finally:
        queueio.shutdown()


if __name__ == "__main__":
    app()
