from textual.app import App
from textual.app import ComposeResult
from textual.widgets import DataTable
from textual.widgets import Footer
from textual.widgets import Header

from .invocation import Invocation
from .queue import ShutDown
from .queueio import QueueIO
from .result import Err
from .result import Ok
from .thread import Thread


class Monitor(App):
    """TUI for monitoring queueio events."""

    TITLE = "queueio Monitor"

    def __init__(self):
        super().__init__()
        self.__queueio = QueueIO()
        self.__thread = Thread(target=self.__listen)
        self.__events = self.__queueio.subscribe(
            {
                Invocation.Submitted,
                Invocation.Started,
                Invocation.Suspended,
                Invocation.Continued,
                Invocation.Threw,
                Invocation.Resumed,
                Invocation.Completed,
            }
        )

    def __listen(self):
        while True:
            try:
                event = self.__events.get()
            except ShutDown:
                break

            self.call_from_thread(self.handle_invocation_event, event)

    def handle_invocation_event(
        self,
        event: Invocation.Submitted
        | Invocation.Started
        | Invocation.Suspended
        | Invocation.Continued
        | Invocation.Threw
        | Invocation.Resumed
        | Invocation.Completed,
    ):
        table = self.query_one(DataTable)
        match event:
            case Invocation.Submitted():
                table.add_row(
                    event.id,
                    event.routine,
                    "Submitted",
                    self.__queueio.routine(event.routine).queue,
                    key=event.id,
                )
            case Invocation.Started():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Started",
                )
            case Invocation.Suspended():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Suspended",
                )
            case Invocation.Continued():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Continued",
                )
            case Invocation.Threw():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Threw",
                )
            case Invocation.Resumed():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Resumed",
                )
            case Invocation.Completed(result=Ok()):
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Succeeded",
                )
            case Invocation.Completed(result=Err()):
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Errored",
                )

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self):
        table = self.query_one(DataTable)
        self.__column_keys = table.add_columns("ID", "Name", "Status", "Queue")
        self.__thread.start()

    def on_unmount(self) -> None:
        self.__queueio.shutdown()
        self.__thread.join()
