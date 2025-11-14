from textual.app import App
from textual.app import ComposeResult
from textual.widgets import DataTable
from textual.widgets import Footer
from textual.widgets import Header

from .invocation import InvocationCompleted
from .invocation import InvocationContinued
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSubmitted
from .invocation import InvocationSuspended
from .invocation import InvocationThrew
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
                InvocationSubmitted,
                InvocationStarted,
                InvocationSuspended,
                InvocationContinued,
                InvocationThrew,
                InvocationResumed,
                InvocationCompleted,
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
        event: InvocationSubmitted
        | InvocationStarted
        | InvocationSuspended
        | InvocationContinued
        | InvocationThrew
        | InvocationResumed
        | InvocationCompleted,
    ):
        table = self.query_one(DataTable)
        match event:
            case InvocationSubmitted():
                table.add_row(
                    event.id,
                    event.routine,
                    "Submitted",
                    self.__queueio.routine(event.routine).queue,
                    key=event.id,
                )
            case InvocationStarted():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Started",
                )
            case InvocationSuspended():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Suspended",
                )
            case InvocationContinued():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Continued",
                )
            case InvocationThrew():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Threw",
                )
            case InvocationResumed():
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Resumed",
                )
            case InvocationCompleted(result=Ok()):
                table.update_cell(
                    event.id,
                    self.__column_keys[2],
                    "Succeeded",
                )
            case InvocationCompleted(result=Err()):
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
