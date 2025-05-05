from queue import ShutDown

from textual.app import App
from textual.app import ComposeResult
from textual.widgets import DataTable
from textual.widgets import Footer
from textual.widgets import Header

from .invocation import InvocationContinued
from .invocation import InvocationEnqueued
from .invocation import InvocationErrored
from .invocation import InvocationResumed
from .invocation import InvocationStarted
from .invocation import InvocationSubmitted
from .invocation import InvocationSucceeded
from .invocation import InvocationSuspended
from .invocation import InvocationThrew
from .qio import Qio
from .thread import Thread


class Monitor(App):
    """TUI for monitoring Qio events."""

    TITLE = "Qio Monitor"

    def __init__(self):
        super().__init__()
        self.__qio = Qio()
        self.__thread = Thread(target=self.__listen)
        self.__events = self.__qio.bus.subscribe(
            {
                InvocationSubmitted,
                InvocationEnqueued,
                InvocationStarted,
                InvocationSuspended,
                InvocationContinued,
                InvocationThrew,
                InvocationResumed,
                InvocationSucceeded,
                InvocationErrored,
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
        | InvocationEnqueued
        | InvocationStarted
        | InvocationSuspended
        | InvocationContinued
        | InvocationThrew
        | InvocationResumed
        | InvocationSucceeded
        | InvocationErrored,
    ):
        table = self.query_one(DataTable)
        match event:
            case InvocationSubmitted():
                table.add_row(
                    event.invocation.id,
                    str(event.timestamp),
                    event.invocation.routine.name,
                    "Submitted",
                    key=event.invocation.id,
                )
            case InvocationEnqueued():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Enqueued",
                )
            case InvocationStarted():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Started",
                )
            case InvocationSuspended():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Suspended",
                )
            case InvocationContinued():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Continued",
                )
            case InvocationThrew():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Threw",
                )
            case InvocationResumed():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Resumed",
                )
            case InvocationSucceeded():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Succeeded",
                )
            case InvocationErrored():
                table.update_cell(
                    event.invocation.id,
                    self.__column_keys[3],
                    "Errored",
                )

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self):
        table = self.query_one(DataTable)
        self.__column_keys = table.add_columns("ID", "Time", "Name", "Status")
        self.__thread.start()

    def on_unmount(self) -> None:
        self.__qio.shutdown()
        self.__thread.join()
