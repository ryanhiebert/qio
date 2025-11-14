from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable

from .message import Message


class Receiver(Iterable[Message], ABC):
    """Receive and report on messages from a message source.

    Iterating the receiver delivers messages, which can pause, unpause,
    or finish as the messages are processed.

    Under normal operation, all messages will finish. Messages may pause
    to temporarily release their assigned capacity. Paused messages
    should unpause later.

    Paused messages must unpause before they finish. Messages must not unpause
    if they have not first been paused.
    """

    @abstractmethod
    def pause(self, message: Message, /):
        """Pause processing of a message.

        The message processing is not completed, and is expected to unpause,
        but its assigned capacity may be allocated elsewhere temporarily.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def unpause(self, message: Message, /):
        """Unpause processing of a message.

        The previously paused message processing is resuming, so its assigned
        capacity is no longer available for allocation elsewhere.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def finish(self, message: Message, /):
        """Finish processing a message.

        The message is done processing, and its assigned capacity may be
        allocated elsewhere permanently.
        """
        raise NotImplementedError("Subclasses must implement this method.")
