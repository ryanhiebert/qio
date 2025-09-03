from dataclasses import dataclass
from typing import Self


@dataclass
class QueueSpec:
    """The parsed specification for how a worker will process tasks.

    The queues are all the queues that are assigned to this amount of
    concurrency. The exact implementation is broker-specific, but invocations
    will often be roughly taken in round-robin order between the queues.
    """

    queues: list[str]
    concurrency: int

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse a queue spec string.

        Examples:

        | queuespec          | queues               | concurrency      |
        +--------------------+----------------------+------------------+
        | queue2=5           | ['queue2']           | 5                |
        | queue1,queue2=10   | ['queue1', 'queue2'] | 10               |
        """

        if not value or value.strip() == "":
            raise ValueError("Queue spec cannot be empty")

        if "=" not in value:
            raise ValueError(
                f"Invalid queue spec. Be sure to include both the queue and capacity. "
                f"got: '{value}'"
            )

        # Left split instead of right split to make = invalid for queue names
        raw_queues, raw_concurrency = value.split("=", 1)

        try:
            concurrency = int(raw_concurrency)
        except ValueError:
            raise ValueError(
                f"Concurrency must be a positive integer, "
                f"got: '{raw_concurrency}' in '{value}'"
            ) from None
        if concurrency <= 0:
            raise ValueError(
                f"Concurrency must be a positive integer, "
                f"got: '{raw_concurrency}' in '{value}'"
            )

        queues = [q.strip() for q in raw_queues.split(",") if q.strip()]

        if not queues:
            raise ValueError(f"No valid queue names found in '{value}'")

        return cls(queues=queues, concurrency=concurrency)
