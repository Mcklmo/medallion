from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict, Field
from typing import Iterator, Any


class Message(BaseModel):
    """A message received from the queue."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    data: bytes
    args: dict[str, Any]
    raw_message: Any = Field(  # underlying message object, needed for ack/nack
        default=None,
        exclude=True,
        repr=False,
    )
    delivery_attempt: int | None = None


class QueueReader(ABC):
    @abstractmethod
    def read_stream(self) -> Iterator[Message]:
        """Yield messages indefinitely until the consumer is closed."""

    @abstractmethod
    def ack(self, message: Message) -> None:
        """Acknowledge successful processing.

        Must be safe to call concurrently from multiple threads.
        """

    @abstractmethod
    def nack(self, message: Message) -> None:
        """Reject a message so it can be redelivered.

        Must be safe to call concurrently from multiple threads.
        """

    @abstractmethod
    def close(self) -> None:
        """Signal `read_stream` to stop yielding and return.

        Idempotent; safe to call from any thread. Does not release resources —
        that still happens in `__exit__`.
        """


class QueueWriter(ABC):
    @abstractmethod
    def write(
        self,
        data: bytes,
        args: dict[
            str,
            Any,
        ],
        ordering_key: str,
    ) -> None:
        """Publish a message to the queue.

        Must be safe to call concurrently from multiple threads.

        ordering_key: messages sharing a key are delivered in publish order
        to subscribers with message ordering enabled. Required so callers
        always make an explicit ordering decision.
        """


class Queue(
    QueueReader,
    QueueWriter,
    ABC,
):
    @abstractmethod
    def __enter__(self) -> "Queue":
        """Open connection / subscribe."""

    @abstractmethod
    def __exit__(
        self,
        exc_type,
        exc_val,
        exc_tb,
    ) -> None:
        """Close connection cleanly."""
