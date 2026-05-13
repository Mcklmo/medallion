from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Iterator, Any


class Message(BaseModel):
    """A message received from the queue."""

    data: bytes
    queue_name: str  # populated by the consumer
    _raw: Any  # underlying message object, needed for ack/nack


class MessageConsumer(ABC):
    @abstractmethod
    def __enter__(self) -> "MessageConsumer":
        """Open connection / subscribe."""

    @abstractmethod
    def __exit__(
        self,
        exc_type,
        exc_val,
        exc_tb,
    ) -> None:
        """Close connection cleanly."""

    @abstractmethod
    def messages(self) -> Iterator[Message]:
        """Yield messages indefinitely until the consumer is closed."""

    @abstractmethod
    def ack(self, message: Message) -> None:
        """Acknowledge successful processing."""

    @abstractmethod
    def nack(self, message: Message) -> None:
        """Reject a message so it can be redelivered."""

    @abstractmethod
    def publish(
        self,
        data: bytes,
        queue_name: str,
    ) -> None:
        """Publish a message to the queue."""
