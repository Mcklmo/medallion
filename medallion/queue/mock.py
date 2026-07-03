import queue
import threading
from typing import Any, Iterable

from medallion.queue.base import Queue, Message


class MockQueue[T](Queue):
    def __init__(
        self,
        messages: Iterable[
            tuple[
                bytes,
                dict[str, Any],
            ]
        ] = (),
        block_when_empty: bool = True,
    ):
        self._initial: list[tuple[bytes, dict[str, Any], str]] = [
            (data, args, "") for data, args in messages
        ]
        self._subscribers: list[queue.Queue] = []
        self._lock = threading.Lock()

        self._block = block_when_empty
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._closed = True

    def read_stream(self) -> Iterable[Message]:
        inbox: queue.Queue = queue.Queue()
        with self._lock:
            for entry in self._initial:
                inbox.put(entry)

            self._subscribers.append(inbox)

        try:
            while True:
                try:
                    entry = (
                        inbox.get(timeout=0.1) if self._block else inbox.get_nowait()
                    )
                    data, args, _ordering_key = entry
                    yield Message(
                        data=data,
                        args=args,
                        raw_message=inbox,
                        delivery_attempt=1,
                    )
                except queue.Empty:
                    if self._closed or not self._block:
                        return
        finally:
            with self._lock:
                try:
                    self._subscribers.remove(inbox)
                except ValueError:
                    pass

            # Drop anything this consumer will never process so that
            # wait_drained() does not block forever on an abandoned inbox.
            while True:
                try:
                    inbox.get_nowait()
                    inbox.task_done()
                except queue.Empty:
                    break

    def ack(self, message) -> None:
        inbox: queue.Queue = message.raw_message
        inbox.task_done()

    def nack(self, message) -> None:
        inbox: queue.Queue = message.raw_message
        inbox.task_done()

    def close(self) -> None:
        self._closed = True

    def wait_drained(self) -> None:
        """Block until every put() (initial + published) has been ack/nack'd."""
        with self._lock:
            inboxes = list(self._subscribers)

        for inbox in inboxes:
            inbox.join()

    def write(
        self,
        data: bytes,
        args: dict[
            str,
            Any,
        ],
        ordering_key: str,
    ) -> None:
        if self._closed:
            raise RuntimeError("Queue is closed")

        with self._lock:
            inboxes = list(self._subscribers)

        for inbox in inboxes:
            inbox.put((data, args, ordering_key))
