import queue
from typing import Iterable

from medallion.stream import MessageConsumer, Message


class MockConsumer(MessageConsumer):
    def __init__(
        self,
        messages: Iterable[tuple[bytes, str]] = (),
        block_when_empty: bool = True,
    ):
        self._queue: queue.Queue = queue.Queue()

        for m in messages:
            self._queue.put(m)

        self._block = block_when_empty
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._closed = True

    def messages(self):
        while not self._closed:
            try:
                data = (
                    self._queue.get(timeout=0.1)
                    if self._block
                    else self._queue.get_nowait()
                )
                yield Message(
                    data=data[0],
                    queue_name=data[1],
                    _raw=None,
                )
            except queue.Empty:
                if not self._block:
                    return

    def ack(self, message):
        self._queue.task_done()

    def nack(self, message):
        self._queue.task_done()

    def close(self) -> None:
        self._closed = True

    def wait_drained(self) -> None:
        """Block until every put() (initial + published) has been ack/nack'd."""
        self._queue.join()

    # test helper
    def push(self, data: bytes, queue_name: str) -> None:
        self._queue.put((data, queue_name))

    def publish(self, data: bytes, queue_name: str) -> None:
        if self._closed:
            raise RuntimeError("Queue is closed")

        self._queue.put((data, queue_name))
