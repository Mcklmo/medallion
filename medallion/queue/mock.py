import queue
from typing import Any, Iterable

from medallion.stream import Queue, Message


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
        self._queue: queue.Queue = queue.Queue()

        for data, args in messages:
            self._queue.put((data, args, ""))

        self._block = block_when_empty
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._closed = True

    def read_stream(self):
        while not self._closed:
            try:
                entry = (
                    self._queue.get(timeout=0.1)
                    if self._block
                    else self._queue.get_nowait()
                )
                data, args, _ordering_key = entry
                yield Message(
                    data=data,
                    args=args,
                    raw_message=None,
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

        self._queue.put((data, args, ordering_key))
