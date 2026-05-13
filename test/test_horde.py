import threading

from medallion.base import BaseExtractor, BaseTransformer
from medallion.consumer.mock import MockConsumer
from medallion.horde import Horde
from test.test_resolve_classes import _make_capture_logger
from io import BytesIO

MESSAGE_QUEUE_NAME_1 = "queue1"
MESSAGE_QUEUE_NAME_2 = "queue2"
MESSAGE_QUEUE_NAME_3 = "queue3"
MESSAGE_QUEUE_NAME_4 = "queue4"


class MockProcessingStep:
    @property
    def file_extension(self) -> str:
        return "txt"

    def write_output(self, output_data) -> BytesIO:
        return BytesIO(b"")


class MockExtractor(MockProcessingStep, BaseExtractor[list[dict]]):
    def extract(self) -> list[dict]:
        return []

    def read_bytes(self, data: BytesIO) -> list[dict]:
        return []

    @property
    def queue_to(self):
        return MESSAGE_QUEUE_NAME_1


class MockTransformer1(MockProcessingStep, BaseTransformer[list[dict], list[dict]]):
    @property
    def queue_from(self):
        return MESSAGE_QUEUE_NAME_1

    def transform(self, data: list[dict]) -> list[dict]:
        return data

    def read_bytes(self, data: BytesIO) -> list[dict]:
        return []

    @property
    def queue_to(self):
        return MESSAGE_QUEUE_NAME_3


class MockTransformer2(MockProcessingStep, BaseTransformer[list[dict], list[dict]]):
    @property
    def queue_from(self):
        return MESSAGE_QUEUE_NAME_2

    def transform(self, data: list[dict]) -> list[dict]:
        return data

    def read_bytes(self, data: BytesIO) -> list[dict]:
        return []

    @property
    def queue_to(self):
        return MESSAGE_QUEUE_NAME_4


def test_happy_path():
    logger, buf = _make_capture_logger()
    consumer = MockConsumer(
        messages=[
            (
                b"data1",
                MESSAGE_QUEUE_NAME_1,
            ),
            (
                b"data2",
                MESSAGE_QUEUE_NAME_2,
            ),
        ]
    )
    horde = Horde(
        message_consumer=consumer,
        logger=logger,
        transformers=[
            MockTransformer1(),
            MockTransformer2(),
        ],
        extractors=[
            MockExtractor(),
        ],
    )

    t = threading.Thread(target=horde.run)
    t.start()

    consumer.wait_drained()
    consumer.close()
    t.join()

    out = buf.getvalue()

    out == f"""Received message from queue {MESSAGE_QUEUE_NAME_1}, dispatching to 1 transformers[{MockTransformer1.name}]
Received message from queue {MESSAGE_QUEUE_NAME_2}, dispatching to 1 transformers[{MockTransformer2.name}]
No transformers found for queue {MESSAGE_QUEUE_NAME_3}
No transformers found for queue {MESSAGE_QUEUE_NAME_4}
"""
