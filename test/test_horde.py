import re
import threading

from pydantic import ValidationError
import pytest

from medallion.base import BaseExtractor, BaseTransformer
from medallion.consumer.mock import MockConsumer
from medallion.horde import Horde, generate_type_mismatch_message
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


class MockExtractor(MockProcessingStep, BaseExtractor[dict]):
    def extract(self) -> dict:
        return {}

    def read_bytes(self, data: BytesIO) -> list[dict]:
        return []

    @property
    def queue_to(self):
        return MESSAGE_QUEUE_NAME_1


class MockTransformer1(MockProcessingStep, BaseTransformer[dict, dict]):
    @property
    def queue_from(self):
        return MESSAGE_QUEUE_NAME_1

    def transform(self, data: dict) -> dict:
        return data

    def read_bytes(self, data: BytesIO) -> list[dict]:
        return []

    @property
    def queue_to(self):
        return MESSAGE_QUEUE_NAME_3


class MockTransformer2(MockProcessingStep, BaseTransformer[dict, dict]):
    @property
    def queue_from(self):
        return MESSAGE_QUEUE_NAME_2

    def transform(self, data: dict) -> dict:
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

    assert (
        f"Received message from queue {MESSAGE_QUEUE_NAME_1}, dispatching to 1 transformers[{MockTransformer1.__name__}]"
        in out
    )
    assert (
        f"Received message from queue {MESSAGE_QUEUE_NAME_2}, dispatching to 1 transformers[{MockTransformer2.__name__}]"
        in out
    )
    assert (
        f"Received message from queue {MESSAGE_QUEUE_NAME_3}, but did not find any transformers"
        in out
    )
    assert (
        f"Received message from queue {MESSAGE_QUEUE_NAME_4}, but did not find any transformers"
        in out
    )


class MockTransformer3(MockProcessingStep, BaseTransformer[int, dict]):
    @property
    def queue_from(self):
        return MESSAGE_QUEUE_NAME_1

    def transform(self, data: dict) -> dict:
        return data

    def read_bytes(self, data: BytesIO) -> list[dict]:
        return []

    @property
    def queue_to(self):
        return MESSAGE_QUEUE_NAME_4


def test_transformer_type_mismatch():
    logger, buf = _make_capture_logger()
    consumer = MockConsumer(
        messages=[
            (
                b"data1",
                MESSAGE_QUEUE_NAME_1,
            ),
        ]
    )
    with pytest.raises(
        ValidationError,
        match=re.escape(
            generate_type_mismatch_message(
                MockExtractor(),
                MockTransformer3(),
            )
        ),
    ):
        _ = Horde(
            message_consumer=consumer,
            logger=logger,
            transformers=[
                MockTransformer3(),
            ],
            extractors=[
                MockExtractor(),
            ],
        )
