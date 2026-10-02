import logging
import threading
import time
from typing import Iterable

import pytest

from medallion.model.base import BasePydanticProcessingStep, DataModel, PydanticReader
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BasePydanticStreamingTransformer
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-fatal-error-stops-run")


class Item(DataModel):
    id: int


class Items(BaseExtractor[Item], BasePydanticProcessingStep[Item]):
    def extract(self) -> Iterable[Item]:
        return [Item(id=i) for i in range(500)]


class Fail(BasePydanticStreamingTransformer[Item, Item], PydanticReader[Item]):
    calls = 0
    lock = threading.Lock()

    def transform_one(self, data: Item) -> Item:
        with Fail.lock:
            Fail.calls += 1

        # a slow failure, like an HTTP error: the extractor has queued everything by the time it lands
        time.sleep(0.05)
        raise RuntimeError("429 Too Many Requests")


def test_a_fatal_error_stops_the_run_without_processing_the_rest(tmp_path, monkeypatch):
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "false")

    with pytest.raises(RuntimeError):
        PipeLine(
            extractor=Items(_LOGGER),
            transformers=[Fail(_LOGGER)],
            queues=[MockQueue() for _ in range(2)],
            logger=_LOGGER,
            store_output=LocalStorage(str(tmp_path), _LOGGER),
            force_run_transformer=False,
        ).run()

    # at most the messages already in flight when the first one failed
    assert Fail.calls <= 8
