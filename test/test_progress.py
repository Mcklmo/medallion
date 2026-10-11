import json
import logging
import re
from typing import Iterable

import pytest

import medallion.progress
from medallion import RateLimited
from medallion.log import JsonFormatter
from medallion.model.base import BasePydanticProcessingStep, DataModel, PydanticReader
from medallion.model.extractor import (
    ARG_EXECUTION_START_TIME,
    ARG_ITEM_INDEX,
    ARG_PREVIOUS_STEPS,
    BaseExtractor,
)
from medallion.model.store import BaseStore
from medallion.model.transformer import BasePydanticStreamingTransformer
from medallion.pipeline import PipeLine
from medallion.progress import Progress
from medallion.queue.mock import MockQueue
from medallion.run.transformer import TransformerListener
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-progress")
_LOGGER.setLevel(logging.INFO)


class Item(DataModel):
    id: int


class Double(BasePydanticStreamingTransformer[Item, Item], PydanticReader[Item]):
    rate_limited = False

    def transform_one(self, data: Item) -> Item:
        if data.id == 5 and not self.rate_limited:
            self.rate_limited = True
            raise RateLimited("429")

        if data.id == 7:
            raise ValueError("bad item")

        return Item(id=data.id * 2)


class Sink(BaseStore[Item], PydanticReader[Item]):
    def store_message_data(self, *args, **kwargs) -> None:
        pass


class _JsonLines(logging.Handler):
    """Formats on the logging thread, where the processor is known."""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(JsonFormatter())
        self.lines: list[dict] = []

    def emit(self, record):
        self.lines.append(json.loads(self.format(record)))


def _run(tmp_path, count: int) -> None:
    class Items(BaseExtractor[Item], BasePydanticProcessingStep[Item]):
        def extract(self) -> Iterable[Item]:
            return [Item(id=i) for i in range(count)]

    PipeLine(
        extractor=Items(_LOGGER),
        transformers=[Double(_LOGGER)],
        store=Sink(_LOGGER),
        queues=[MockQueue() for _ in range(3)],
        logger=_LOGGER,
        store_output=LocalStorage(str(tmp_path), _LOGGER),
        force_run_transformer=False,
        processor_names=["items", "double", "sink"],
    ).run()


def test_a_local_run_logs_each_processors_counts(tmp_path, monkeypatch):
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "true")
    monkeypatch.setenv("MEDALLION_SHOW_PROGRESS", "true")
    _run(tmp_path, 3)

    monkeypatch.setattr(medallion.progress, "LOG_INTERVAL", 1e-9)
    handler = _JsonLines()
    logging.getLogger().addHandler(handler)
    try:
        _run(tmp_path, 6)
    finally:
        logging.getLogger().removeHandler(handler)

    final = {}
    for line in handler.lines:
        if "handled" in line and line["handled"] > final.get(line["processor"], {}).get(
            "handled", 0
        ):
            final[line["processor"]] = line

    # item 5's RateLimited requeue is not counted; items 0-2 are cached from the first run
    assert {p: (l["handled"], l["cached"], l["failed"]) for p, l in final.items()} == {
        "items": (6, 0, 0),
        "double": (6, 3, 0),
        "sink": (6, 0, 0),
    }
    # the total arrives once extraction ends, which may be before or after double's last count
    assert re.match(
        r"double: 6( of 6)? handled \(cached=3 failed=0\), ", final["double"]["message"]
    )
    assert final["double"]["per_second"] > 0


def test_dead_lettered_messages_count_as_failed(tmp_path):
    progress = Progress("double", _LOGGER, None)
    args = {
        ARG_EXECUTION_START_TIME: "2026-10-11T13-15-21320",
        ARG_PREVIOUS_STEPS: ["items"],
    }
    TransformerListener(
        messages_in=MockQueue(
            [
                (Item(id=i).model_dump_json().encode(), {**args, ARG_ITEM_INDEX: i})
                for i in (6, 7)
            ],
            block_when_empty=False,
        ),
        messages_out=MockQueue(),
        dlq=MockQueue(),
        progress=progress,
        logger=_LOGGER,
        transformer=Double(_LOGGER),
        max_retries=1,
        should_start_health_server=False,
        store=LocalStorage(str(tmp_path), _LOGGER),
        force_run_transformer=False,
    ).listen()

    assert (progress.handled, progress.cached, progress.failed) == (2, 0, 1)


def test_show_progress_rejects_other_values(monkeypatch):
    monkeypatch.setenv("MEDALLION_SHOW_PROGRESS", "yes")

    with pytest.raises(ValueError, match="must be true or false"):
        with medallion.progress.show_progress(["items"], _LOGGER):
            pass


class Split(BasePydanticStreamingTransformer[Item, Item], PydanticReader[Item]):
    fan_out = True

    def transform_one(self, data: Item) -> list[Item]:
        return [data, data]


def test_totals_reach_every_processor_up_to_the_first_fan_out(tmp_path, monkeypatch):
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "true")

    class Items(BaseExtractor[Item], BasePydanticProcessingStep[Item]):
        def extract(self) -> Iterable[Item]:
            return []

    pipeline = PipeLine(
        extractor=Items(_LOGGER),
        transformers=[Double(_LOGGER), Split(_LOGGER), Double(_LOGGER)],
        store=Sink(_LOGGER),
        queues=[MockQueue() for _ in range(5)],
        logger=_LOGGER,
        store_output=LocalStorage(str(tmp_path), _LOGGER),
        force_run_transformer=False,
        processor_names=["items", "double", "split", "double-again", "sink"],
    )
    progress = [Progress(name, _LOGGER, None) for name in pipeline.processor_names]
    for _ in range(10):
        progress[0].count()

    pipeline._set_totals(progress)

    # split still takes one message per item; after it, a message is a part
    assert [p.total for p in progress] == [10, 10, 10, None, None]
