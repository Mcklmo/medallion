import json
import logging
import re
import sys
from typing import Iterable

import pytest

import medallion.log
from medallion.log import (
    JsonFormatter,
    log_file_name,
    set_thread_processor,
    start_process_log,
)
from medallion.model.base import BasePydanticProcessingStep, DataModel, PydanticReader
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BasePydanticStreamingTransformer
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-structured-logs")
_LOGGER.setLevel(logging.INFO)

START = "2026-10-11T13-15-21320"


def _format(**kwargs) -> dict:
    record = _LOGGER.makeRecord(
        _LOGGER.name, logging.INFO, "a.py", 7, "sent %d", (3,), None, **kwargs
    )

    return json.loads(JsonFormatter().format(record))


def test_record_has_the_agreed_shape():
    set_thread_processor("csv-to-model")
    try:
        entry = _format(extra={"bundles": 1, "line": "x", "severity": "y"})
    finally:
        set_thread_processor(None)

    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", entry.pop("time"))
    assert entry == {
        "severity": "INFO",
        "message": "sent 3",
        "processor": "csv-to-model",
        "logger": "test-structured-logs",
        "module": "a",
        "function": None,
        "line": 7,
        "bundles": 1,
        "extra_line": "x",
        "extra_severity": "y",
    }


def test_stack_trace_is_appended_to_the_message():
    try:
        raise ValueError("boom")
    except ValueError:
        record = _LOGGER.makeRecord(
            _LOGGER.name, logging.ERROR, "a.py", 1, "failed", (), sys.exc_info()
        )

    entry = json.loads(JsonFormatter().format(record))

    assert entry["message"].startswith("failed\nTraceback")
    assert "ValueError: boom" in entry["message"]
    assert "exception" not in entry
    assert "processor" not in entry


def test_unserialisable_extra_is_written_as_text():
    assert _format(extra={"obj": object()})["obj"].startswith("<object object")


def test_file_name_joins_processors_in_run_order():
    assert log_file_name(START, ["extract-a"]) == f"{START}-extract-a.jsonl"
    assert (
        log_file_name(START, ["extract-a", "transform-b", "store-c"])
        == f"{START}-extract-a-transform-b-store-c.jsonl"
    )


def test_file_name_drops_whole_names_past_255_bytes():
    names = ["extract-a"] + [f"transform-{i:040d}" for i in range(6)]

    name = log_file_name(START, names)

    assert len(name.encode()) <= 255
    assert name == f"{START}-{'-'.join(names[:5])}-and-2-more.jsonl"


def test_file_name_fails_when_even_the_extractor_does_not_fit():
    with pytest.raises(ValueError):
        log_file_name(START, ["x" * 240, "transform-b"])


class Item(DataModel):
    id: int


class Items(BaseExtractor[Item], BasePydanticProcessingStep[Item]):
    def extract(self) -> Iterable[Item]:
        self.logger.info("extracting")

        return [Item(id=i) for i in range(3)]


class Double(BasePydanticStreamingTransformer[Item, Item], PydanticReader[Item]):
    def transform_one(self, data: Item) -> Item:
        self.logger.info("doubling")

        return Item(id=data.id * 2)


def test_local_pipeline_run_writes_one_file_naming_each_processor(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "false")
    monkeypatch.setenv("MEDALLION_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("MEDALLION_WRITE_LOG_FILES", raising=False)

    pipe = PipeLine(
        extractor=Items(_LOGGER),
        transformers=[Double(_LOGGER)],
        queues=[MockQueue() for _ in range(2)],
        logger=_LOGGER,
        store_output=LocalStorage(str(tmp_path / "data"), _LOGGER),
        force_run_transformer=False,
        processor_names=["extract-items", "transform-double"],
    )
    handler = start_process_log(pipe.processor_names)
    try:
        pipe.run()
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()

    files = list((tmp_path / "logs").rglob("*.jsonl"))
    assert len(files) == 1
    assert re.fullmatch(
        r"\d{4}/\d\d/\d\d/\d\d/[\dT-]+-extract-items-transform-double\.jsonl",
        files[0].relative_to(tmp_path / "logs").as_posix(),
    )

    lines = [json.loads(line) for line in files[0].read_text().splitlines()]
    processor_by_message = {line["message"]: line.get("processor") for line in lines}
    assert processor_by_message["extracting"] == "extract-items"
    assert processor_by_message["doubling"] == "transform-double"
    assert {line.get("processor") for line in lines} <= {
        "extract-items",
        "transform-double",
        None,
    }


def test_no_file_when_writing_files_is_off(monkeypatch):
    monkeypatch.setenv("MEDALLION_WRITE_LOG_FILES", "false")
    monkeypatch.setattr(medallion.log, "_process_processor", None)

    assert start_process_log(["extract-a"]) is None


def test_local_run_names_processors_as_config_yml_does(tmp_path, monkeypatch):
    import medallion.medallion

    config = tmp_path / "config.yml"
    config.write_text("""
extractors:
  - {name: a, class: Ex, writes_to: q1}
transformers:
  - {name: b-on-q2, class: Tr, reads_from: q2, writes_to: q3}
  - {name: b-on-q1, class: Tr, reads_from: q1, writes_to: q2}
stores:
  - {name: c, class: St, reads_from: q2}
""")
    monkeypatch.setattr(medallion.medallion, "DEFAULT_CONFIG_YAML_FILE_NAME", config)

    assert medallion.medallion.config_processor_names(["Ex", "Tr", "St"]) == [
        "extract-a",
        "transform-b-on-q1",
        "store-c",
    ]
    assert medallion.medallion.config_processor_names(["Other", "Tr"]) == [
        "Other",
        "Tr",
    ]
