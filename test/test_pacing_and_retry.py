import logging
import os
import signal
import threading
import time
from typing import Iterable

import pytest

from medallion import RetryLater
from medallion.model.base import BasePydanticProcessingStep, DataModel, PydanticReader
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BasePydanticStreamingTransformer
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.run.transformer import TransformerListener
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-pacing-and-retry")


class Item(DataModel):
    id: int


class Recorder(BasePydanticStreamingTransformer[Item, Item], PydanticReader[Item]):
    """Records when each call starts and how many run at once."""

    def __init__(self, logger):
        super().__init__(logger)
        self.lock = threading.Lock()
        self.starts: list[tuple[float, int]] = []
        self.in_flight = 0
        self.peak = 0

    def transform_one(self, data: Item) -> Item:
        with self.lock:
            self.starts.append((time.monotonic(), data.id))
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)

        try:
            self.work(data, sum(1 for _, i in self.starts if i == data.id))
        finally:
            with self.lock:
                self.in_flight -= 1

        return data

    def work(self, data: Item, attempt: int) -> None:
        pass


def _run(tmp_path, monkeypatch, step: Recorder, count: int) -> None:
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "false")

    class Items(BaseExtractor[Item], BasePydanticProcessingStep[Item]):
        def extract(self) -> Iterable[Item]:
            return [Item(id=i) for i in range(count)]

    PipeLine(
        extractor=Items(_LOGGER),
        transformers=[step],
        queues=[MockQueue() for _ in range(2)],
        logger=_LOGGER,
        store_output=LocalStorage(str(tmp_path), _LOGGER),
        force_run_transformer=False,
    ).run()


class Paced(Recorder):
    min_interval = 0.05
    max_concurrent_messages = 8

    def work(self, data, attempt):
        time.sleep(0.02)


def test_calls_are_spaced_across_all_threads(tmp_path, monkeypatch):
    step = Paced(_LOGGER)
    _run(tmp_path, monkeypatch, step, 10)

    starts = sorted(t for t, _ in step.starts)
    assert len(starts) == 10
    assert min(b - a for a, b in zip(starts, starts[1:])) >= 0.045


class FailsTwice(Recorder):
    def work(self, data, attempt):
        if data.id == 0 and attempt <= 2:
            raise RetryLater("busy", after=0.05)


def test_retry_later_then_success_completes(tmp_path, monkeypatch):
    step = FailsTwice(_LOGGER)
    _run(tmp_path, monkeypatch, step, 5)

    assert len(step.starts) == 5 + 2


class PausesOnce(Recorder):
    max_concurrent_messages = 4
    retry_at = 0.0

    def work(self, data, attempt):
        time.sleep(0.01)

        if data.id == 0 and attempt == 1:
            self.retry_at = time.monotonic()
            raise RetryLater("busy", after=0.5)


def test_a_pause_holds_every_thread(tmp_path, monkeypatch):
    step = PausesOnce(_LOGGER)
    _run(tmp_path, monkeypatch, step, 30)

    assert len(step.starts) == 30 + 1
    during_pause = [
        t for t, _ in step.starts if step.retry_at + 0.05 < t < step.retry_at + 0.45
    ]
    assert during_pause == []


class AlwaysBusy(Recorder):
    max_retries_later = 2

    def work(self, data, attempt):
        raise RetryLater("busy", after=0.01)


def test_too_many_retries_stop_the_run(tmp_path, monkeypatch):
    step = AlwaysBusy(_LOGGER)

    with pytest.raises(RuntimeError, match="after 2 retries: busy"):
        _run(tmp_path, monkeypatch, step, 1)

    assert len(step.starts) == 3


class LongPause(Recorder):
    def work(self, data, attempt):
        raise RetryLater("busy", after=60)


def test_ctrl_c_during_a_pause_stops_within_seconds(tmp_path, monkeypatch):
    step = LongPause(_LOGGER)
    threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGINT)).start()
    started = time.monotonic()

    with pytest.raises(KeyboardInterrupt):
        _run(tmp_path, monkeypatch, step, 20)

    assert time.monotonic() - started < 2


class OneAtATime(Recorder):
    max_concurrent_messages = 1

    def work(self, data, attempt):
        time.sleep(0.01)


def test_step_limits_messages_in_flight(tmp_path, monkeypatch):
    step = OneAtATime(_LOGGER)
    _run(tmp_path, monkeypatch, step, 10)

    assert len(step.starts) == 10
    assert step.peak == 1


class OneAtATimeBatched(Recorder):
    max_concurrent_messages = 1
    batch_size = 4


def test_batch_size_raises_the_step_limit(tmp_path):
    listener = TransformerListener(
        messages_in=MockQueue(),
        messages_out=MockQueue(),
        logger=_LOGGER,
        transformer=OneAtATimeBatched(_LOGGER),
        max_retries=0,
        should_start_health_server=False,
        store=LocalStorage(str(tmp_path), _LOGGER),
        force_run_transformer=False,
    )

    assert listener.max_concurrent_messages == 4


class SlowPaced(Recorder):
    min_interval = 0.0


def test_cache_hits_skip_pacing(tmp_path, monkeypatch):
    step = SlowPaced(_LOGGER)
    _run(tmp_path, monkeypatch, step, 5)
    monkeypatch.setattr(SlowPaced, "min_interval", 1.0)
    started = time.monotonic()

    _run(tmp_path, monkeypatch, step, 5)

    assert len(step.starts) == 5
    assert time.monotonic() - started < 1
