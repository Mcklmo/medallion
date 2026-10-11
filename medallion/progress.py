import sys
import threading
import time
from contextlib import contextmanager
from logging import Logger
from typing import Iterator

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from medallion.store.base import get_env_or_default

SHOW_PROGRESS_ENV_VAR = "MEDALLION_SHOW_PROGRESS"
LOG_INTERVAL = 60.0


class Progress:
    """Counts a processor's handled messages, as a bar on a terminal or else as a line every LOG_INTERVAL seconds."""

    def __init__(self, processor: str, logger: Logger, bar: tqdm | None) -> None:
        self._processor = processor
        self._logger = logger
        self._bar = bar
        self._lock = threading.Lock()
        self.handled = 0
        self.cached = 0
        self.failed = 0
        self.total: int | None = None
        self._logged_at = time.monotonic()
        self._logged_handled = 0

    def set_total(self, total: int) -> None:
        with self._lock:
            self.total = total

            if self._bar is not None:
                self._bar.total = total
                self._bar.refresh()

    def count(self, cached: bool = False, failed: bool = False) -> None:
        with self._lock:
            self.handled += 1
            self.cached += cached
            self.failed += failed
            handled, cached_count, failed_count = self.handled, self.cached, self.failed
            total = self.total

            if self._bar is not None:
                self._bar.set_postfix_str(
                    f"cached={cached_count:,} failed={failed_count:,}", refresh=False
                )
                self._bar.update()

                return

            now = time.monotonic()
            if now - self._logged_at < LOG_INTERVAL:
                return

            per_second = round(
                (handled - self._logged_handled) / (now - self._logged_at), 1
            )
            self._logged_at = now
            self._logged_handled = handled

        # on the counting thread, so the line carries this processor
        of_total = "" if total is None else f" of {total:,}"
        self._logger.info(
            f"{self._processor}: {handled:,}{of_total} handled (cached={cached_count:,} failed={failed_count:,}), {per_second}/s",
            extra={
                "handled": handled,
                "total": total,
                "cached": cached_count,
                "failed": failed_count,
                "per_second": per_second,
            },
        )


@contextmanager
def show_progress(
    processors: list[str],
    logger: Logger,
) -> Iterator[list[Progress | None]]:
    """One Progress per processor, in order; all None unless MEDALLION_SHOW_PROGRESS is true."""
    show = get_env_or_default(SHOW_PROGRESS_ENV_VAR, "false").lower()
    if show not in ("true", "false"):
        raise ValueError(f"{SHOW_PROGRESS_ENV_VAR} must be true or false, got {show}")

    if show == "false":
        yield [None] * len(processors)

        return

    if not sys.stdout.isatty():
        yield [Progress(p, logger, None) for p in processors]

        return

    # the console log handler writes to stdout, so the bars share it and log lines go above them
    bars = [
        tqdm(desc=p, file=sys.stdout, postfix="cached=0 failed=0", dynamic_ncols=True)
        for p in processors
    ]
    try:
        with logging_redirect_tqdm():
            yield [Progress(p, logger, bar) for p, bar in zip(processors, bars)]
    finally:
        for bar in bars:
            bar.close()
