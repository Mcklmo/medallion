import json
import logging
import sys
import threading
import time
from pathlib import Path

import pendulum

from medallion.model.extractor import generate_utc_timestamp_foldername
from medallion.store.base import build_timestamp_path_segments, get_env_or_default

LOG_FORMAT_ENV_VAR = "MEDALLION_LOG_FORMAT"
WRITE_LOG_FILES_ENV_VAR = "MEDALLION_WRITE_LOG_FILES"
LOG_DIR_ENV_VAR = "MEDALLION_LOG_DIR"
PROCESSOR_NAME_ENV_VAR = "PROCESSOR_NAME"
MAX_FILE_NAME_BYTES = 255

_thread = threading.local()
_process_processor: str | None = None


def set_thread_processor(processor: str | None) -> None:
    """Names the processor that lines logged on this thread come from."""
    _thread.processor = processor


def current_processor() -> str | None:
    return getattr(_thread, "processor", None) or _process_processor


class ColoredFormatter(logging.Formatter):
    # Color codes
    CYAN = "\033[36m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    GREEN = "\033[32m"
    RESET = "\033[0m"

    LEVEL_COLORS = {
        logging.DEBUG: CYAN,
        logging.INFO: GREEN,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: RED,
    }

    def format(self, record):
        timestamp = self.formatTime(record, self.datefmt)
        colored_timestamp = f"{self.CYAN}{timestamp}{self.RESET}"

        level_color = self.LEVEL_COLORS.get(record.levelno, self.RESET)
        colored_level = f"{level_color}{record.levelname}{self.RESET}"

        message = f"{colored_timestamp} - {colored_level} - {record.getMessage()}"

        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"

        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"

        return message

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        s = time.strftime(datefmt or self.default_time_format, ct)
        return f"{s}.{int(record.msecs):03d}"


_RESERVED_KEYS = {
    "time",
    "severity",
    "message",
    "processor",
    "logger",
    "module",
    "function",
    "line",
}
_RECORD_ATTRIBUTES = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record):
        message = record.getMessage()

        # Cloud Error Reporting reads the stack trace from the message
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"

        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"

        time_utc = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
        entry = {
            "time": f"{time_utc}.{int(record.msecs):03d}Z",
            "severity": record.levelname,
            "message": message,
        }

        processor = current_processor()
        if processor is not None:
            entry["processor"] = processor

        entry.update(
            logger=record.name,
            module=record.module,
            function=record.funcName,
            line=record.lineno,
        )

        for key, value in vars(record).items():
            if key not in _RECORD_ATTRIBUTES:
                entry[f"extra_{key}" if key in _RESERVED_KEYS else key] = value

        return json.dumps(entry, ensure_ascii=False, default=str)


def _console_formatter() -> logging.Formatter:
    log_format = get_env_or_default(LOG_FORMAT_ENV_VAR, "json").lower()

    if log_format == "json":
        return JsonFormatter()

    if log_format == "text":
        return ColoredFormatter("", datefmt="%Y-%m-%d %H:%M:%S")

    raise ValueError(f"{LOG_FORMAT_ENV_VAR} must be json or text, got {log_format}")


def log_file_name(start: str, processors: list[str]) -> str:
    for kept in range(len(processors), 0, -1):
        dropped = len(processors) - kept
        more = f"-and-{dropped}-more" if dropped else ""
        name = "-".join([start, *processors[:kept]]) + more + ".jsonl"

        if len(name.encode()) <= MAX_FILE_NAME_BYTES:
            return name

    raise ValueError(
        f"Log file name for processors {processors} exceeds {MAX_FILE_NAME_BYTES} bytes"
    )


def start_process_log(processors: list[str]) -> logging.FileHandler | None:
    """Names the processors this process runs, in run order, and opens its log file
    unless MEDALLION_WRITE_LOG_FILES is false."""
    global _process_processor
    _process_processor = processors[0] if len(processors) == 1 else None

    write_files = get_env_or_default(WRITE_LOG_FILES_ENV_VAR, "true").lower()
    if write_files not in ("true", "false"):
        raise ValueError(
            f"{WRITE_LOG_FILES_ENV_VAR} must be true or false, got {write_files}"
        )

    if write_files == "false":
        return None

    start = generate_utc_timestamp_foldername(pendulum.now("UTC"))
    folder = Path(
        get_env_or_default(LOG_DIR_ENV_VAR, "output/logs"),
        *build_timestamp_path_segments(start),
    )
    folder.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(
        folder / log_file_name(start, processors),
        encoding="utf-8",
    )
    handler.setFormatter(JsonFormatter())
    logging.getLogger().addHandler(handler)

    return handler


_logger: logging.Logger | None = None


def create_logger(log_output_directory_path: str | None = None) -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # on the root logger, so every propagating logger's lines get the same shape
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_console_formatter())
    logging.getLogger().addHandler(handler)

    if log_output_directory_path is not None:
        log_file_path = f"{log_output_directory_path}/log.txt"
        file_handler = logging.FileHandler(log_file_path)

        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(file_handler)
        logger.info(f'Logs are also written to "{log_file_path}"')

    _logger = logger

    return logger
