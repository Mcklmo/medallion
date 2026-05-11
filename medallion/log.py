import logging
import time


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

        return f"{colored_timestamp} - {colored_level} - {record.getMessage()}"

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        s = time.strftime(datefmt or self.default_time_format, ct)
        return f"{s}.{int(record.msecs):03d}"


_logger: logging.Logger | None = None


def create_logger(log_output_directory_path: str | None = None) -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger

    logger = logging.getLogger(__name__)
    logger.propagate = False

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    datefmt = "%Y-%m-%d %H:%M:%S"
    colored_formatter = ColoredFormatter("", datefmt=datefmt)

    handler.setFormatter(colored_formatter)
    logger.addHandler(handler)

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
