import logging
import os
import sys
from datetime import datetime
from pathlib import Path

_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
_DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

_COLOR_RESET = "\033[0m"
_LEVEL_COLORS = {
    logging.DEBUG: "\033[36m",  # cyan
    logging.INFO: "\033[32m",  # green
    logging.WARNING: "\033[33m",  # yellow
    logging.ERROR: "\033[31m",  # red
    logging.CRITICAL: "\033[1;31m",  # bold red
}

_CONFIGURED = False

_LOGGERS: dict[str, logging.Logger] = {}
_NULL_HANDLER = logging.NullHandler()
_FILE_HANDLER: logging.Handler | None = None
_CONSOLE_HANDLER: logging.Handler | None = None


class _Formatter(logging.Formatter):
    def formatTime(self, record, datefmt: str | None = None):
        record_time = datetime.fromtimestamp(record.created)
        return record_time.strftime(_DATE_TIME_FORMAT)[:-3]


class _ColorFormatter(_Formatter):
    def format(self, record):
        level = record.levelname
        color = _LEVEL_COLORS.get(record.levelno, _COLOR_RESET)
        record.levelname = f"{color}{level}{_COLOR_RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = level


def configure_logging() -> None:
    global _CONFIGURED, _FILE_HANDLER, _CONSOLE_HANDLER
    if _CONFIGURED:
        return

    now = datetime.now()
    log_dir = _LOG_DIR / now.strftime("%Y%m%d")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{now.strftime('%Y%m%d_%H%M%S')}.log"

    _FILE_HANDLER = logging.FileHandler(log_file)
    _FILE_HANDLER.setFormatter(_Formatter(_LOG_FORMAT))
    _FILE_HANDLER.setLevel(logging.DEBUG)

    _CONSOLE_HANDLER = logging.StreamHandler(sys.stderr)
    _CONSOLE_HANDLER.setFormatter(_ColorFormatter(_LOG_FORMAT))
    _CONSOLE_HANDLER.setLevel(
        logging.getLevelNamesMapping().get(os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    )

    _CONFIGURED = True
    for logger in _LOGGERS.values():
        _configure_logger(logger)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    _LOGGERS[name] = logger
    if _CONFIGURED:
        _configure_logger(logger)
    elif not logger.handlers:
        logger.addHandler(_NULL_HANDLER)
    return logger


def _configure_logger(logger: logging.Logger) -> None:
    if _FILE_HANDLER is None or _CONSOLE_HANDLER is None:
        return
    if _NULL_HANDLER in logger.handlers:
        logger.removeHandler(_NULL_HANDLER)
    if _FILE_HANDLER not in logger.handlers:
        logger.addHandler(_FILE_HANDLER)
    if _CONSOLE_HANDLER not in logger.handlers:
        logger.addHandler(_CONSOLE_HANDLER)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
