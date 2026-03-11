import atexit
import logging
import queue
import sys
from datetime import datetime
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path

from infra import Env, fmt_ts_s

_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"

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
_QUEUE_HANDLER: QueueHandler | None = None
_QUEUE_LISTENER: QueueListener | None = None
_CONSOLE_HANDLER: logging.Handler | None = None


class _Formatter(logging.Formatter):
    def formatTime(self, record, datefmt: str | None = None):
        return fmt_ts_s(record.created, fmt="datetime_ms")


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
    global _CONFIGURED, _QUEUE_HANDLER, _QUEUE_LISTENER, _CONSOLE_HANDLER
    if _CONFIGURED:
        return

    now = datetime.now()
    log_dir = _LOG_DIR / now.strftime("%Y%m%d")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{now.strftime('%Y%m%d_%H%M%S')}.log"

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(_Formatter(_LOG_FORMAT))
    file_handler.setLevel(logging.DEBUG)

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue()
    _QUEUE_HANDLER = QueueHandler(log_queue)
    _QUEUE_HANDLER.setLevel(logging.DEBUG)
    _QUEUE_LISTENER = QueueListener(log_queue, file_handler, respect_handler_level=True)
    _QUEUE_LISTENER.start()
    atexit.register(_stop_queue_listener)

    _CONSOLE_HANDLER = logging.StreamHandler(sys.stderr)
    _CONSOLE_HANDLER.setFormatter(_ColorFormatter(_LOG_FORMAT))
    _CONSOLE_HANDLER.setLevel(logging.getLevelNamesMapping().get(Env.LOG_LEVEL, logging.INFO))

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
    if _QUEUE_HANDLER is None or _CONSOLE_HANDLER is None:
        return
    if _NULL_HANDLER in logger.handlers:
        logger.removeHandler(_NULL_HANDLER)
    if _QUEUE_HANDLER not in logger.handlers:
        logger.addHandler(_QUEUE_HANDLER)
    if _CONSOLE_HANDLER not in logger.handlers:
        logger.addHandler(_CONSOLE_HANDLER)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False


def _stop_queue_listener() -> None:
    global _QUEUE_LISTENER
    if _QUEUE_LISTENER is None:
        return
    _QUEUE_LISTENER.stop()
    _QUEUE_LISTENER = None
