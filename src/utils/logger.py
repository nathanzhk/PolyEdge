import atexit
import logging
import queue
import re
import sys
from datetime import datetime
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from threading import RLock

from utils.env import Env
from utils.time import fmt_ts_s

_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
_MARKET_LOGGER_NAMES = frozenset(
    {
        "STATE",
        "MARKET QUOTE",
        "MARKET TRADE",
        "CRYPTO QUOTE",
        "CRYPTO OHLCV",
    }
)
_TRADE_LOGGER_NAMES = frozenset(
    {
        "TRADE",
        "MANAGER",
        "MAKER",
        "TAKER",
        "PAPER",
        "PAPER-MAKER",
        "PAPER-TAKER",
    }
)

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
_FILE_HANDLER: "_LogFileHandler | None" = None


class _LogQueueHandler(QueueHandler):
    def __init__(self, log_queue: queue.Queue[logging.LogRecord], file_handler: "_LogFileHandler"):
        super().__init__(log_queue)
        self._file_handler = file_handler

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        record = super().prepare(record)
        record.base_log_file = self._file_handler.log_file
        return record


class _LogFileHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self._lock = RLock()
        self._handlers: dict[Path, logging.FileHandler] = {}
        self._target_log_file: Path | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            base_log_file = getattr(record, "base_log_file", None)
            if base_log_file is None:
                return
            log_file = _route_log_file(base_log_file, record.name)
            with self._lock:
                handler = self._handlers.get(log_file)
                if handler is None:
                    handler = self._open_log_file(log_file)
                handler.emit(record)
        except Exception:
            self.handleError(record)

    @property
    def log_file(self) -> Path | None:
        with self._lock:
            return self._target_log_file

    def set_log_file(self, log_file: Path) -> None:
        with self._lock:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            if log_file != self._target_log_file:
                self._close_handlers()
            self._target_log_file = log_file
            self._open_log_file(log_file)

    def _open_log_file(self, log_file: Path) -> logging.FileHandler:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        existing_handler = self._handlers.get(log_file)
        if existing_handler is not None:
            return existing_handler

        next_handler = logging.FileHandler(log_file)
        next_handler.setFormatter(_Formatter(_LOG_FORMAT))
        next_handler.setLevel(logging.DEBUG)
        self._handlers[log_file] = next_handler
        return next_handler

    def close(self) -> None:
        with self._lock:
            handlers = list(self._handlers.values())
            self._handlers.clear()
            self._target_log_file = None
        for handler in handlers:
            handler.close()
        super().close()

    def _close_handlers(self) -> None:
        handlers = list(self._handlers.values())
        self._handlers.clear()
        for handler in handlers:
            handler.close()


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
    global _CONFIGURED, _QUEUE_HANDLER, _QUEUE_LISTENER, _CONSOLE_HANDLER, _FILE_HANDLER
    if _CONFIGURED:
        return

    _FILE_HANDLER = _LogFileHandler()

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue()
    _QUEUE_HANDLER = _LogQueueHandler(log_queue, _FILE_HANDLER)
    _QUEUE_HANDLER.setLevel(logging.DEBUG)
    _QUEUE_LISTENER = QueueListener(log_queue, _FILE_HANDLER, respect_handler_level=True)
    _QUEUE_LISTENER.start()
    atexit.register(_stop_queue_listener)

    _CONSOLE_HANDLER = logging.StreamHandler(sys.stderr)
    _CONSOLE_HANDLER.setFormatter(_ColorFormatter(_LOG_FORMAT))
    _CONSOLE_HANDLER.setLevel(logging.getLevelNamesMapping().get(Env.LOG_LEVEL, logging.INFO))

    _CONFIGURED = True
    for logger in _LOGGERS.values():
        _configure_logger(logger)


def set_log_file(name: str) -> Path | None:
    if _FILE_HANDLER is None:
        return None
    log_file = _build_log_file(name, datetime.now())
    _FILE_HANDLER.set_log_file(log_file)
    return log_file


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
    global _QUEUE_LISTENER, _FILE_HANDLER
    if _QUEUE_LISTENER is None:
        return
    _QUEUE_LISTENER.stop()
    _QUEUE_LISTENER = None
    if _FILE_HANDLER is not None:
        _FILE_HANDLER.close()
        _FILE_HANDLER = None


def _build_log_file(name: str, ts: datetime) -> Path:
    safe_name = _sanitize_log_name(name)
    return _LOG_DIR / ts.strftime("%Y%m%d") / f"{ts.strftime('%Y%m%d_%H%M%S')}-{safe_name}.log"


def _route_log_file(base_log_file: Path, logger_name: str) -> Path:
    if logger_name in _MARKET_LOGGER_NAMES:
        return base_log_file.with_suffix(".market.log")
    if logger_name in _TRADE_LOGGER_NAMES:
        return base_log_file.with_suffix(".trade.log")
    return base_log_file


def _sanitize_log_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-") or "market"
