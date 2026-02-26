from __future__ import annotations

import os

from dotenv import load_dotenv


class Env:
    LOG_LEVEL: str
    ENABLE_STATS: bool

    TELEGRAM_BOT_KEY: str
    TELEGRAM_CHAT_ID: str
    TELEGRAM_API_BASE_URL: str

    POLY_BACKTEST_KEY: str
    POLY_BACKTEST_BASE_URL: str

    BINANCE_WS_BASE_URL: str
    BINANCE_API_BASE_URL: str

    POLYMARKET_WS_BASE_URL: str
    POLYMARKET_CLOB_BASE_URL: str
    POLYMARKET_GAMMA_BASE_URL: str

    POLYMARKET_PRIVATE_KEY: str
    POLYMARKET_PROXY_WALLET: str

    @classmethod
    def load(cls) -> None:
        load_dotenv()

        cls.LOG_LEVEL = _env_str("LOG_LEVEL").upper()
        cls.ENABLE_STATS = _env_bool("ENABLE_STATS")

        cls.TELEGRAM_BOT_KEY = _env_str("TELEGRAM_BOT_KEY")
        cls.TELEGRAM_CHAT_ID = _env_str("TELEGRAM_CHAT_ID")
        cls.TELEGRAM_API_BASE_URL = _env_str("TELEGRAM_API_BASE_URL")

        cls.POLY_BACKTEST_KEY = _env_str("POLY_BACKTEST_KEY")
        cls.POLY_BACKTEST_BASE_URL = _env_str("POLY_BACKTEST_BASE_URL")

        cls.BINANCE_WS_BASE_URL = _env_str("BINANCE_WS_BASE_URL")
        cls.BINANCE_API_BASE_URL = _env_str("BINANCE_API_BASE_URL")

        cls.POLYMARKET_WS_BASE_URL = _env_str("POLYMARKET_WS_BASE_URL")
        cls.POLYMARKET_CLOB_BASE_URL = _env_str("POLYMARKET_CLOB_BASE_URL")
        cls.POLYMARKET_GAMMA_BASE_URL = _env_str("POLYMARKET_GAMMA_BASE_URL")

        cls.POLYMARKET_PRIVATE_KEY = _env_str("POLYMARKET_PRIVATE_KEY")
        cls.POLYMARKET_PROXY_WALLET = _env_str("POLYMARKET_PROXY_WALLET")


def _env_str(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"missing required env: {name}")
    return value.strip()


def _env_bool(name: str) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"missing required env: {name}")
    return value.strip().lower() in {"true", "yes", "on", "1"}
