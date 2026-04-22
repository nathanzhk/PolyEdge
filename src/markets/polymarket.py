from typing import TypedDict

import orjson
import requests

from utils.env import Env
from utils.logger import get_logger

logger = get_logger("MARKET API")


class MarketMetadata(TypedDict):
    id: str
    title: str
    tokens: dict[str, str]
    fee_rate: float


class CryptoPriceResult(TypedDict):
    open_price: float
    close_price: float
    timestamp_ms: int
    completed: bool
    outcome: str


def get_market_by_slug(slug: str) -> MarketMetadata | None:
    try:
        resp = requests.get(
            f"{Env.POLYMARKET_GAMMA_BASE_URL}/markets/slug/{slug}",
            timeout=(1, 2),
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("get market failed: %s", e)
        return None

    try:
        market = orjson.loads(resp.content)
        condition_id = market["conditionId"]
        title = market["question"]
        fee_rate = float(market["feeSchedule"]["rate"])
        outcomes = orjson.loads(market["outcomes"])
        token_ids = orjson.loads(market["clobTokenIds"])
        if len(outcomes) != len(token_ids):
            return None
        tokens = dict(zip(outcomes, token_ids))
    except (KeyError, TypeError, ValueError) as e:
        logger.error("invalid response: %s", e)
        return None

    return {
        "id": condition_id,
        "title": title,
        "tokens": tokens,
        "fee_rate": fee_rate,
    }


def get_crypto_price_result(
    *,
    symbol: str,
    event_start_ts_s: int,
    variant: str,
) -> CryptoPriceResult | None:
    try:
        resp = requests.get(
            "https://polymarket.com/api/crypto/crypto-price",
            params={
                "symbol": symbol,
                "eventStartTime": event_start_ts_s,
                "variant": variant,
            },
            timeout=(1, 3),
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("get crypto price failed: %s", e)
        return None

    try:
        payload = orjson.loads(resp.content)
        completed = bool(payload["completed"])
        open_price = float(payload["openPrice"])
        close_price = payload["closePrice"]
        timestamp_ms = int(payload["timestamp"])
    except (KeyError, TypeError, ValueError) as e:
        logger.error("invalid crypto price response: %s", e)
        return None

    if not completed or close_price is None:
        return None

    close_price = float(close_price)
    return {
        "open_price": open_price,
        "close_price": close_price,
        "timestamp_ms": timestamp_ms,
        "completed": completed,
        "outcome": "UP" if close_price > open_price else "DOWN",
    }
