from typing import TypedDict

import orjson
import requests

from infra import Env, get_logger

logger = get_logger("MARKET API")


class MarketMetadata(TypedDict):
    id: str
    title: str
    tokens: dict[str, str]
    fee_rate: float


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
