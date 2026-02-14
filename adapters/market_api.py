import json

import requests

from schemas.http_responses import MarketMetadata

_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


def get_market_by_slug(slug: str) -> MarketMetadata | None:
    try:
        resp = requests.get(f"{_GAMMA_BASE_URL}/markets/slug/{slug}", timeout=(1, 2))
        resp.raise_for_status()
        market = resp.json()
    except requests.RequestException:
        return None

    try:
        condition_id = market["conditionId"]
        title = market["question"]
        fee_rate = float(market["feeSchedule"]["rate"])
        outcomes = json.loads(market["outcomes"])
        token_ids = json.loads(market["clobTokenIds"])
        if len(outcomes) != len(token_ids):
            return None
        tokens = dict(zip(outcomes, token_ids))
    except (KeyError, TypeError, ValueError):
        return None

    return {
        "id": condition_id,
        "title": title,
        "tokens": tokens,
        "fee_rate": fee_rate,
    }
