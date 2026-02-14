from typing import TypedDict


class MarketMetadata(TypedDict):
    id: str
    title: str
    tokens: dict[str, str]
    fee_rate: float
