from typing import Literal, TypedDict


class MarketMetadata(TypedDict):
    id: str
    title: str
    tokens: dict[str, str]
    fee_rate: float


class OrderMetadata(TypedDict):
    id: str
    type: Literal["GTC", "FOK", "GTD", "FAK"] | str
    side: Literal["BUY", "SELL"] | str
    status: Literal["LIVE", "FILLED", "CANCELED", "EXPIRED", "MATCHED", "UNMATCHED"] | str
    market_id: str
    token_id: str
    shares: float
    matched: float
    price: float
