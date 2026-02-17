from typing import Literal, TypedDict


class MarketMetadata(TypedDict):
    id: str
    title: str
    tokens: dict[str, str]
    fee_rate: float


class OrderMetadata(TypedDict):
    id: str
    side: Literal["BUY", "SELL"] | str
    type: Literal["GTC", "GTD", "FOK", "FAK"] | str
    status: Literal["LIVE", "DELAYED", "MATCHED", "UNMATCHED"] | str
    ordered_shares: float
    matched_shares: float
    market_id: str
    token_id: str
    price: float
