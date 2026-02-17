from dataclasses import dataclass
from typing import Literal

from schemas.http_responses import OrderMetadata

OrderSide = Literal["BUY", "SELL"]
OrderType = Literal["GTC", "FOK", "GTD", "FAK"]
OrderStatus = Literal["LIVE", "FILLED", "CANCELED", "EXPIRED", "MATCHED", "UNMATCHED"]


@dataclass(slots=True, frozen=True)
class Order:
    id: str
    type: OrderType | str
    side: OrderSide | str
    status: OrderStatus | str
    market_id: str
    token_id: str
    shares: float
    matched: float
    price: float

    @classmethod
    def from_metadata(cls, metadata: OrderMetadata) -> "Order":
        return cls(
            id=metadata["id"],
            type=metadata["type"],
            side=metadata["side"],
            status=metadata["status"],
            market_id=metadata["market_id"],
            token_id=metadata["token_id"],
            shares=metadata["shares"],
            matched=metadata["matched"],
            price=metadata["price"],
        )

    @property
    def remaining_size(self) -> float:
        return round(self.shares - self.matched, 6)

    @property
    def is_live(self) -> bool:
        return self.status == "LIVE"
