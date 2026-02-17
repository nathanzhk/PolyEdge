from dataclasses import dataclass
from typing import Literal

from schemas.http_responses import OrderMetadata

OrderSide = Literal["BUY", "SELL"]
OrderType = Literal["GTC", "GTD", "FOK", "FAK"]
OrderStatus = Literal["LIVE", "DELAYED", "MATCHED", "UNMATCHED"]

MATCHED_SHARES_GAP = 0.1


@dataclass(slots=True, frozen=True)
class Order:
    id: str
    side: OrderSide | str
    type: OrderType | str
    status: OrderStatus | str
    ordered_shares: float
    matched_shares: float
    market_id: str
    token_id: str
    price: float

    @classmethod
    def from_metadata(cls, metadata: OrderMetadata) -> "Order":
        return cls(
            id=metadata["id"],
            side=metadata["side"],
            type=metadata["type"],
            status=metadata["status"],
            ordered_shares=metadata["ordered_shares"],
            matched_shares=metadata["matched_shares"],
            market_id=metadata["market_id"],
            token_id=metadata["token_id"],
            price=metadata["price"],
        )

    @property
    def is_matched(self) -> bool:
        return self.ordered_shares - self.matched_shares < MATCHED_SHARES_GAP
