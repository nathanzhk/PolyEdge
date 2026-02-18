from dataclasses import dataclass

from models.market import Token
from models.order import OrderSide


@dataclass(slots=True, frozen=True)
class TradeIntent:
    side: OrderSide
    token: Token
    shares: float
    price: float
