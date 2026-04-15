from __future__ import annotations

import math

from enums import Role, Side
from events import MarketOrderEvent, MarketTradeEvent
from markets.base import Market, Token
from paper.simulator import PaperExchangeSimulator
from utils.logger import get_logger


class PaperTradeClient:
    role: Role

    def __init__(self, simulator: PaperExchangeSimulator) -> None:
        self._simulator = simulator
        self.logger = get_logger(f"PAPER-{self.role}")

    def warm_up(self, market: Market) -> None:
        self.logger.debug("warm up %s", market.slug)

    def buy(self, token: Token, shares: float, price: float) -> str | None:
        return self._simulator.submit_order(
            token=token,
            side=Side.BUY,
            role=self.role,
            shares=shares,
            price=price,
        )

    def sell(self, token: Token, shares: float, price: float) -> str | None:
        return self._simulator.submit_order(
            token=token,
            side=Side.SELL,
            role=self.role,
            shares=shares,
            price=price,
        )

    def fee_rate(self, market: Market) -> float:
        if self.role == Role.MAKER:
            return 0.0
        if self.role == Role.TAKER:
            return market.fee_rate
        raise ValueError(f"invalid role: {self.role}")

    def calc_fee(self, market: Market, shares: float, price: float) -> float:
        return round(self.fee_rate(market) * shares * price * (1 - price), 6)

    def calc_net_buy_shares(
        self, market: Market, shares: float, price: float
    ) -> tuple[float, float]:
        fee_shares = _truncate_decimal(shares * self.fee_rate(market) * (1 - price), 5)
        return round(shares - fee_shares, 6), fee_shares

    def calc_net_sell_amount(
        self, market: Market, shares: float, price: float
    ) -> tuple[float, float]:
        fee_amount = _truncate_decimal(shares * price * self.fee_rate(market) * (1 - price), 5)
        return round(shares * price - fee_amount, 6), fee_amount

    def get_cash_balance(self) -> float:
        return 100_000.0

    def get_token_shares(self, token_id: str) -> float:
        return 0.0

    def get_order_by_id(self, order_id: str) -> MarketOrderEvent | None:
        return self._simulator.get_order_by_id(order_id)

    def get_trade_by_id(self, trade_id: str) -> MarketTradeEvent | None:
        return self._simulator.get_trade_by_id(trade_id)

    def cancel_order_by_id(self, order_id: str) -> tuple[bool, str]:
        return self._simulator.cancel_order(order_id)


class PaperMakerTradeClient(PaperTradeClient):
    role: Role = Role.MAKER


class PaperTakerTradeClient(PaperTradeClient):
    role: Role = Role.TAKER


def _truncate_decimal(x: float, digits: int) -> float:
    factor = 10**digits
    return math.trunc(x * factor) / factor
