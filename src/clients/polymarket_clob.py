import math
import time

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OpenOrderParams,
    OrderArgs,
    OrderType,
    TradeParams,
)
from py_clob_client.exceptions import PolyApiException
from py_clob_client.order_builder.constants import BUY, SELL

from enums import Role
from events import MarketOrderEvent, MarketTradeEvent
from markets.base import Market, Token
from streams import build_order_event, build_trade_event
from utils.env import Env
from utils.logger import get_logger


class TradeClient:
    role: Role
    post_only: bool
    order_type: OrderType

    def __init__(self) -> None:
        self.logger = get_logger(self.role)
        self.client = ClobClient(
            Env.POLYMARKET_CLOB_BASE_URL,
            key=Env.POLYMARKET_PRIVATE_KEY,
            funder=Env.POLYMARKET_PROXY_WALLET,
            chain_id=137,
            signature_type=2,
        )
        self.client.set_api_creds(self.get_credentials())
        self.client.get_ok()

    def get_credentials(self) -> ApiCreds:
        return self.client.create_or_derive_api_creds()

    def buy(self, token: Token, shares: float, price: float) -> str | None:
        return self._submit_order(token=token, shares=shares, price=price, side=BUY)

    def sell(self, token: Token, shares: float, price: float) -> str | None:
        return self._submit_order(token=token, shares=shares, price=price, side=SELL)

    def warm_up(self, market: Market):
        try:
            for token in {market.yes_token, market.no_token}:
                self.logger.debug("warm up %s", token.id)
                self.client.get_neg_risk(token.id)
                self.client.get_tick_size(token.id)
                self.client.get_fee_rate_bps(token.id)
            # self.client.get_market(market.id)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("warm up failed: %s", _error_message(e))

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
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,  # type: ignore
        )
        balance = self._get_balance(params)
        self.logger.debug("cash balance: %.6f", balance)
        return balance

    def get_token_shares(self, token: Token) -> float:
        params = BalanceAllowanceParams(
            token_id=token.id,
            asset_type=AssetType.CONDITIONAL,  # type: ignore
        )
        shares = self._get_balance(params)
        self.logger.debug("token shares: %.6f", shares)
        return shares

    def get_order_by_id(self, order_id: str) -> MarketOrderEvent | None:
        try:
            resp = self.client.get_order(order_id)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get order failed: %s", _error_message(e))
            return None

        if resp is None:
            self.logger.warning("order not found: %s", order_id)
            return None

        if not isinstance(resp, dict):
            self.logger.error("invalid response: %r", resp)
            return None

        return build_order_event(resp, source="pull")

    def get_orders_by_token(self, token: Token) -> list[MarketOrderEvent]:
        try:
            params = OpenOrderParams(asset_id=token.id)
            resp = self.client.get_orders(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get orders failed: %s", _error_message(e))
            return []

        if not isinstance(resp, list):
            self.logger.error("invalid response: %r", resp)
            return []

        orders: list[MarketOrderEvent] = []
        for item in resp:
            if not isinstance(item, dict):
                self.logger.error("invalid response: %r", item)
                continue
            order = build_order_event(item, source="pull")
            orders.append(order) if order is not None else ...
        return orders

    def get_trade_by_id(self, trade_id: str) -> MarketTradeEvent | None:
        try:
            params = TradeParams(id=trade_id)
            resp = self.client.get_trades(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get trade failed: %s", _error_message(e))
            return None

        if not isinstance(resp, list) or len(resp) > 1:
            self.logger.error("invalid response: %r", resp)
            return None

        if len(resp) == 0:
            self.logger.warning("trade not found: %s", trade_id)
            return None

        resp = resp[0]
        if not isinstance(resp, dict):
            self.logger.error("invalid response: %r", resp)
            return None

        return build_trade_event(resp, Env.POLYMARKET_PROXY_WALLET, source="pull")

    def get_trades_by_token(self, token: Token) -> list[MarketTradeEvent]:
        try:
            params = TradeParams(asset_id=token.id)
            resp = self.client.get_trades(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get trades failed: %s", _error_message(e))
            return []

        if not isinstance(resp, list):
            self.logger.error("invalid response: %r", resp)
            return []

        trades: list[MarketTradeEvent] = []
        for item in resp:
            if not isinstance(item, dict):
                self.logger.error("invalid response: %r", item)
                continue
            trade = build_trade_event(item, Env.POLYMARKET_PROXY_WALLET, source="pull")
            trades.append(trade) if trade is not None else ...
        return trades

    def cancel_order_by_id(self, order_id: str) -> tuple[bool, str]:
        try:
            # params = OrderPayload(orderID=order_id)
            resp = self.client.cancel(order_id)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            error_message = _error_message(e)
            self.logger.error("cancel order failed: %s", error_message)
            return False, error_message

        success_list = resp.get("canceled", []) if isinstance(resp, dict) else []
        if order_id in success_list:
            return True, ""

        failed_dict = resp.get("not_canceled", {}) if isinstance(resp, dict) else {}
        failed_reason = (
            failed_dict.get(order_id, "unknown reason")
            if isinstance(failed_dict, dict)
            else "unknown reason"
        )
        self.logger.error("cancel order failed: %s", failed_reason)
        return False, failed_reason

    def _get_balance(self, params: BalanceAllowanceParams) -> float:
        resp = self.client.get_balance_allowance(params)
        if not isinstance(resp, dict):
            return 0.0
        balance = resp.get("balance", "0")
        if not isinstance(balance, str | int):
            return 0.0
        return round(int(balance) / 1_000_000, 6)

    def _submit_order(self, *, token: Token, shares: float, price: float, side: str) -> str | None:
        self.logger.info("%s %s %.6f at $%.2f", side.lower(), token.key, shares, price)
        try:
            create_start_ns = time.perf_counter_ns()
            order = self.client.create_order(
                OrderArgs(token_id=token.id, size=shares, price=price, side=side)
            )
            create_latency_ms = (time.perf_counter_ns() - create_start_ns) / 1_000_000
            self.logger.info("create order latency %.3f ms", create_latency_ms)

            submit_start_ns = time.perf_counter_ns()
            try:
                resp = self.client.post_order(
                    order, post_only=self.post_only, orderType=self.order_type
                )
                self.logger.debug("%r", resp)
            finally:
                submit_latency_ms = (time.perf_counter_ns() - submit_start_ns) / 1_000_000
                self.logger.info("submit order latency %.3f ms", submit_latency_ms)

            if not isinstance(resp, dict):
                self.logger.error("invalid response: %r", resp)
                return None

            if resp.get("success") is not True:
                self.logger.error("%s", resp.get("errorMsg") or "unknown error")
                return None

            order_id = resp.get("orderID")
            if not isinstance(order_id, str) or not order_id:
                self.logger.error("missing order id: %r", resp)
                return None

            self.logger.info("order %s", order_id)
            return order_id

        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("submit order failed: %s", _error_message(e))
            return None


class MakerTradeClient(TradeClient):
    role: Role = Role.MAKER
    post_only: bool = True
    order_type: OrderType = OrderType.GTC  # type: ignore


class TakerTradeClient(TradeClient):
    role: Role = Role.TAKER
    post_only: bool = False
    order_type: OrderType = OrderType.FOK  # type: ignore


def _truncate_decimal(x, digits):
    factor = 10**digits
    return math.trunc(x * factor) / factor


def _error_message(error: PolyApiException) -> str:
    error_msg = error.error_msg
    if isinstance(error_msg, dict):
        error_msg = error_msg.get("error", error_msg)
    return str(error_msg)
