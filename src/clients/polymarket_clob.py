import time
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OpenOrderParams,
    OrderArgs,
)
from py_clob_client.exceptions import PolyApiException
from py_clob_client.order_builder.constants import BUY, SELL

from domain import MarketOrderStatus, OrderType, Side
from execution import MarketOrder
from infra import Env
from infra.logger import get_logger
from markets import Market, Token


class _Role(StrEnum):
    MAKER = "maker"
    TAKER = "taker"


class TradeClient:
    role: _Role
    post_only: bool

    def __init__(self) -> None:
        self.logger = get_logger(self.role.upper())
        self.client = ClobClient(
            Env.POLYMARKET_CLOB_BASE_URL,
            key=Env.POLYMARKET_PRIVATE_KEY,
            funder=Env.POLYMARKET_PROXY_WALLET,
            chain_id=137,
            signature_type=2,
        )
        self.client.set_api_creds(self.get_credentials())

    def get_credentials(self) -> ApiCreds:
        return self.client.create_or_derive_api_creds()

    def buy(self, token: Token, shares: float, price: float) -> str | None:
        return self._submit_order(token=token, shares=shares, price=price, side=BUY)

    def sell(self, token: Token, shares: float, price: float) -> str | None:
        return self._submit_order(token=token, shares=shares, price=price, side=SELL)

    def warm_up(self, market: Market):
        self._warm_up_token(market.yes_token)
        self._warm_up_token(market.no_token)

    def fee_rate(self, market: Market) -> float:
        if self.role == _Role.MAKER:
            return 0.0
        if self.role == _Role.TAKER:
            return market.fee_rate
        raise ValueError(f"invalid role: {self.role}")

    def calc_fee(self, market: Market, shares: float, price: float) -> float:
        return round(self.fee_rate(market) * shares * price * (1 - price), 6)

    def calc_net_buy_shares(self, market: Market, shares: float, price: float) -> float:
        return round(shares * (1 - self.fee_rate(market) * (1 - price)), 6)

    def calc_net_sell_value(self, market: Market, shares: float, price: float) -> float:
        return round(shares * price * (1 - self.fee_rate(market) * (1 - price)), 6)

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

    def get_order_by_id(self, order_id: str) -> MarketOrder | None:
        try:
            resp = self.client.get_order(order_id)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get order failed: %s", _error_message(e))
            return None

        if not isinstance(resp, dict):
            self.logger.error("invalid response: %r", resp)
            return None

        try:
            return _parse_market_order(resp)
        except (KeyError, TypeError, ValueError) as e:
            self.logger.error("invalid response: %s", e)
            return None

    def get_orders_by_token(self, token: Token) -> list[MarketOrder]:
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

        orders: list[MarketOrder] = []
        for item in resp:
            if not isinstance(item, dict):
                self.logger.error("invalid response: %r", item)
                continue
            try:
                orders.append(_parse_market_order(item))
            except (KeyError, TypeError, ValueError) as e:
                self.logger.error("invalid response: %s", e)
        return orders

    def cancel_order_by_id(self, order_id: str) -> tuple[bool, str]:
        try:
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

    def _warm_up_token(self, token: Token):
        self.logger.debug("warm up %s", token.id)
        try:
            self.client.get_neg_risk(token.id)
            self.client.get_tick_size(token.id)
            self.client.get_fee_rate_bps(token.id)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("warm up failed: %s", _error_message(e))

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
                resp = self.client.post_order(order, post_only=self.post_only)
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
    role = _Role.MAKER
    post_only = True


class TakerTradeClient(TradeClient):
    role = _Role.TAKER
    post_only = False


def _parse_market_order(data: Mapping[str, Any]) -> MarketOrder:
    return MarketOrder(
        id=data["id"],
        side=Side(data["side"]),
        type=OrderType(data["order_type"]),
        status=MarketOrderStatus(data["status"]),
        ordered_shares=round(float(data["original_size"]), 6),
        matched_shares=round(float(data["size_matched"]), 6),
        market_id=data["market"],
        token_id=data["asset_id"],
        price=round(float(data["price"]), 3),
    )


def _error_message(error: PolyApiException) -> str:
    error_msg = error.error_msg
    if isinstance(error_msg, dict):
        error_msg = error_msg.get("error", error_msg)
    return str(error_msg)
