import os
import time
from collections.abc import Mapping
from typing import Any, Literal

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    OpenOrderParams,
    OrderArgs,
)
from py_clob_client.exceptions import PolyApiException
from py_clob_client.order_builder.constants import BUY, SELL

from common.logger import get_logger
from domain.market import Market, Token
from domain.order import Order
from schemas.http_responses import OrderMetadata

TradeRole = Literal["maker", "taker"]


class TradeClient:
    role: TradeRole
    post_only: bool

    def __init__(self) -> None:
        self.logger = get_logger(self.role.upper())
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
        if not private_key:
            raise ValueError("missing POLYMARKET_PRIVATE_KEY")
        proxy_wallet = os.getenv("POLYMARKET_PROXY_WALLET")
        if not proxy_wallet:
            raise ValueError("missing POLYMARKET_PROXY_WALLET")
        self.client = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            funder=proxy_wallet,
            chain_id=137,
            signature_type=2,
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

    def buy(self, token: Token, shares: float, price: float) -> str | None:
        return self._submit_order(token=token, shares=shares, price=price, side=BUY)

    def sell(self, token: Token, shares: float, price: float) -> str | None:
        return self._submit_order(token=token, shares=shares, price=price, side=SELL)

    def warm_up(self, token: Token) -> bool:
        self.logger.debug("warm up %s", token.id)
        try:
            self.client.get_neg_risk(token.id)
            self.client.get_tick_size(token.id)
            self.client.get_fee_rate_bps(token.id)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("warm up failed: %s", self._error_message(e))
            return False
        return True

    def fee_rate(self, market: Market) -> float:
        if self.role == "maker":
            return 0.0
        if self.role == "taker":
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
        self.logger.debug("token (%s) balance: %.6f", token.outcome.lower(), shares)
        return shares

    def get_order_by_id(self, order_id: str) -> Order | None:
        try:
            resp = self.client.get_order(order_id)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get order failed: %s", self._error_message(e))
            return None

        if not isinstance(resp, dict):
            self.logger.error("invalid response: %r", resp)
            return None

        try:
            return Order.from_metadata(self._order_metadata(resp))
        except (KeyError, TypeError, ValueError) as e:
            self.logger.error("invalid response: %s", e)
            return None

    def get_orders_by_token(self, token: Token) -> list[Order]:
        try:
            params = OpenOrderParams(asset_id=token.id)
            resp = self.client.get_orders(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("get orders failed: %s", self._error_message(e))
            return []

        if not isinstance(resp, list):
            self.logger.error("invalid response: %r", resp)
            return []

        orders: list[Order] = []
        for item in resp:
            if not isinstance(item, dict):
                self.logger.error("invalid response: %r", item)
                continue
            try:
                orders.append(Order.from_metadata(self._order_metadata(item)))
            except (KeyError, TypeError, ValueError) as e:
                self.logger.error("invalid response: %s", e)
        return orders

    def cancel_order(self, order_id: str) -> bool:
        try:
            resp = self.client.cancel(order_id)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("cancel order failed: %s", self._error_message(e))
            return False

        success_list = resp.get("canceled", []) if isinstance(resp, dict) else []
        if order_id in success_list:
            return True

        failed_dict = resp.get("not_canceled", {}) if isinstance(resp, dict) else {}
        failed_reason = (
            failed_dict.get(order_id, "unknown reason")
            if isinstance(failed_dict, dict)
            else "unknown reason"
        )
        self.logger.error("cancel failed: %s", failed_reason)
        return False

    def _get_balance(self, params: BalanceAllowanceParams) -> float:
        resp = self.client.get_balance_allowance(params)
        if not isinstance(resp, dict):
            return 0.0
        balance = resp.get("balance", "0")
        if not isinstance(balance, str | int):
            return 0.0
        return round(int(balance) / 1_000_000, 6)

    def _submit_order(self, *, token: Token, shares: float, price: float, side: str) -> str | None:
        self.logger.info("%s %s %.6f at $%.2f", side.lower(), token.outcome.lower(), shares, price)
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
            self.logger.error("submit order failed: %s", self._error_message(e))
            return None

    @staticmethod
    def _order_metadata(data: Mapping[str, Any]) -> OrderMetadata:
        return {
            "id": data["id"],
            "type": data["order_type"],
            "side": data["side"],
            "status": data["status"],
            "market_id": data["market"],
            "token_id": data["asset_id"],
            "shares": float(data["original_size"]),
            "matched": float(data["size_matched"]),
            "price": float(data["price"]),
        }

    @staticmethod
    def _error_message(error: PolyApiException) -> str:
        error_msg = error.error_msg
        if isinstance(error_msg, dict):
            error_msg = error_msg.get("error", error_msg)
        return str(error_msg)


class MakerTradeClient(TradeClient):
    role: TradeRole = "maker"
    post_only = True


class TakerTradeClient(TradeClient):
    role: TradeRole = "taker"
    post_only = False
