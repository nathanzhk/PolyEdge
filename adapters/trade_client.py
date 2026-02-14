import os
import time
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

    def fee_rate(self, market: Market) -> float:
        if self.role == "maker":
            return 0.0
        if self.role == "taker":
            return market.fee_rate
        raise ValueError(f"invalid role: {self.role}")

    def calc_fee(self, market: Market, shares: float, price: float) -> float:
        return round(self.fee_rate(market) * shares * price * (1 - price), 6)

    def calc_bought_shares(self, market: Market, shares: float, price: float) -> float:
        return round(shares * (1 - self.fee_rate(market) * (1 - price)), 6)

    def calc_sold_amount(self, market: Market, shares: float, price: float) -> float:
        return round(shares * price * (1 - self.fee_rate(market) * (1 - price)), 6)

    def buy(self, token: Token, shares: float, price: float) -> tuple[bool, str | None]:
        return self._submit_order(token=token, shares=shares, price=price, side=BUY)

    def sell(self, token: Token, shares: float, price: float) -> tuple[bool, str | None]:
        return self._submit_order(token=token, shares=shares, price=price, side=SELL)

    def warm_up(self, token: Token) -> bool:
        self.logger.debug("warm up %s", token.id)
        try:
            self.client.get_neg_risk(token.id)
            self.client.get_tick_size(token.id)
            self.client.get_fee_rate_bps(token.id)
        except PolyApiException as e:
            self.logger.error("warm up failed: %s", self._error_message(e))
            return False
        self.logger.debug("warm up success")
        return True

    def get_cash_balance(self) -> float:
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,  # type: ignore
        )
        balance = self._get_balance(params)
        self.logger.debug("cash balance: %.6f", balance)
        return balance

    def get_token_balance(self, token: Token) -> float:
        params = BalanceAllowanceParams(
            token_id=token.id,
            asset_type=AssetType.CONDITIONAL,  # type: ignore
        )
        shares = self._get_balance(params)
        self.logger.debug("token (%s) balance: %.6f", token.outcome.lower(), shares)
        return shares

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        try:
            return self.client.get_order(order_id)
        except PolyApiException as e:
            self.logger.error("get_order failed: %s", self._error_message(e))
            return None

    def get_orders(self, token: Token | None = None) -> Any:
        token_id = token.id if token else None
        params = OpenOrderParams(asset_id=token_id) if token_id else OpenOrderParams()
        return self.client.get_orders(params)

    def cancel_order(self, order_id: str) -> bool:
        try:
            response = self.client.cancel(order_id)
        except PolyApiException as e:
            self.logger.error("cancel failed: %s", self._error_message(e))
            return False

        success_list = response.get("canceled", []) if isinstance(response, dict) else []
        if order_id in success_list:
            return True

        failed_dict = response.get("not_canceled", {}) if isinstance(response, dict) else {}
        failed_reason = (
            failed_dict.get(order_id, "unknown reason")
            if isinstance(failed_dict, dict)
            else "unknown reason"
        )
        self.logger.error("cancel failed: %s", failed_reason)
        return False

    def _submit_order(
        self, *, token: Token, shares: float, price: float, side: str
    ) -> tuple[bool, str | None]:
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
            finally:
                submit_latency_ms = (time.perf_counter_ns() - submit_start_ns) / 1_000_000
                self.logger.info("submit order latency %.3f ms", submit_latency_ms)
            self.logger.debug("%r", resp)

            if not isinstance(resp, dict):
                self.logger.error("invalid response: %r", resp)
                return False, None

            if resp.get("success") is not True:
                self.logger.error("%s", resp.get("errorMsg") or "unknown error")
                return False, None

            order_id = resp.get("orderID")
            self.logger.info("order %s", order_id)
            return True, order_id

        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.error("%s", self._error_message(e))
            return False, None

    def _get_balance(self, params: BalanceAllowanceParams) -> float:
        resp = self.client.get_balance_allowance(params)
        if not isinstance(resp, dict):
            return 0.0
        balance = resp.get("balance", "0")
        if not isinstance(balance, str | int):
            return 0.0
        return round(int(balance) / 1_000_000, 6)

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
