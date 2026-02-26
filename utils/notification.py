import os

import requests

from utils.time import fmt_ts_ms

_CHAT_ID = "895951888"
_BOT_KEY = os.getenv("TELEGRAM_BOT_KEY", "")
_BASE_URL = f"https://api.telegram.org/bot{_BOT_KEY}"


def send_trade(market_start_ms, market_end_ms, side, token, shares, price, amount=None, pnl=None):
    if amount is None:
        amount = shares * price
    market_date = fmt_ts_ms(market_start_ms, tz="market", fmt="date")
    market_start_time = fmt_ts_ms(market_start_ms, tz="market", fmt="time")
    market_end_time = fmt_ts_ms(market_end_ms, tz="market", fmt="time")
    message = [
        f"BTC 5M {market_date} {market_start_time}-{market_end_time}",
        f"{side.upper()} {shares:.2f} {token.upper()} at {price:.2f}",
        f"AMOUNT ${amount:.2f}",
    ]
    if pnl is not None:
        message[-1] += f" >>> -${abs(pnl):.2f}" if pnl < 0 else f" >>> +${pnl:.2f}"
    send_message(message)


def send_message(message: list[str]) -> bool:
    response = requests.post(
        f"{_BASE_URL}/sendMessage",
        json={
            "parse_mode": "HTML",
            "chat_id": _CHAT_ID,
            "text": "\n".join(f"<code>{line}</code>" for line in message),
        },
    )
    return response.json()["ok"]
