from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    GTC = "GTC"
    GTD = "GTD"
    FOK = "FOK"
    FAK = "FAK"


class MarketOrderStatus(StrEnum):
    LIVE = "LIVE"
    INVALID = "INVALID"
    MATCHED = "MATCHED"
    CANCELED = "CANCELED"
    CANCELED_MARKET_RESOLVED = "CANCELED_MARKET_RESOLVED"


class MarketTradeStatus(StrEnum):
    MATCHED = "MATCHED"
    MINED = "MINED"
    CONFIRMED = "CONFIRMED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"


class ManagedOrderStatus(StrEnum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    SUBMIT_FAILED = "SUBMIT_FAILED"
    PENDING_MATCH = "PENDING_MATCH"
    MATCHED = "MATCHED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"


class ManagedTradeStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
