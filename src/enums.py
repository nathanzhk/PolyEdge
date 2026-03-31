from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class Role(StrEnum):
    MAKER = "MAKER"
    TAKER = "TAKER"


class MarketOrderType(StrEnum):
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
    SUBMITTING = "SUBMITTING"  # order_id is None & (pending_shares > 0)
    SUB_FAILED = "SUBMIT_FAILED"  # order_id is None & (invalid_shares > 0)
    ZERO_MATCHED = "ZERO_MATCHED"  # valid & (matched_shares == 0)
    PART_MATCHED = "PART_MATCHED"  # valid & (0 < matched_shares < threshold)
    FULL_MATCHED = "FULL_MATCHED"  # valid & (matched_shares > threshold)
    ZERO_MATCHED_CANCELED = "CANCELED/ZERO_MATCHED"  # invalid & (matched_shares == 0)
    PART_MATCHED_CANCELED = "CANCELED/PART_MATCHED"  # invalid & (matched_shares > 0)


class ManagedTradeStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
