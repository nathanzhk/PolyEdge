from strategies.context import StrategyContext
from strategies.strategy import PositionTarget
from utils.logger import get_logger

logger = get_logger("LOGGING STRATEGY")


class LoggingStrategy:
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        return None
