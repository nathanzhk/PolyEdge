from dotenv import load_dotenv

from adapters.trade_client import MakerTradeClient
from common.logger import configure_logging
from domain.market import Btc5mMarket


def main():
    load_dotenv()
    configure_logging()

    maker = MakerTradeClient()

    market = Btc5mMarket.now()
    maker.warm_up(market.up_token)
    maker.buy(market.up_token, 5, 0.01)
    maker.buy(market.up_token, 5, 0.01)


if __name__ == "__main__":
    main()
