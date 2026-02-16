from dotenv import load_dotenv

from adapters.trade_client import MakerTradeClient
from common.logger import configure_logging
from domain.market import Btc5mMarket, Market


def main():
    load_dotenv()
    configure_logging()

    maker = MakerTradeClient()
    market: Market = Btc5mMarket.now()

    maker.warm_up(market.yes_token)
    maker.warm_up(market.no_token)

    order_id = maker.buy(market.yes_token, 5, 0.01)
    if order_id is not None:
        order = maker.get_order_by_id(order_id)
        if order is not None:
            print(order)
            maker.cancel_order(order.id)


if __name__ == "__main__":
    main()
