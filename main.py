from dotenv import load_dotenv

from adapters.trade_client import MakerTradeClient
from common.logger import configure_logging
from domain.btc_market import BTC5mMarket
from domain.market import Market


def main():
    load_dotenv()
    configure_logging()

    maker = MakerTradeClient()
    market: Market = BTC5mMarket.now()

    maker.warm_up(market.yes_token)
    maker.warm_up(market.no_token)

    order_id = maker.buy(market.yes_token, 5, 0.01)
    if order_id is not None:
        orders = maker.get_orders_by_token(market.yes_token)
        for order in orders:
            maker.cancel_order_by_id(order.id)


if __name__ == "__main__":
    main()
