from domain.market import Btc5mMarket


def main():
    market = Btc5mMarket.now()
    print(market)
    next_market = Btc5mMarket.next()
    print(next_market)


if __name__ == "__main__":
    main()
