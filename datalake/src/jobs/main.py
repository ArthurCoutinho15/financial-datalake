import sys
import os

from datetime import date, datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from moedas.raw.raw_coins import RawCoins
from moedas.curated.curated_coins import CuratedCoins
from stocks.raw.stocks_job import RawStocksJob
from stocks.curated.stocks_job import CuratedStocks

if __name__ == "__main__":
    raw = RawCoins(date=date(2026, 3, 13))
    raw.run()
    curated = CuratedCoins(date=date(2026, 3, 13))
    curated.run()

    raw_stocks = RawStocksJob(date=datetime(2026, 3, 13, 21, 24, 0))
    raw_stocks.run()
    curated_stocks = CuratedStocks(date=date(2026, 3, 13))
    curated_stocks.run()
