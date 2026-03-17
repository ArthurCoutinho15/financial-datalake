import sys 
import os 

from datetime import date
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from moedas.raw.raw_coins import RawCoins
from moedas.curated.curated_coins import CuratedCoins

if __name__ == "__main__":
    raw = RawCoins(date=date(2026, 3, 13))
    raw.run()
    curated = CuratedCoins(date=date(2026, 3, 13))
    curated.run()