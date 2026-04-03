import os
import requests
from typing import Optional, Dict, Any
from datetime import timedelta, datetime, time

from utils.rate_limiter import RateLimiter

import logging

from dotenv import load_dotenv

load_dotenv()


class CryptoStocksClient:
    def __init__(
        self,
        base_url: str = "https://api.twelvedata.com/",
        timeout: int = 30,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.API_KEY = str(os.getenv("API_KEY"))
        self.session = requests.Session()
        self.limiter = RateLimiter(max_requests=8, period=60)

    def _make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        
        self.limiter.wait()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            logging.info(f"Making request | URL: {url}")
            response = self.session.get(url=url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            if response.status_code == 429:
                logging.warning("429 Too Many Requests. Aguardando 60s...")
                time.sleep(60)
                return self._make_request(endpoint, params)

            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {e} | URL: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP error: {e} | URL: {url}")
            raise
        except Exception as e:
            logging.error(f"Unknown error: {e} | URL: {url}")
            raise

    def get_stocks(self, date: datetime, stocks_symbol: list[str]):
        endpoint = "time_series"

        params = {
            "apikey": self.API_KEY,
            "symbol": ",".join(stocks_symbol),
            "interval": "1day",
            "format": "JSON",
            "start_date": (date - timedelta(1)).strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": date.strftime("%Y-%m-%d %H:%M:%S"),
        }
        data = self._make_request(endpoint=endpoint, params=params)

        return data
