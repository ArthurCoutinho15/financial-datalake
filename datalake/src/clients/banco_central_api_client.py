import requests
from typing import Dict, Any, Optional
import logging

from datetime import date


class BancoCentralApiClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout

    def make_request(
        self, endpoint: str, params: Optional[dict] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            logging.info(f"Fazendo requisição | URL: {url}")
            response = requests.get(url=url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {e} | URL: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e} | URL: {url}")
            raise

    def get_moedas(self) -> dict:
        endpoint = "Moedas"
        params = {"$format": "json"}

        data = self.make_request(endpoint=endpoint, params=params)

        return data

    def get_coin_daily_cotation(self, moeda: str, cotation_date: date) -> dict:
        endpoint = "CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)"
        params = {
            "@moeda": f"'{moeda}'",
            "@dataCotacao": f"'{cotation_date.strftime('%m-%d-%Y')}'",
            "$format": "json"
        }
        
        data = self.make_request(endpoint=endpoint, params=params)
        
        return data
