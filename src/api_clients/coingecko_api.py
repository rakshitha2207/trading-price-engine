# src/price_engine/data_sources/coingecko_api.py
import requests
from datetime import datetime

class CoinGeckoAPI:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"

    def get_price(self, coin_id: str, vs_currency: str = "usd") -> float:
        url = f"{self.base_url}/simple/price"
        params = {"ids": coin_id, "vs_currencies": vs_currency}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if coin_id in data and vs_currency in data[coin_id]:
                return float(data[coin_id][vs_currency])
            else:
                raise Exception(f"Invalid coin ID or currency: {coin_id}, {vs_currency}")
        else:
            raise Exception(f"Failed to fetch price from CoinGecko: {response.text}")

    def get_historical_price(self, coin_id: str, date: str, vs_currency: str = "usd") -> float:
        """
        Fetch historical price for a specific date.
        Date format: dd-mm-yyyy
        """
        url = f"{self.base_url}/coins/{coin_id}/history"
        params = {"date": date, "localization": "false"}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if "market_data" in data:
                return float(data["market_data"]["current_price"][vs_currency])
            else:
                raise Exception(f"No market data found for {coin_id} on {date}")
        else:
            raise Exception(f"Failed to fetch historical price from CoinGecko: {response.text}")