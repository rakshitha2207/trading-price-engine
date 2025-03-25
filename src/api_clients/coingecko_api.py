import requests
from datetime import datetime, timedelta
from typing import Optional, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class CoinGeckoAPI:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"

    def _map_symbol(self, symbol: str) -> str:
        """Map symbols to CoinGecko IDs (e.g., BTCUSDT -> bitcoin)."""
        symbol_map = {
            "BTCUSDT": "bitcoin",
            "ETHUSDT": "ethereum",
            "SOLUSDT": "solana",
        }
        return symbol_map.get(symbol, symbol)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException,)),
        reraise=True
    )
    def get_price(self, symbol: str) -> Optional[float]:
        """Fetch the current price for a symbol from CoinGecko."""
        coingecko_id = self._map_symbol(symbol)
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": coingecko_id,
            "vs_currencies": "usd",
        }
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            return float(data[coingecko_id]["usd"])
        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"Failed to fetch price from CoinGecko for {symbol}: {e}")
            return None

    def get_historical_price(self, symbol: str, start_date: str, end_date: str) -> Optional[Dict[str, float]]:
        """Fetch historical prices for a symbol from CoinGecko."""
        # CoinGecko's historical API is daily and not suitable for minute-level data
        print(f"Historical data fetching from CoinGecko is not supported for minute-level granularity for {symbol}.")
        return None