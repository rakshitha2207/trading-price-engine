import requests
from typing import Optional, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class CoinbaseAPI:
    def __init__(self):
        self.base_url = "https://api.coinbase.com/v2/prices"

    def _map_symbol(self, symbol: str) -> str:
        """Map symbols to Coinbase format (e.g., BTCUSDT -> BTC-USD)."""
        symbol_map = {
            "BTCUSDT": "BTC-USD",
            "ETHUSDT": "ETH-USD",
            "SOLUSDT": "SOL-USD",
        }
        return symbol_map.get(symbol, symbol)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException,)),
        reraise=True
    )
    def get_price(self, symbol: str) -> Optional[float]:
        """Fetch the current price for a symbol from Coinbase."""
        coinbase_symbol = self._map_symbol(symbol)
        url = f"{self.base_url}/{coinbase_symbol}/spot"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            return float(data["data"]["amount"])
        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"Failed to fetch price from Coinbase for {symbol}: {e}")
            return None

    def get_historical_price(self, symbol: str, start_date: str, end_date: str) -> Optional[Dict[str, float]]:
        """Fetch historical prices for a symbol from Coinbase (simplified)."""
        print(f"Historical data fetching from Coinbase is not supported in this setup for {symbol}.")
        return None