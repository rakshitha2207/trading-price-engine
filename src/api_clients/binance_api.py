import requests
from datetime import datetime
from typing import Optional, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class BinanceAPI:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException,)),
        reraise=True
    )
    def get_price(self, symbol: str) -> Optional[float]:
        """Fetch the current price for a symbol from Binance."""
        url = f"{self.base_url}/ticker/price"
        params = {"symbol": symbol}
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            return float(response.json()["price"])
        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"Failed to fetch price from Binance for {symbol}: {e}")
            return None

    def get_historical_price(self, symbol: str, start_date: str, end_date: str) -> Optional[Dict[str, float]]:
        """Fetch historical prices for a symbol from Binance."""
        url = f"{self.base_url}/klines"
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S" if " " in start_date else "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S" if " " in end_date else "%Y-%m-%d")
            start_ts = int(start_dt.timestamp() * 1000)
            end_ts = int(end_dt.timestamp() * 1000) + (86399999 if " " not in end_date else 0)

            if symbol not in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
                print(f"Unsupported symbol for Binance: {symbol}")
                return None

            params = {
                "symbol": symbol,
                "interval": "1m",
                "startTime": start_ts,
                "endTime": end_ts,
                "limit": 1000,
            }

            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()

            if not data:
                print(f"No historical data returned from Binance for {symbol} between {start_date} and {end_date}")
                return None

            historical_prices = {}
            for entry in data:
                date = datetime.fromtimestamp(entry[0] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                close_price = float(entry[4])
                historical_prices[date] = close_price

            return historical_prices if historical_prices else None

        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"Failed to fetch historical price from Binance for {symbol}: {e}")
            print(f"Response (if any): {response.text if 'response' in locals() else 'No response'}")
            return None
        except Exception as e:
            print(f"Unexpected error in Binance historical fetch for {symbol}: {e}")
            return None