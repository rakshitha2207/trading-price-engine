# src/price_engine/engine.py
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
import os
from src.api_clients.binance_api import BinanceAPI
from src.api_clients.coinbase_api import CoinbaseAPI
from src.api_clients.coingecko_api import CoinGeckoAPI
from src.database.db_manager import DatabaseManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(__file__)), "trading_engine.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CryptoTradingEngine")

class PriceEngine:
    def __init__(self, db_path: str):
        self.binance = BinanceAPI()
        self.coinbase = CoinbaseAPI()
        self.coingecko = CoinGeckoAPI()
        self.db = DatabaseManager(db_path)
        self.sources = {
            "Binance": (self.binance, 0.4),
            "Coinbase": (self.coinbase, 0.3),
            "CoinGecko": (self.coingecko, 0.3),
        }
        self.last_fetch_time = None

    def fetch_live_prices(self, symbol: str, fetch_time: Optional[datetime] = None) -> Dict[str, float]:
        """Fetch live prices from all sources and save the weighted average to the database."""
        prices = {}
        for source_name, (api, _) in self.sources.items():
            try:
                price = api.get_price(symbol)
                if price is not None:
                    prices[source_name] = price
            except Exception as e:
                logger.error(f"Error fetching price from {source_name} for {symbol}: {e}")

        weighted_avg = self.calculate_weighted_average(prices)
        if weighted_avg is not None:
            save_time = fetch_time if fetch_time else datetime.now()
            self.db.save_live_price(symbol, weighted_avg, save_time)
            self.last_fetch_time = save_time
            logger.info(f"Saved live price for {symbol} at {save_time}: {weighted_avg}")

        return prices

    def fetch_historical_prices(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Dict[str, float]]:
        """Fetch historical prices from all sources and save to database."""
        historical_data = {}
        for source_name, (api, _) in self.sources.items():
            prices = api.get_historical_price(symbol, start_date, end_date)
            if prices is not None:
                historical_data[source_name] = prices
                for date, price in prices.items():
                    self.db.save_historical_price(symbol, source_name, date, price)
        return historical_data

    def calculate_weighted_average(self, prices: Dict[str, float]) -> Optional[float]:
        """Calculate the weighted average price from all sources."""
        if not prices:
            return None
        total_weight = 0
        weighted_sum = 0
        for source_name, price in prices.items():
            _, weight = self.sources[source_name]
            weighted_sum += price * weight
            total_weight += weight
        return weighted_sum / total_weight if total_weight > 0 else None

    def fill_missed_data(self, symbol: str, start_time: datetime, end_time: datetime):
        """Fill missed data for the given time range by fetching historical data."""
        logger.info(f"Filling missed data for {symbol} from {start_time} to {end_time}...")
        start_date = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_date = end_time.strftime("%Y-%m-%d %H:%M:%S")

        historical_data = self.fetch_historical_prices(symbol, start_date, end_date)
        logger.info(f"Historical data fetched: {historical_data}")

        existing_entries = self.db.get_live_prices_in_range(symbol, start_time, end_time)
        logger.info(f"Existing entries in range: {existing_entries}")

        # Align the start time to the next 5-second interval
        start_seconds = start_time.second
        start_remainder = start_seconds % 5
        if start_remainder != 0:
            start_time = start_time + timedelta(seconds=(5 - start_remainder))
        current_time = start_time

        # Get the last known price before the gap as a fallback
        last_known_price = None
        if existing_entries:
            last_timestamp = max(existing_entries.keys())
            last_known_price = existing_entries[last_timestamp]
        else:
            # Fetch the most recent price before start_time
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT price FROM live_prices WHERE symbol = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT 1",
                    (symbol, start_date)
                )
                result = cursor.fetchone()
                if result:
                    last_known_price = result[0]

        while current_time < end_time:  # Stop before the end_time to avoid duplicating the reconnection entry
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            if current_time_str in existing_entries:
                logger.info(f"Skipping {current_time_str} - entry already exists: {existing_entries[current_time_str]}")
                current_time += timedelta(seconds=5)
                continue

            historical_prices = {}
            for source_name, prices in historical_data.items():
                closest_price = None
                closest_time_diff = float('inf')
                for timestamp, price in prices.items():
                    ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    time_diff = abs((ts - current_time).total_seconds())
                    if time_diff < closest_time_diff:
                        closest_time_diff = time_diff
                        closest_price = price
                if closest_price is not None:
                    historical_prices[source_name] = closest_price

            weighted_avg = self.calculate_weighted_average(historical_prices)
            logger.info(f"Calculated weighted average for {current_time_str}: {weighted_avg}")

            if weighted_avg is not None:
                last_price = last_known_price if last_known_price is not None else weighted_avg
                if abs(weighted_avg - last_price) > 50:
                    logger.warning(f"Outlier detected for {symbol} at {current_time_str}: {weighted_avg}. Using last known price: {last_price}")
                    weighted_avg = last_price

                self.db.save_live_price(symbol, weighted_avg, current_time)
                logger.info(f"Filled data for {symbol} at {current_time_str}: {weighted_avg}")
                last_known_price = weighted_avg
            else:
                if last_known_price is not None:
                    logger.info(f"No historical data available to fill {symbol} at {current_time_str}. Using last known price: {last_known_price}")
                    self.db.save_live_price(symbol, last_known_price, current_time)
                    logger.info(f"Filled data for {symbol} at {current_time_str}: {last_known_price}")
                else:
                    logger.warning(f"No historical data or last known price available to fill {symbol} at {current_time_str}")

            current_time += timedelta(seconds=5)

    def track_live_prices(self, symbol: str, interval: int = 5):
        """Continuously track live prices at the specified interval (in seconds)."""
        logger.info(f"Starting real-time price tracking for {symbol} (interval: {interval} seconds)...")
        
        # Align the start time to the nearest 5-second interval
        start_time = datetime.now()
        seconds = start_time.second
        remainder = seconds % interval
        if remainder != 0:
            time.sleep(interval - remainder)
            start_time = datetime.now().replace(microsecond=0)
        
        next_fetch_time = start_time

        while True:
            current_time = datetime.now()
            if current_time < next_fetch_time:
                time.sleep((next_fetch_time - current_time).total_seconds())
                continue

            fetch_start_time = next_fetch_time  # Use the scheduled fetch time
            logger.info(f"Attempting to fetch prices at {fetch_start_time}...")
            prices = self.fetch_live_prices(symbol, fetch_start_time)

            if not prices:
                logger.warning(f"Disconnection detected at {fetch_start_time} - all sources failed.")
                disconnect_time = fetch_start_time

                while True:
                    try:
                        reconnect_time = datetime.now()
                        test_prices = self.fetch_live_prices(symbol, fetch_start_time)
                        if test_prices:
                            logger.info(f"Connection restored at {reconnect_time}.")
                            break
                    except Exception as e:
                        logger.error(f"Connection still down: {e}")
                        time.sleep(1)

                if self.last_fetch_time:
                    # Calculate the correct time range for filling missed data
                    # Start from the last successful fetch time
                    start_fill_time = self.last_fetch_time
                    # End at the next scheduled fetch time after reconnection
                    reconnect_fetch_time = disconnect_time
                    while reconnect_fetch_time <= reconnect_time:
                        reconnect_fetch_time += timedelta(seconds=interval)
                    logger.info(f"Filling missed data from {start_fill_time} to {reconnect_fetch_time}...")
                    self.fill_missed_data(symbol, start_fill_time, reconnect_fetch_time)
                else:
                    logger.warning("No previous fetch time available; cannot fill missed data.")

                self.last_fetch_time = reconnect_fetch_time - timedelta(seconds=interval)
                prices = test_prices

                next_fetch_time = reconnect_fetch_time
            else:
                next_fetch_time += timedelta(seconds=interval)

            weighted_avg = self.calculate_weighted_average(prices)
            data = []
            for source, price in prices.items():
                data.append([source, symbol, f"{price:.2f}"])
            if weighted_avg is not None:
                data.append(["Weighted Avg", symbol, f"{weighted_avg:.2f}"])

            df = pd.DataFrame(data, columns=["Source", "Symbol", "Price (USD)"])
            logger.info(f"\nLive Prices at {fetch_start_time.strftime('%Y-%m-%d %H:%M:%S')}:")
            logger.info(df.to_string(index=False))

    def display_live_prices(self, symbol: str, use_db: bool = False):
        """Fetch and display live prices in a table."""
        if use_db:
            prices = self.db.get_live_prices(symbol)
            if prices:
                weighted_avg = prices["Weighted Avg"]
                data = [["Weighted Avg", symbol, f"{weighted_avg:.2f}"]]
            else:
                logger.warning("No data found in the database for this symbol.")
                return
        else:
            prices = self.fetch_live_prices(symbol)
            weighted_avg = self.calculate_weighted_average(prices)

            data = []
            for source, price in prices.items():
                data.append([source, symbol, f"{price:.2f}"])
            if weighted_avg is not None:
                data.append(["Weighted Avg", symbol, f"{weighted_avg:.2f}"])

        df = pd.DataFrame(data, columns=["Source", "Symbol", "Price (USD)"])
        logger.info("\nLive Prices:")
        logger.info(df.to_string(index=False))

    def display_historical_prices(self, symbol: str, start_date: str, end_date: str, use_db: bool = False):
        """Fetch and display historical prices in a table."""
        if use_db:
            historical_data = self.db.get_historical_prices(symbol, start_date, end_date)
        else:
            historical_data = self.fetch_historical_prices(symbol, start_date, end_date)

        if not historical_data:
            logger.warning("No historical data available.")
            return

        all_dates = set()
        for prices in historical_data.values():
            all_dates.update(prices.keys())
        all_dates = sorted(all_dates)

        data = []
        for date in all_dates:
            row = [date]
            for source_name in historical_data:
                price = historical_data[source_name].get(date, "N/A")
                row.append(f"{price:.2f}" if isinstance(price, (int, float)) else price)
            data.append(row)

        columns = ["Date"] + list(historical_data.keys())
        df = pd.DataFrame(data, columns=columns)
        logger.info(f"\nHistorical Prices for {symbol} ({start_date} to {end_date}):")
        logger.info(df.to_string(index=False))