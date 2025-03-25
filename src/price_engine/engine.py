# src/price_engine/engine.py
import sys
import os

# Add project_root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import time
import logging
import pandas as pd
from datetime import datetime
import csv
import requests
import sqlite3
from src.api_clients.websocket_client import WebSocketClient
import pytz

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
    def __init__(self):
        self.ws_client = None
        self.csv_files = {
            "Binance": "data/binance_data.csv",
            "Coinbase": "data/coinbase_data.csv"
        }
        self.final_csv = "data/final_weighted_avg.csv"
        self.weights = {
            "Binance": 0.6,
            "Coinbase": 0.4,
            "CoinGecko": 0.3  # Added weight for CoinGecko
        }
        self._initialize_final_csv()

    def _initialize_final_csv(self):
        """Initialize the final CSV file with headers if it doesn't exist."""
        os.makedirs("data", exist_ok=True)
        if not os.path.exists(self.final_csv):
            with open(self.final_csv, mode='w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "weighted_avg"])
            logger.info(f"Initialized final CSV file: {self.final_csv}")

    def _fetch_coingecko_historical_trades(self, symbol, start_time, end_time):
        """Fetch historical trades from CoinGecko for the given time range."""
        try:
            # Map symbol to CoinGecko ID (ETHUSDT -> Ethereum)
            symbol_map = {
                "ETHUSDT": "ethereum"
            }
            coin_id = symbol_map.get(symbol, None)
            if not coin_id:
                logger.error(f"Unsupported symbol for CoinGecko: {symbol}")
                return []

            # CoinGecko API expects timestamps in Unix seconds
            start_time_unix = int(start_time.timestamp())
            end_time_unix = int(end_time.timestamp())

            # CoinGecko historical data endpoint (daily granularity, we'll need to interpolate)
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
            params = {
                "vs_currency": "usd",
                "from": start_time_unix,
                "to": end_time_unix
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract prices (CoinGecko returns data points at varying intervals)
            if "prices" not in data or not data["prices"]:
                logger.warning(f"No historical data from CoinGecko for {symbol} in the specified range.")
                return []

            trades = []
            for price_point in data["prices"]:
                timestamp_ms = price_point[0]  # Timestamp in milliseconds
                price = price_point[1]  # Price in USD
                trade_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                if trade_time < start_time or trade_time > end_time:
                    continue
                trades.append({
                    "trade_id": f"coingecko_{timestamp_ms}",  # Synthetic trade ID
                    "price": float(price),
                    "timestamp": trade_time
                })

            return trades

        except Exception as e:
            logger.error(f"Error fetching CoinGecko historical trades: {e}")
            return []

    def calculate_weighted_average(self, prices):
        """Calculate the weighted average price from all sources."""
        if not prices:
            return None
        total_weight = 0
        weighted_sum = 0
        for source_name, price in prices.items():
            if source_name in self.weights:
                weight = self.weights[source_name]
                weighted_sum += price * weight
                total_weight += weight
        return weighted_sum / total_weight if total_weight > 0 else None

    def process_csv_files(self):
        """Process individual CSV files, calculate weighted averages per second, and write to final CSV."""
        logger.info("Processing CSV files to calculate weighted averages per second...")

        # Get the local timezone
        local_tz = datetime.now().astimezone().tzinfo

        # Read all CSV files into DataFrames
        dfs = {}
        for source, file_path in self.csv_files.items():
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    # Parse timestamps and make them timezone-aware in the local timezone
                    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(local_tz)
                    dfs[source] = df.set_index('timestamp')
                else:
                    dfs[source] = pd.DataFrame(columns=['price'], index=pd.DatetimeIndex([]))
                    logger.warning(f"{source} CSV is empty.")
            except FileNotFoundError:
                logger.warning(f"CSV file not found for {source}: {file_path}")
                dfs[source] = pd.DataFrame(columns=['price'], index=pd.DatetimeIndex([]))
            except Exception as e:
                logger.error(f"Error reading CSV file for {source}: {e}")
                dfs[source] = pd.DataFrame(columns=['price'], index=pd.DatetimeIndex([]))

        # Find the union of all timestamps
        all_timestamps = pd.concat([df.reset_index()['timestamp'] for df in dfs.values()], ignore_index=True).drop_duplicates().sort_values()
        if all_timestamps.empty:
            logger.warning("No timestamps found in CSV files.")
            return

        # Resample to per-second intervals and calculate weighted averages
        for source, df in dfs.items():
            if not df.empty:
                # Resample to per-second intervals, taking the last price in each second
                df_resampled = df.resample('s').last().ffill()
                dfs[source] = df_resampled

        # Align timestamps across all sources
        start_time = min(df.index.min() for df in dfs.values() if not df.empty)
        end_time = max(df.index.max() for df in dfs.values() if not df.empty)
        if not start_time or not end_time:
            logger.warning("No valid time range found in CSV files.")
            return

        # Generate per-second timestamps in the local timezone
        time_range = pd.date_range(start=start_time.floor('s'), end=end_time.floor('s'), freq='s', tz=local_tz)
        try:
            with open(self.final_csv, mode='w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "weighted_avg"])  # Rewrite header

                for timestamp in time_range:
                    prices = {}
                    for source, df in dfs.items():
                        if timestamp in df.index:
                            prices[source] = df.loc[timestamp, 'price']
                        else:
                            # Find the most recent price before this timestamp
                            past_data = df[df.index < timestamp]
                            if not past_data.empty:
                                prices[source] = past_data.iloc[-1]['price']

                    weighted_avg = self.calculate_weighted_average(prices)
                    if weighted_avg is not None:
                        # Timestamp is already in local timezone
                        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                        writer.writerow([timestamp_str, weighted_avg])
                        logger.debug(f"Calculated weighted average for {timestamp_str}: {weighted_avg}")
                f.flush()  # Ensure data is written to disk
        except Exception as e:
            logger.error(f"Error writing to final CSV file {self.final_csv}: {e}")
            return

        logger.info(f"Finished processing CSV files and writing to {self.final_csv}.")

        # Store the CSV data in SQLite database
        db_path = "crypto_prices.db"  # Store in project root directory
        conn = None
        try:
            # Connect to SQLite database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            logger.info(f"Connected to SQLite database at {db_path}")

            # Create the weighted_avg_prices table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS weighted_avg_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                weighted_avg REAL NOT NULL
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
            logger.info("Table 'weighted_avg_prices' created or already exists.")

            # Read the CSV file into a DataFrame
            df = pd.read_csv(self.final_csv)
            logger.info(f"Read {len(df)} rows from {self.final_csv}")

            # Insert each row into the database
            insert_query = """
            INSERT INTO weighted_avg_prices (timestamp, weighted_avg)
            VALUES (?, ?)
            """
            for _, row in df.iterrows():
                cursor.execute(insert_query, (
                    row['timestamp'],
                    row['weighted_avg']
                ))
            conn.commit()
            logger.info(f"Stored {len(df)} rows in the database.")

        except (pd.errors.EmptyDataError, KeyError, sqlite3.Error) as e:
            logger.error(f"Error storing CSV data to database: {e}")
        finally:
            if conn:
                conn.close()
                logger.info("Database connection closed.")

    def query_weighted_avg_prices(self, start_date, end_date):
        """Query the weighted average prices from the SQLite database within the specified date range."""
        logger.info(f"Querying weighted average prices from {start_date} to {end_date}...")

        # Parse start and end dates
        try:
            start_time = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            logger.error(f"Invalid date format. Use 'YYYY-MM-DD HH:MM:SS'. Error: {e}")
            return []

        # Validate time range
        if start_time >= end_time:
            logger.error("Start date must be before end date.")
            return []

        db_path = "crypto_prices.db"
        conn = None
        results = []
        try:
            # Connect to SQLite database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            logger.info(f"Connected to SQLite database at {db_path}")

            # Query the database
            query = """
            SELECT timestamp, weighted_avg
            FROM weighted_avg_prices
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            """
            cursor.execute(query, (start_date, end_date))
            rows = cursor.fetchall()

            # Format the results
            for row in rows:
                results.append({
                    "timestamp": row[0],
                    "weighted_avg": row[1]
                })

            logger.info(f"Retrieved {len(results)} rows from the database.")

        except sqlite3.Error as e:
            logger.error(f"Error querying database: {e}")
        finally:
            if conn:
                conn.close()
                logger.info("Database connection closed.")

        return results

    def display_historical_data(self, symbol, start_date, end_date, sources=None):
        """Fetch and display historical weighted average data for the specified time range."""
        logger.info(f"Fetching historical data for {symbol} from {start_date} to {end_date}...")

        # Parse start and end dates as UTC (since API timestamps are in UTC)
        try:
            start_time = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
            end_time = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        except ValueError as e:
            logger.error(f"Invalid date format. Use 'YYYY-MM-DD HH:MM:SS'. Error: {e}")
            return

        # Validate time range
        if start_time >= end_time:
            logger.error("Start date must be before end date.")
            return

        # Determine sources
        if sources is None:
            sources = ["Binance", "Coinbase", "CoinGecko"]
        else:
            valid_sources = ["Binance", "Coinbase", "CoinGecko"]
            for source in sources:
                if source not in valid_sources:
                    logger.error(f"Invalid source: {source}. Valid sources are {valid_sources}")
                    return

        # Fetch historical data
        historical_data = {}
        for source in sources:
            if source == "Binance":
                trades = self.ws_client._fetch_binance_historical_trades(symbol, start_time, end_time)
            elif source == "Coinbase":
                trades = self.ws_client._fetch_coinbase_historical_trades(start_time, end_time)
            elif source == "CoinGecko":
                trades = self._fetch_coingecko_historical_trades(symbol, start_time, end_time)
            else:
                continue

            # Convert trades to DataFrame
            if trades:
                df = pd.DataFrame(trades, columns=["timestamp", "price", "trade_id"])
                df = df[["timestamp", "price"]].set_index("timestamp")
                historical_data[source] = df
            else:
                historical_data[source] = pd.DataFrame(columns=['price'], index=pd.DatetimeIndex([]))
                logger.warning(f"No historical data fetched for {source}.")

        # If no data was fetched, exit
        if not any(df.shape[0] > 0 for df in historical_data.values()):
            logger.warning("No historical data available for the specified time range.")
            return

        # Resample to per-second intervals
        local_tz = datetime.now().astimezone().tzinfo
        for source, df in historical_data.items():
            if not df.empty:
                # Convert UTC timestamps to local timezone
                df.index = df.index.tz_convert(local_tz)
                df_resampled = df.resample('s').last().ffill()
                historical_data[source] = df_resampled

        # Align timestamps across all sources
        start_time_local = start_time.astimezone(local_tz)
        end_time_local = end_time.astimezone(local_tz)
        time_range = pd.date_range(start=start_time_local.floor('s'), end=end_time_local.floor('s'), freq='s', tz=local_tz)

        # Calculate and display weighted averages
        print("\nHistorical Weighted Average Prices:")
        print("-----------------------------------")
        print("Timestamp".ljust(20), "Weighted Average Price")
        print("-----------------------------------")
        for timestamp in time_range:
            prices = {}
            for source, df in historical_data.items():
                if timestamp in df.index:
                    prices[source] = df.loc[timestamp, 'price']
                else:
                    # Find the most recent price before this timestamp
                    past_data = df[df.index < timestamp]
                    if not past_data.empty:
                        prices[source] = past_data.iloc[-1]['price']

            weighted_avg = self.calculate_weighted_average(prices)
            if weighted_avg is not None:
                timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                print(f"{timestamp_str.ljust(20)} {weighted_avg:.2f}")

        logger.info("Finished displaying historical data.")

    def track_live_prices(self, symbol, sources=None):
        """Continuously track live prices using WebSocket and store in CSV files."""
        logger.info(f"Starting real-time price tracking for {symbol} with sources: {sources if sources else 'all'}...")

        def on_price_update(symbol, prices, fetch_time):
            # No action needed; data is already written to CSV by WebSocketClient
            pass

        def on_disconnect(source):
            """Handle disconnection events from WebSocketClient."""
            logger.warning(f"Disconnection detected for {source}. Data will be missing in CSV until reconnection.")

        self.ws_client = WebSocketClient(on_price_update, on_disconnect)
        self.ws_client.start(symbol, sources=sources)

        try:
            while True:
                time.sleep(1)  # Keep the main thread alive
        except KeyboardInterrupt:
            logger.info("Stopping WebSocket connections...")
            self.ws_client.stop()
            logger.info("Generating final weighted average CSV file...")
            self.process_csv_files()