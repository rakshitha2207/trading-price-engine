# src/api_clients/websocket_client.py
import websocket
import json
import threading
import time
from datetime import datetime
import logging
import csv
import os
from collections import deque
import requests
import pytz

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "trading_engine.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("WebSocketClient")

class WebSocketClient:
    def __init__(self, on_price_update, on_disconnect):
        self.on_price_update = on_price_update
        self.on_disconnect = on_disconnect
        self.ws_threads = {}
        self.ws_apps = {}
        self.running = False
        self.csv_files = {
            "Binance": "data/binance_data.csv",
            "Coinbase": "data/coinbase_data.csv"
        }
        self.csv_locks = {source: threading.Lock() for source in self.csv_files}
        self.processed_trade_ids = {
            "Binance": deque(maxlen=10000),
            "Coinbase": deque(maxlen=10000)
        }
        self.processed_entries = {
            "Binance": deque(maxlen=10000),
            "Coinbase": deque(maxlen=10000)
        }
        self.dedupe_locks = {source: threading.Lock() for source in self.csv_files}
        self.disconnect_times = {source: None for source in self.csv_files}
        self.reconnect_times = {source: None for source in self.csv_files}
        self._initialize_csv_files()

    def _initialize_csv_files(self):
        """Initialize CSV files with headers if they don't exist."""
        os.makedirs("data", exist_ok=True)
        for source, file_path in self.csv_files.items():
            if not os.path.exists(file_path):
                with open(file_path, mode='w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp", "price"])
                logger.info(f"Initialized CSV file for {source}: {file_path}")

    def _write_to_csv(self, source, timestamp, price):
        """Write price data to the corresponding CSV file in the system's local timezone."""
        file_path = self.csv_files[source]
        # Convert UTC timestamp to local timezone
        local_timestamp = timestamp.astimezone()
        timestamp_str = local_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with self.csv_locks[source]:
            with open(file_path, mode='a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp_str, price])
            logger.debug(f"Wrote to {source} CSV: {timestamp_str}, {price}")

    def _fetch_binance_historical_trades(self, symbol, start_time, end_time):
        """Fetch historical trades from Binance for the given time range."""
        try:
            url = "https://api.binance.com/api/v3/historicalTrades"
            params = {
                "symbol": symbol,
                "limit": 1000
            }
            trades = []
            last_id = None

            start_time_ms = int(start_time.timestamp() * 1000)
            end_time_ms = int(end_time.timestamp() * 1000)

            while True:
                if last_id:
                    params["fromId"] = last_id
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                for trade in data:
                    trade_time_ms = int(trade["time"])
                    if trade_time_ms < start_time_ms:
                        continue
                    if trade_time_ms > end_time_ms:
                        return trades
                    trades.append({
                        "trade_id": trade["id"],
                        "price": float(trade["price"]),
                        "timestamp": datetime.fromtimestamp(trade_time_ms / 1000, tz=pytz.UTC)
                    })
                    last_id = trade["id"]

        except Exception as e:
            logger.error(f"Error fetching Binance historical trades: {e}")
            return []
        return trades

    def _fetch_coinbase_historical_trades(self, start_time, end_time):
        """Fetch historical trades from Coinbase for the given time range."""
        try:
            url = "https://api.exchange.coinbase.com/products/ETH-USD/trades"
            headers = {"Accept": "application/json"}
            trades = []
            after = None

            start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
            end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"

            while True:
                params = {"limit": 100}
                if after:
                    params["after"] = after
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                for trade in data:
                    trade_time = datetime.strptime(trade["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
                    if trade_time < start_time:
                        return trades
                    if trade_time > end_time:
                        continue
                    trades.append({
                        "trade_id": trade["trade_id"],
                        "price": float(trade["price"]),
                        "timestamp": trade_time
                    })
                    after = trade["trade_id"]

        except Exception as e:
            logger.error(f"Error fetching Coinbase historical trades: {e}")
            return []
        return trades

    def _fill_missed_data(self, source, symbol, disconnect_time, reconnect_time):
        """Fetch and write missed data for the disconnection period."""
        if not disconnect_time or not reconnect_time:
            logger.warning(f"No disconnection time recorded for {source}. Skipping historical data fetch.")
            return

        logger.info(f"Fetching missed data for {source} from {disconnect_time} to {reconnect_time}...")

        if source == "Binance":
            trades = self._fetch_binance_historical_trades(symbol, disconnect_time, reconnect_time)
        elif source == "Coinbase":
            trades = self._fetch_coinbase_historical_trades(disconnect_time, reconnect_time)
        else:
            return

        for trade in trades:
            trade_id = trade["trade_id"]
            price = trade["price"]
            fetch_time = trade["timestamp"]
            entry_key = (fetch_time, price)

            with self.dedupe_locks[source]:
                if trade_id in self.processed_trade_ids[source]:
                    logger.debug(f"Skipping duplicate {source} trade ID (historical): {trade_id}")
                    continue
                if entry_key in self.processed_entries[source]:
                    logger.debug(f"Skipping duplicate {source} entry (historical): {entry_key}")
                    continue
                self.processed_trade_ids[source].append(trade_id)
                self.processed_entries[source].append(entry_key)

            logger.debug(f"Fetched historical {source} data: {fetch_time}, {price}, trade_id: {trade_id}")
            self._write_to_csv(source, fetch_time, price)

        logger.info(f"Finished fetching missed data for {source}.")

    def _on_binance_message(self, ws, message):
        """Handle Binance WebSocket messages."""
        try:
            logger.debug(f"Raw Binance message: {message}")
            data = json.loads(message)
            if "e" in data and data["e"] == "trade":
                trade_id = data["t"]
                price = float(data["p"])
                timestamp_ms = int(data["T"])
                fetch_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                entry_key = (fetch_time, price)

                with self.dedupe_locks["Binance"]:
                    if trade_id in self.processed_trade_ids["Binance"]:
                        logger.debug(f"Skipping duplicate Binance trade ID: {trade_id}")
                        return
                    if entry_key in self.processed_entries["Binance"]:
                        logger.debug(f"Skipping duplicate Binance entry: {entry_key}")
                        return
                    self.processed_trade_ids["Binance"].append(trade_id)
                    self.processed_entries["Binance"].append(entry_key)

                logger.debug(f"Binance data received: {fetch_time}, {price}, trade_id: {trade_id}")
                self._write_to_csv("Binance", fetch_time, price)
                self.on_price_update("ETHUSDT", {"Binance": price}, fetch_time)
        except Exception as e:
            logger.error(f"Error processing Binance message: {e}")

    def _on_coinbase_message(self, ws, message):
        """Handle Coinbase WebSocket messages."""
        try:
            logger.debug(f"Raw Coinbase message: {message}")
            data = json.loads(message)
            if data.get("type") == "match":
                trade_id = data["trade_id"]
                price = float(data["price"])
                timestamp_str = data["time"]
                fetch_time = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
                entry_key = (fetch_time, price)

                with self.dedupe_locks["Coinbase"]:
                    if trade_id in self.processed_trade_ids["Coinbase"]:
                        logger.debug(f"Skipping duplicate Coinbase trade ID: {trade_id}")
                        return
                    if entry_key in self.processed_entries["Coinbase"]:
                        logger.debug(f"Skipping duplicate Coinbase entry: {entry_key}")
                        return
                    self.processed_trade_ids["Coinbase"].append(trade_id)
                    self.processed_entries["Coinbase"].append(entry_key)

                logger.debug(f"Coinbase data received: {fetch_time}, {price}, trade_id: {trade_id}")
                self._write_to_csv("Coinbase", fetch_time, price)
                self.on_price_update("ETHUSDT", {"Coinbase": price}, fetch_time)
        except Exception as e:
            logger.error(f"Error processing Coinbase message: {e}")

    def _on_error(self, ws, error, source):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error from {source}: {error}")
        self.on_disconnect(source)

    def _on_close(self, ws, close_status_code, close_msg, source):
        """Handle WebSocket closure."""
        logger.warning(f"WebSocket closed for {source}: {close_status_code} - {close_msg}")
        self.disconnect_times[source] = datetime.now(tz=pytz.UTC)
        self.on_disconnect(source)
        if self.running:
            logger.info(f"Reconnecting to {source}...")
            time.sleep(5)
            self._start_websocket(source)

    def _on_open(self, ws, source):
        """Handle WebSocket opening."""
        logger.info(f"WebSocket opened for {source}")
        self.reconnect_times[source] = datetime.now(tz=pytz.UTC)

        disconnect_time = self.disconnect_times[source]
        reconnect_time = self.reconnect_times[source]
        if disconnect_time and reconnect_time:
            self._fill_missed_data(source, "ETHUSDT", disconnect_time, reconnect_time)

        if source == "Coinbase":
            subscription = {
                "type": "subscribe",
                "product_ids": ["ETH-USD"],
                "channels": ["matches"]
            }
            ws.send(json.dumps(subscription))
            logger.info(f"Sending Coinbase subscription: {subscription}")

    def _start_websocket(self, source):
        """Start a WebSocket connection for a specific source."""
        if source == "Binance":
            ws_url = "wss://stream.binance.com:9443/ws/ethusdt@trade"
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=self._on_binance_message,
                on_error=lambda ws, error: self._on_error(ws, error, "Binance"),
                on_close=lambda ws, code, msg: self._on_close(ws, code, msg, "Binance"),
                on_open=lambda ws: self._on_open(ws, "Binance")
            )
        elif source == "Coinbase":
            ws_url = "wss://ws-feed.exchange.coinbase.com"
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=self._on_coinbase_message,
                on_error=lambda ws, error: self._on_error(ws, error, "Coinbase"),
                on_close=lambda ws, code, msg: self._on_close(ws, code, msg, "Coinbase"),
                on_open=lambda ws: self._on_open(ws, "Coinbase")
            )
        else:
            raise ValueError(f"Unsupported source: {source}")

        self.ws_apps[source] = ws
        thread = threading.Thread(target=ws.run_forever)
        thread.daemon = True
        thread.start()
        self.ws_threads[source] = thread
        logger.info(f"Started WebSocket thread for {source}")

    def start(self, symbol, sources=None):
        """Start WebSocket connections for the specified sources in parallel."""
        self.running = True
        if sources is None:
            sources = ["Binance", "Coinbase"]
        else:
            valid_sources = ["Binance", "Coinbase"]
            for source in sources:
                if source not in valid_sources:
                    raise ValueError(f"Invalid source: {source}. Valid sources are {valid_sources}")

        threads = []
        for source in sources:
            thread = threading.Thread(target=self._start_websocket, args=(source,))
            threads.append(thread)
            thread.start()
            logger.info(f"Initiated WebSocket connection for {source}")

        for thread in threads:
            thread.join(0.1)

    def stop(self):
        """Stop all WebSocket connections."""
        self.running = False
        for source, ws in self.ws_apps.items():
            ws.close()
        for source, thread in self.ws_threads.items():
            thread.join()
        logger.info("All WebSocket threads stopped.")