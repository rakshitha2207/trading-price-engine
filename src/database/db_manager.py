import sqlite3
from datetime import datetime
from typing import Dict, Optional

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._create_tables()

    def _create_tables(self):
        """Create the necessary tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS live_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historical_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    source TEXT NOT NULL,
                    date TEXT NOT NULL,
                    price REAL NOT NULL
                )
            """)
            conn.commit()

    def save_live_price(self, symbol: str, price: float, timestamp: Optional[datetime] = None):
        """Save a live price to the database."""
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO live_prices (symbol, price, timestamp) VALUES (?, ?, ?)",
                (symbol, price, timestamp_str)
            )
            conn.commit()

    def save_historical_price(self, symbol: str, source: str, date: str, price: float):
        """Save a historical price to the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO historical_prices (symbol, source, date, price) VALUES (?, ?, ?, ?)",
                (symbol, source, date, price)
            )
            conn.commit()

    def get_live_prices(self, symbol: str) -> Dict[str, float]:
        """Retrieve the latest live prices for a symbol."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT price, timestamp FROM live_prices WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
                (symbol,)
            )
            result = cursor.fetchone()
            if result:
                return {"Weighted Avg": result[0], "Timestamp": result[1]}
            return {}

    def get_live_prices_in_range(self, symbol: str, start_time: datetime, end_time: datetime) -> Dict[str, float]:
        """Retrieve live prices for a symbol within a time range."""
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT price, timestamp FROM live_prices WHERE symbol = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
                (symbol, start_time_str, end_time_str)
            )
            results = cursor.fetchall()
            return {row[1]: row[0] for row in results}

    def get_historical_prices(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Dict[str, float]]:
        """Retrieve historical prices for a symbol within a date range."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT source, date, price FROM historical_prices WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date",
                (symbol, start_date, end_date)
            )
            results = cursor.fetchall()

            historical_data = {}
            for source, date, price in results:
                if source not in historical_data:
                    historical_data[source] = {}
                historical_data[source][date] = price
            return historical_data