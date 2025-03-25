# src/main.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import sqlite3
from src.price_engine.engine import PriceEngine

def main():
    parser = argparse.ArgumentParser(description="Crypto Trading Engine")
    parser.add_argument("--mode", choices=["track", "display-live", "display-historical"], default="track", help="Mode to run the system in")
    parser.add_argument("--symbol", default="ETHUSDT", help="Symbol to track")
    parser.add_argument("--interval", type=int, default=5, help="Interval in seconds for price updates")
    parser.add_argument("--use-db", action="store_true", help="Use database to display prices (for display modes)")
    parser.add_argument("--start-date", help="Start date for historical data (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-date", help="End date for historical data (YYYY-MM-DD HH:MM:SS)")
    args = parser.parse_args()

    db_path = "data/prices.db"
    engine = PriceEngine(db_path)

    # Create the live_prices and historical_prices tables if they don't exist
    with sqlite3.connect(db_path) as conn:
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
                timestamp TEXT NOT NULL,
                price REAL NOT NULL
            )
        """)
        conn.commit()

    try:
        if args.mode == "track":
            engine.track_live_prices(args.symbol, args.interval)
        elif args.mode == "display-live":
            engine.display_live_prices(args.symbol, use_db=args.use_db)
        elif args.mode == "display-historical":
            if not args.start_date or not args.end_date:
                print("Error: --start-date and --end-date are required for display-historical mode.")
                sys.exit(1)
            engine.display_historical_prices(args.symbol, args.start_date, args.end_date, use_db=args.use_db)
    except KeyboardInterrupt:
        print("Shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()