# src/main.py
import argparse
import sys
import os
from datetime import datetime, timedelta

# Add project_root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.price_engine.engine import PriceEngine
from src.api_clients.coingecko_api import CoinGeckoAPI

def fetch_historical_prices(symbol: str, from_date: str, to_date: str) -> list:
    """Fetch historical prices for the specified date range."""
    coingecko = CoinGeckoAPI()
    coin_id = "bitcoin" if symbol == "BTCUSDT" else "ethereum"  # Map symbol to CoinGecko ID
    historical_prices = []

    # Convert date strings to datetime objects
    start_date = datetime.strptime(from_date, "%Y-%m-%d")
    end_date = datetime.strptime(to_date, "%Y-%m-%d")

    # Fetch prices for each date in the range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d-%m-%Y")  # CoinGecko format
        try:
            price = coingecko.get_historical_price(coin_id, date_str)
            historical_prices.append({
                "timestamp": current_date.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "source": "coingecko",
                "price": price,
            })
        except Exception as e:
            print(f"Failed to fetch historical price for {date_str}: {e}")
        current_date += timedelta(days=1)  # Move to the next day

    return historical_prices

def fetch_historical_daily_prices(symbol, from_date, to_date):
    """Fetch historical daily prices for the specified date range from CoinGecko."""
    coingecko = CoinGeckoAPI()
    coin_id = "bitcoin" if symbol == "BTCUSDT" else "ethereum"  # Map symbol to CoinGecko ID
    historical_prices = []

    # Convert date strings to datetime objects
    try:
        start_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"Invalid date format. Use 'YYYY-MM-DD'. Error: {e}")
        sys.exit(1)

    # Validate date range
    if start_date > end_date:
        print("Error: Start date must be before end date.")
        sys.exit(1)

    # Fetch prices for each date in the range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d-%m-%Y")  # CoinGecko format
        try:
            price = coingecko.get_historical_price(coin_id, date_str)
            if price is not None:
                historical_prices.append({
                    "timestamp": current_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "source": "CoinGecko",
                    "price": price,
                })
            else:
                print(f"No historical price available for {date_str}.")
        except Exception as e:
            print(f"Failed to fetch historical price for {date_str}: {e}")
        current_date += timedelta(days=1)  # Move to the next day

    return historical_prices

def display_historical_daily_prices(historical_prices):
    """Display historical daily prices in a table format."""
    if not historical_prices:
        print("No historical data available for the specified date range.")
        return

    print("\nHistorical Daily Prices (CoinGecko):")
    print("-----------------------------------")
    print("Timestamp".ljust(20), "Symbol".ljust(10), "Source".ljust(10), "Price")
    print("-----------------------------------")
    for entry in historical_prices:
        timestamp = entry["timestamp"]
        symbol = entry["symbol"]
        source = entry["source"]
        price = entry["price"]
        print(f"{timestamp.ljust(20)} {symbol.ljust(10)} {source.ljust(10)} {price:.2f}")

def run_historical_mode(symbol: str, from_date: str, to_date: str):
    """Fetch and display historical prices for the specified date range."""
    historical_prices = fetch_historical_prices(symbol, from_date, to_date)

    # Display historical prices in a table
    if historical_prices:
        print("\nHistorical Daily Prices (CoinGecko):")
        print("-----------------------------------")
        print("Timestamp".ljust(20), "Symbol".ljust(10), "Source".ljust(10), "Price")
        print("-----------------------------------")
        for entry in historical_prices:
            timestamp = entry["timestamp"]
            symbol = entry["symbol"]
            source = entry["source"]
            price = entry["price"]
            print(f"{timestamp.ljust(20)} {symbol.ljust(10)} {source.ljust(10)} {price:.2f}")
    else:
        print("\nNo historical prices available for the specified date range.")

def display_query_results(results):
    """Display the query results in a table format."""
    if not results:
        print("No data found in the database for the specified date range.")
        return

    print("\nWeighted Average Prices from Database:")
    print("-----------------------------------")
    print("Timestamp".ljust(20), "Weighted Average Price")
    print("-----------------------------------")
    for entry in results:
        timestamp = entry["timestamp"]
        weighted_avg = entry["weighted_avg"]
        print(f"{timestamp.ljust(20)} {weighted_avg:.2f}")

def main():
    parser = argparse.ArgumentParser(description="Crypto Price Engine")
    parser.add_argument("--mode", choices=["live", "historical", "display-historical-daily", "query-db"], default="live",
                        help="Mode to run the engine in: 'live' for real-time tracking, 'historical' for daily historical data, 'display-historical-daily' for daily historical data from CoinGecko, 'query-db' to query weighted averages from the database")
    parser.add_argument("--symbol", default="ETHUSDT", help="Trading pair symbol (e.g., ETHUSDT)")
    parser.add_argument("--sources", nargs='+', default=None,
                        help="List of sources to fetch data from (e.g., Binance Coinbase CoinGecko). If not specified, all sources are used. (Not used in historical mode)")
    parser.add_argument("--start-date", help="Start date for historical or query data (format: 'YYYY-MM-DD HH:MM:SS' for query-db, 'YYYY-MM-DD' for historical and display-historical-daily)")
    parser.add_argument("--end-date", help="End date for historical or query data (format: 'YYYY-MM-DD HH:MM:SS' for query-db, 'YYYY-MM-DD' for historical and display-historical-daily)")

    args = parser.parse_args()

    if args.mode == "live":
        engine = PriceEngine()
        engine.track_live_prices(args.symbol, sources=args.sources)
    elif args.mode == "historical":
        if not args.start_date or not args.end_date:
            print("Error: --start-date and --end-date are required for historical mode.")
            sys.exit(1)
        # Extract only the date part (YYYY-MM-DD) for historical mode
        try:
            start_date = args.start_date.split()[0]
            end_date = args.end_date.split()[0]
        except IndexError:
            print("Error: Dates must be in 'YYYY-MM-DD' format for historical mode.")
            sys.exit(1)
        run_historical_mode(args.symbol, start_date, end_date)
    elif args.mode == "display-historical-daily":
        if not args.start_date or not args.end_date:
            print("Error: --start-date and --end-date are required for display-historical-daily mode.")
            sys.exit(1)
        # Extract only the date part (YYYY-MM-DD) for daily mode
        try:
            start_date = args.start_date.split()[0]
            end_date = args.end_date.split()[0]
        except IndexError:
            print("Error: Dates must be in 'YYYY-MM-DD' format for display-historical-daily mode.")
            sys.exit(1)
        historical_prices = fetch_historical_daily_prices(args.symbol, start_date, end_date)
        display_historical_daily_prices(historical_prices)
    elif args.mode == "query-db":
        if not args.start_date or not args.end_date:
            print("Error: --start-date and --end-date are required for query-db mode.")
            sys.exit(1)
        engine = PriceEngine()
        results = engine.query_weighted_avg_prices(args.start_date, args.end_date)
        display_query_results(results)

if __name__ == "__main__":
    main()