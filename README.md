# Crypto Price Engine

A Python-based application for tracking cryptocurrency prices in real-time, fetching historical price data, calculating weighted average prices, storing them in an SQLite database, and querying the stored data. The project supports multiple data sources, including Binance, Coinbase, and CoinGecko, and provides a command-line interface for various modes of operation.

## Features

- **Live Price Tracking**: Track real-time prices for a specified trading pair (e.g., `ETHUSDT`) using WebSocket connections from Binance and Coinbase.
- **Historical Data Fetching**: Fetch daily historical prices from CoinGecko for a specified date range.
- **Weighted Average Calculation**: Calculate per-second weighted average prices from multiple sources (Binance, Coinbase, CoinGecko) with configurable weights.
- **Data Storage**: Store weighted average prices in an SQLite database (`crypto_prices.db`) for persistence.
- **Database Querying**: Query the stored weighted average prices from the database within a specified date range.
- **Logging**: Comprehensive logging to both console and a log file (`trading_engine.log`).

## Project Structure

```
CRYPTO_PRICE_ENGINE/
├── data/
│   ├── binance_data.csv        # Stores live price data from Binance
│   ├── coinbase_data.csv       # Stores live price data from Coinbase
│   └── final_weighted_avg.csv  # Stores calculated weighted average prices
├── src/
│   ├── api_clients/
│   │   ├── __init__.py
│   │   ├── coingecko_api.py    # CoinGecko API client for historical data
│   │   └── websocket_client.py # WebSocket client for live price tracking
│   ├── price_engine/
│   │   ├── __init__.py
│   │   ├── engine.py           # Core price engine logic
│   │   └── main.py             # Entry point for the application
│   └── __pycache__/
├── crypto_prices.db            # SQLite database for storing weighted averages
├── trading_engine.log          # Log file for application logs
├── README.md                   # Project documentation
├── requirements.txt            # Project dependencies
```

## Prerequisites

- **Python 3.8+**: Ensure Python is installed on your system.
- **Dependencies**: Install the required Python packages listed in `requirements.txt`.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/rakshitha2207/trading-price-engine.git
   cd CRYPTO_PRICE_ENGINE
   ```

2. **Set Up a Virtual Environment (optional but recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### `requirements.txt`:
```text
pandas
requests
websocket-client
pytz
```

## Usage

The application is run via the command line using `src/main.py`. It supports multiple modes of operation, each with specific arguments.

### Available Modes

#### 1. **Live Mode**
Track real-time prices for a specified trading pair and store them in CSV files. On interruption (Ctrl+C), calculate weighted averages and store them in an SQLite database.

**Command:**
```bash
python -m src.main --mode live --symbol ETHUSDT
```
**Arguments:**
- `--symbol`: Trading pair symbol (default: ETHUSDT).
- `--sources`: List of sources (e.g., Binance Coinbase). If not specified, all sources are used.

#### 2. **Historical Mode**
Fetch daily historical prices for a specified symbol from CoinGecko and display them.

**Command:**
```bash
python -m src.main --mode historical --symbol BTCUSDT --start-date "2024-03-25" --end-date "2024-03-27"
```
**Arguments:**
- `--symbol`: Trading pair symbol (default: ETHUSDT).
- `--start-date`: Start date in YYYY-MM-DD format.
- `--end-date`: End date in YYYY-MM-DD format.

#### 3. **Display Historical Daily Mode**
Fetch daily historical prices for a specified symbol from CoinGecko and display them.

**Command:**
```bash
python -m src.main --mode display-historical-daily --symbol ETHUSDT --start-date "2024-03-25" --end-date "2024-03-27"
```

#### 4. **Query Database Mode**
Query the weighted average prices stored in the SQLite database within a specified date range.

**Command:**
```bash
python -m src.main --mode query-db --start-date "2025-03-25 20:11:25" --end-date "2025-03-25 20:11:30"
```

## Contributing

Contributions are welcome! Please fork the repository, create a new branch, and submit a pull request with your changes.

1. Fork the repository.
2. Create a new branch:
   ```bash
   git checkout -b feature/your-feature
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add your feature"
   ```
4. Push to the branch:
   ```bash
   git push origin feature/your-feature
   ```
5. Open a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.