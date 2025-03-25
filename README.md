# Crypto Trading Engine

A Python-based cryptocurrency trading system that tracks live and historical prices, handles disconnections, and fills missing data. This project fetches real-time price data from multiple exchanges (Binance, Coinbase, CoinGecko), stores it in a SQLite database, and provides functionalities to display live and historical prices.

## Features

- **Live Price Tracking**: Fetches real-time prices at a user-defined interval.
- **Disconnection Handling**: Detects network failures and fills missing data.
- **Historical Price Retrieval**: Queries past price data from supported exchanges.
- **Database Storage**: Saves price data in SQLite (`data/prices.db`).
- **Logging**: Logs operations in `trading_engine.log`.

## Project Structure

```
crypto_price_engine/
├── src/
│   ├── main.py                  # Application entry point
│   ├── price_engine/
│   │   ├── engine.py            # Core logic for price tracking
│   │   ├── binance_api.py       # Binance API integration
│   │   ├── coinbase_api.py      # Coinbase API integration
│   │   ├── coingecko_api.py     # CoinGecko API integration
│   ├── database/
│   │   ├── db_manager.py        # SQLite database manager
├── data/
│   ├── prices.db                # SQLite database
├── trading_engine.log           # Log file
├── README.md                    # Documentation
```

## Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/rakshitha2207/trading-price-engine.git
   cd Automated-Trading-Agent
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Track Live Prices
```bash
python -m src.main --mode track --symbol ETHUSDT --interval 5
```

### Display Live Prices
```bash
python -m src.main --mode display-live --symbol ETHUSDT
```

### Display Historical Prices
```bash
python -m src.main --mode display-historical --symbol ETHUSDT --start-date "2025-03-25 13:05:00" --end-date "2025-03-25 13:06:00"
```

## Troubleshooting

- **ModuleNotFoundError**: Run the command from the project root using `python -m src.main`.
- **No Data Available**: Ensure the exchange provides historical data.
- **Database Issues**: If `prices.db` is missing or corrupted, delete it and rerun the application.

## Future Improvements

- WebSocket support for real-time streaming.
- Expanded exchange support.
- Automated trading strategies.

## Contributing

Fork the repository, make your changes, and submit a pull request.

## License

MIT License.
