# Crypto Data Collector ( Free and Unlimited )

A comprehensive Python-based crypto market data collector that fetches historical and live OHLCV (Open, High, Low, Close, Volume) candlestick data from multiple cryptocurrency exchanges. It supports various intervals and symbols, storing data efficiently in Parquet format for easy analysis.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Supported Exchanges](#supported-exchanges)
- [Supported Symbols and Intervals](#supported-symbols-and-intervals)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Storage Structure](#data-storage-structure)
- [Architecture and Design](#architecture-and-design)
- [Extending the Collector](#extending-the-collector)
- [Troubleshooting and Logging](#troubleshooting-and-logging)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Overview

The Crypto Data Collector is designed to automate the collection of cryptocurrency market data from several major exchanges. It retrieves spot market OHLCV data for multiple trading pairs and intervals, backfills historical data, and continues with live data collection aligned to precise interval boundaries.

Data is saved in Parquet files organized by symbol, interval, exchange, and date, enabling efficient storage and retrieval for analytics, backtesting, or machine learning workflows.

## Features

- **Multi-exchange support:** Coinbase, Bitstamp, Bitfinex, Kucoin, Binance.US
- **Multiple intervals:** 15m, 1h, 4h, 6h, 1d (configurable)
- **Historical backfill:** Automatically backfills data from configurable start times
- **Live data collection:** Runs continuously, collecting new data at exact interval boundaries
- **Rate limiting:** Respects exchange API rate limits with configurable delays
- **Robust error handling:** Logs errors and continues operation without crashing
- **Data storage:** Saves data in Parquet format, grouped by date, exchange, symbol, and interval
- **Extensible architecture:** Easily add new exchanges or data types (derivatives, options)

## Supported Exchanges

| Exchange    | Spot Market | Perpetual Futures | Options |
|-------------|-------------|-------------------|---------|
| Coinbase    | Yes         | No                | No      |
| Bitstamp    | Yes         | No                | No      |
| Bitfinex    | Yes         | No                | No      |
| Kucoin      | Yes         | No (via this API) | No      |
| Binance.US  | Yes         | No                | No      |

## Supported Symbols and Intervals

### Symbols

Default symbols collected:

- BTC-USDT
- ETH-USDT
- SOL-USDT
- DOGE-USDT
- SUI-USDT
- XRP-USDT

*Note:* Some exchanges do not support USDT pairs natively and will convert USDT to USD internally.

### Intervals

- 15 minutes (`15m`)
- 1 hour (`1h`)
- 4 hours (`4h`)
- 6 hours (`6h`)
- 1 day (`1d`)

## Installation

### Prerequisites

- Python 3.8 or higher
- `pip` package manager

### Required Python Packages

Install dependencies with:

```bash
pip install pandas requests python-dotenv pyarrow
```

- `pandas` and `pyarrow` for Parquet file handling
- `requests` for HTTP API calls
- `python-dotenv` to load environment variables from `.env`

## Configuration

### Environment Variables

Create a `.env` file in the project root to store API keys securely:

```ini
COINBASE_API_KEY=your_coinbase_api_key
BITSTAMP_API_KEY=your_bitstamp_api_key
BITFINEX_API_KEY=your_bitfinex_api_key
KUCOIN_API_KEY=your_kucoin_api_key
BINANCEUS_API_KEY=your_binanceus_api_key
```

*Note:* API keys are optional for public endpoints but recommended if you want to increase rate limits or access private data.

## Usage

Run the collector with:

```bash
python your_script_name.py
```

The collector will:

1. Backfill historical data for all configured symbols and intervals.
2. Start live data collection aligned to 15-minute interval boundaries.
3. Save all collected data in Parquet files under the `data/crypto` directory.

### Stopping the Collector

- Press `Ctrl+C` to stop gracefully.

## Data Storage Structure

Data is stored locally under the `data/crypto` directory with the following hierarchy:

```
data/
└── crypto/
    └── /
        └── /
            └── /          # spot, derivatives, options (currently derivatives/options empty)
                └── /
                    └── __.parquet
```

- **symbol:** Trading pair, e.g., `BTC-USDT`
- **interval:** Candlestick interval, e.g., `15m`
- **data_type:** Type of data, currently only `spot` populated
- **exchange_name:** Exchange identifier, e.g., `coinbase`
- **file:** Parquet file containing that day's data for the symbol, interval, and exchange

Files are appended with new data and duplicates are removed to maintain clean datasets.

## Architecture and Design

- **ExchangeAPI Base Class:** Defines the interface and common functionality for all exchange API wrappers.
- **Exchange Implementations:** Each exchange (Coinbase, Bitstamp, etc.) extends `ExchangeAPI` and implements data fetching with exchange-specific API details.
- **CryptoCollector:** Orchestrates data collection across all exchanges, symbols, and intervals.
- **Asynchronous Workflow:** Uses `asyncio` to manage concurrent data fetching and timed live data collection.
- **Rate Limiting:** Implements delays between API calls to respect exchange limits.
- **Data Persistence:** Uses `pandas` and `pyarrow` to save data efficiently in Parquet format.
- **Time Alignment:** Live collection aligns precisely to interval boundaries (e.g., every 15 minutes).

## Extending the Collector

To add support for new exchanges or data types:

1. Create a new class inheriting from `ExchangeAPI`.
2. Implement the required methods:
   - `get_spot_ohlcv`
   - `get_perpetual_ohlcv` (if supported)
   - `get_options_data` (if supported)
3. Add your exchange instance to the `self.exchanges` list in `CryptoCollector`.
4. Update symbol formatting or interval mapping if needed.

## Troubleshooting and Logging

- Logs are printed to the console with timestamps, log levels, and messages.
- Errors during API calls or data processing are logged but do not stop the collector.
- Ensure your system clock is accurate to avoid timing issues.
- Check API key validity and rate limits if you encounter repeated errors.

## Acknowledgments

- Thanks to the open API providers for making market data accessible.
- Inspired by best practices in crypto data collection and time series data management.

*Happy Data Collecting!* 🚀📈