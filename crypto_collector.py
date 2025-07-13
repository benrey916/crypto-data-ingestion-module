import os
import asyncio
import logging
import time
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import requests
import sys
import math

# Load environment variables
load_dotenv()

# Set Windows event loop policy for aiodns compatibility
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Shared volume configuration
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExchangeAPI:
    """Base class for exchange API integrations"""
    def __init__(self, name, base_url):
        self.name = name
        self.base_url = base_url
        self.api_key = os.getenv(f"{name.upper()}_API_KEY")
        logger.info(f"Initialized {name} API")
        self.rate_limit_delay = 1.0  # Reduced delay for bulk requests

    def _rate_limit(self):
        """Add delay before requests to avoid rate limiting"""
        time.sleep(self.rate_limit_delay)
    
    def get_spot_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=300):
        """Get spot OHLCV data (to be implemented by subclasses)"""
        raise NotImplementedError
    
    def get_perpetual_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=None):
        """Get perpetual futures OHLCV data (to be implemented by subclasses)"""
        raise NotImplementedError
    
    def get_options_data(self, symbol):
        """Get options data"""
        raise NotImplementedError

class CoinbaseAPI(ExchangeAPI):
    """Coinbase exchange API implementation"""
    def __init__(self):
        super().__init__(
            "coinbase",
            "https://api.exchange.coinbase.com"
        )
        self.max_candles_per_request = 300  # Coinbase's maximum

    def get_spot_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=300):
        self._rate_limit()
        # Map interval to Coinbase granularity (in seconds)
        interval_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "1d": 86400
        }
        granularity = interval_map.get(interval)
        if not granularity:
            logger.warning(f"Unsupported interval {interval} for Coinbase")
            return []
        
        # Calculate actual limit (capped at max_candles_per_request)
        actual_limit = min(limit, self.max_candles_per_request) if limit else self.max_candles_per_request
        
        # Convert timestamps to ISO 8601 format
        start_iso = end_iso = None
        if starttime:
            start_dt = datetime.fromtimestamp(starttime/1000, tz=timezone.utc)
            start_iso = start_dt.isoformat()
        if endtime:
            end_dt = datetime.fromtimestamp(endtime/1000, tz=timezone.utc)
            end_iso = end_dt.isoformat()
        
        params = {
            "granularity": granularity,
            "limit": actual_limit
        }
        if start_iso:
            params["start"] = start_iso
        if end_iso:
            params["end"] = end_iso
            
        try:
            # Format symbol for Coinbase (BTC-USDT -> BTC-USD)
            parts = symbol.split('-')
            if len(parts) == 2:
                base, quote = parts
                if quote == 'USDT':
                    quote = 'USD'
                coinbase_symbol = f"{base}-{quote}"
            else:
                coinbase_symbol = symbol.replace("USDT", "USD")
                
            url = f"{self.base_url}/products/{coinbase_symbol}/candles"
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                # Reverse to chronological order (oldest first)
                candles_data = list(reversed(response.json()))
                candles = []
                for kline in candles_data:
                    # Coinbase format: [timestamp, low, high, open, close, volume]
                    timestamp = datetime.fromtimestamp(kline[0], tz=timezone.utc)
                    candles.append({
                        "open": float(kline[3]),
                        "high": float(kline[2]),
                        "low": float(kline[1]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "timestamp": timestamp.isoformat(),
                        "symbol": symbol  # Keep original symbol format
                    })
                logger.info(f"Retrieved {len(candles)} {symbol} {interval} candles from Coinbase")
                return candles
            else:
                logger.error(f"Coinbase API error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Coinbase spot error for {symbol}: {str(e)}")
        return []
    
    def get_perpetual_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=None):
        """Coinbase doesn't support perpetual futures - return empty list"""
        return []
    
    def get_options_data(self, symbol):
        """Coinbase doesn't support options - return None"""
        return None

class BitstampAPI(ExchangeAPI):
    """Bitstamp exchange API implementation"""
    def __init__(self):
        super().__init__(
            "bitstamp",
            "https://www.bitstamp.net/api/v2"
        )
        self.max_candles_per_request = 300  # Bitstamp's maximum

    def get_spot_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=300):
        self._rate_limit()
        # Map interval to Bitstamp step (in seconds)
        interval_map = {
            "15m": 900,
            "1h": 3600,
            "4h": 14400,  # Added 4-hour interval
            "6h": 21600,
            "1d": 86400
        }
        step = interval_map.get(interval)
        if not step:
            logger.warning(f"Unsupported interval {interval} for Bitstamp")
            return []
        
        # Calculate actual limit
        actual_limit = min(limit, self.max_candles_per_request) if limit else self.max_candles_per_request
        
        # Convert symbol to Bitstamp format (BTC-USDT -> btcusd)
        parts = symbol.split('-')
        if len(parts) == 2:
            base, quote = parts
            # Use USD instead of USDT
            if quote == "USDT":
                quote = "USD"
            bitstamp_symbol = f"{base.lower()}{quote.lower()}"
        else:
            bitstamp_symbol = symbol.lower().replace("usdt", "usd")
        
        # Prepare parameters
        params = {
            "step": step,
            "limit": actual_limit
        }
        if starttime:
            params["start"] = starttime // 1000  # Convert to seconds
        if endtime:
            params["end"] = endtime // 1000      # Convert to seconds
            
        try:
            url = f"{self.base_url}/ohlc/{bitstamp_symbol}/"
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                candles = []
                for item in data.get("data", {}).get("ohlc", []):
                    # Convert timestamp to datetime
                    timestamp = datetime.fromtimestamp(int(item["timestamp"]), tz=timezone.utc)
                    candles.append({
                        "open": float(item["open"]),
                        "high": float(item["high"]),
                        "low": float(item["low"]),
                        "close": float(item["close"]),
                        "volume": float(item["volume"]),
                        "timestamp": timestamp.isoformat(),
                        "symbol": symbol  # Keep original symbol
                    })
                logger.info(f"Retrieved {len(candles)} {symbol} {interval} candles from Bitstamp")
                return candles
            else:
                logger.error(f"Bitstamp API error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Bitstamp spot error for {symbol}: {str(e)}")
        return []
    
    def get_perpetual_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=None):
        """Bitstamp doesn't support perpetual futures - return empty list"""
        return []

class BitfinexAPI(ExchangeAPI):
    """Bitfinex exchange API implementation"""
    def __init__(self):
        super().__init__(
            "bitfinex",
            "https://api-pub.bitfinex.com/v2"
        )
        self.max_candles_per_request = 300  # Bitfinex's maximum

    def get_spot_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=300):
        self._rate_limit()
        # Map interval to Bitfinex timeframe
        interval_map = {
            "15m": "15m",
            "1h": "1h",
            "6h": "6h",
            "1d": "1D"
        }
        timeframe = interval_map.get(interval)
        if not timeframe:
            logger.warning(f"Unsupported interval {interval} for Bitfinex")
            return []
        
        # Calculate actual limit
        actual_limit = min(limit, self.max_candles_per_request) if limit else self.max_candles_per_request
        
        # Convert symbol to Bitfinex format (BTC-USDT -> tBTCUST)
        parts = symbol.split('-')
        if len(parts) == 2:
            base, quote = parts
            # Use UST for USDT pairs
            if quote == "USDT":
                quote = "USD"
            bitfinex_symbol = f"t{base}{quote}"
        else:
            bitfinex_symbol = f"t{symbol.replace('-', '')}".replace("USDT", "USD")
        
        # Prepare parameters
        params = {
            "limit": actual_limit,
            "sort": 1
        }
        if starttime:
            params["start"] = starttime
        if endtime:
            params["end"] = endtime
            
        try:
            url = f"{self.base_url}/candles/trade:{timeframe}:{bitfinex_symbol}/hist"
            response = requests.get(url, params=params)
            if response.status_code == 200:
                candles_data = response.json()
                candles = []
                for kline in candles_data:
                    # Bitfinex format: [timestamp, open, close, high, low, volume]
                    timestamp = datetime.fromtimestamp(kline[0]/1000, tz=timezone.utc)
                    candles.append({
                        "open": kline[1],
                        "high": kline[3],
                        "low": kline[4],
                        "close": kline[2],
                        "volume": kline[5],
                        "timestamp": timestamp.isoformat(),
                        "symbol": symbol  # Keep original symbol
                    })
                logger.info(f"Retrieved {len(candles)} {symbol} {interval} candles from Bitfinex")
                return candles
            else:
                logger.error(f"Bitfinex API error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Bitfinex spot error for {symbol}: {str(e)}")
        return []
    
    def get_perpetual_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=None):
        """Bitfinex doesn't support perpetual futures - return empty list"""
        return []

class KucoinAPI(ExchangeAPI):
    """Kucoin exchange API implementation"""
    def __init__(self):
        super().__init__(
            "kucoin",
            "https://api.kucoin.com"
        )
        self.max_candles_per_request = 300

    def get_spot_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=300):
        self._rate_limit()
        # Map interval to Kucoin type
        interval_map = {
            "15m": "15min",
            "1h": "1hour",
            "4h": "4hour",  # Added 4-hour interval
            "6h": "6hour",
            "1d": "1day"
        }
        kucoin_interval = interval_map.get(interval)
        if not kucoin_interval:
            logger.warning(f"Unsupported interval {interval} for Kucoin")
            return []
        
        # Calculate actual limit
        actual_limit = min(limit, self.max_candles_per_request) if limit else self.max_candles_per_request
        
        # Convert timestamps to seconds (Kucoin uses seconds)
        start_sec = starttime // 1000 if starttime else None
        end_sec = endtime // 1000 if endtime else None
        
        # Kucoin requires both start and end times to be provided
        if not start_sec or not end_sec:
            # If not provided, use default window (last 1500 candles)
            if not end_sec:
                end_sec = int(time.time())
            if not start_sec:
                start_sec = end_sec - (self.get_interval_seconds(interval) * actual_limit)
        
        params = {
            "symbol": symbol.replace('-', '-'),  # Ensure correct symbol format
            "type": kucoin_interval,
            "startAt": start_sec,
            "endAt": end_sec
        }
        
        try:
            url = f"{self.base_url}/api/v1/market/candles"
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data["code"] == "200000":
                    candles_data = data["data"]
                    candles = []
                    # Kucoin returns newest first, so reverse for chronological order
                    for kline in reversed(candles_data):
                        # Format: [timestamp, open, close, high, low, volume, turnover]
                        timestamp = datetime.fromtimestamp(int(kline[0]), tz=timezone.utc)
                        candles.append({
                            "open": float(kline[1]),
                            "high": float(kline[3]),
                            "low": float(kline[4]),
                            "close": float(kline[2]),
                            "volume": float(kline[5]),
                            "timestamp": timestamp.isoformat(),
                            "symbol": symbol
                        })
                    logger.info(f"Retrieved {len(candles)} {symbol} {interval} candles from Kucoin")
                    return candles
                else:
                    logger.error(f"Kucoin API error: {data['msg']}")
            else:
                logger.error(f"Kucoin API error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Kucoin spot error for {symbol}: {str(e)}")
        return []
    
    def get_perpetual_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=None):
        """Kucoin doesn't support perpetual futures through this endpoint - return empty list"""
        return []
    
    def get_interval_seconds(self, interval):
        """Get interval duration in seconds"""
        conversions = {
            "15m": 900,
            "1h": 3600,
            "4h": 14400,  # Added 4-hour interval
            "6h": 21600,
            "1d": 86400
        }
        return conversions.get(interval, 900)
    
class BinanceUSAPI(ExchangeAPI):
    """Binance.US exchange API implementation"""
    def __init__(self):
        super().__init__(
            "binanceus",
            "https://api.binance.us"
        )
        self.max_candles_per_request = 300 
        self.rate_limit_delay = 0.5

    def get_spot_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=300):
        self._rate_limit()
        # Map interval to Binance format
        interval_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "1h": "1h",
            "4h": "4h",
            "6h": "6h",
            "1d": "1d"
        }
        binance_interval = interval_map.get(interval)
        if not binance_interval:
            logger.warning(f"Unsupported interval {interval} for Binance.US")
            return []
        
        # Calculate actual limit
        actual_limit = min(limit, self.max_candles_per_request) if limit else self.max_candles_per_request
        
        # Convert symbol to Binance format (BTC-USDT -> BTCUSDT)
        binance_symbol = symbol.replace('-', '')
        
        params = {
            "symbol": binance_symbol,
            "interval": binance_interval,
            "limit": actual_limit
        }
        if starttime:
            params["startTime"] = starttime
        if endtime:
            params["endTime"] = endtime
            
        try:
            url = f"{self.base_url}/api/v3/klines"
            response = requests.get(url, params=params)
            if response.status_code == 200:
                candles_data = response.json()
                candles = []
                for kline in candles_data:
                    # Binance format: [
                    #   open_time, open, high, low, close, volume, 
                    #   close_time, quote_asset_volume, trades, 
                    #   taker_buy_base, taker_buy_quote, ignore
                    # ]
                    timestamp = datetime.fromtimestamp(kline[0]/1000, tz=timezone.utc)
                    candles.append({
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "timestamp": timestamp.isoformat(),
                        "symbol": symbol
                    })
                logger.info(f"Retrieved {len(candles)} {symbol} {interval} candles from Binance.US")
                return candles
            else:
                logger.error(f"Binance.US API error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Binance.US spot error for {symbol}: {str(e)}")
        return []
    
    def get_perpetual_ohlcv(self, symbol, interval="15m", starttime=None, endtime=None, limit=None):
        """Binance.US doesn't support perpetual futures - return empty list"""
        return []
    
    def get_options_data(self, symbol):
        """Binance.US doesn't support options - return None"""
        return None

class CryptoCollector:
    def __init__(self, symbols=None, intervals=None, symbol_start_times=None, end_time=None):
        self.symbols = symbols or ["BTC-USDT"]
        self.intervals = intervals or ["15m", "1h", "4h", "6h", "1d"]
        self.end_time = end_time or int(datetime.now(timezone.utc).timestamp() * 1000)
        self.running = True
        
        # Candles per day for each interval
        self.candles_per_day = {
            "15m": 96,    # 24 hours * 4 candles/hour
            "1h": 24,      # 24 candles/day
            "4h": 6,       # 6 candles/day (24/4)
            "6h": 4,       # 4 candles/day
            "1d": 1        # 1 candle/day
        }

        # Initialize exchange APIs
        self.coinbase = CoinbaseAPI()
        self.bitstamp = BitstampAPI()
        self.bitfinex = BitfinexAPI()
        self.kucoin = KucoinAPI()
        self.binanceus = BinanceUSAPI()

        self.exchanges = [self.coinbase, self.bitstamp, self.bitfinex, self.kucoin, self.binanceus]

        # Set custom start times per symbol
        default_start = int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        self.symbol_start_times = symbol_start_times or {}
        
        # Initialize last collected timestamps
        self.last_collected_ts = {}
        for symbol in self.symbols:
            # Use custom start time if defined, otherwise default
            start_ts = self.symbol_start_times.get(symbol, default_start)
            self.last_collected_ts[symbol] = {
                interval: start_ts for interval in self.intervals
            }

    def get_parquet_path(self, date_str, data_type, exchange_name, interval, symbol):
        """Generate parquet file path with exchange folder, interval, and symbol"""
        base_path = os.path.join(
            DATA_DIR, "crypto", symbol, interval, data_type, exchange_name
        )
        os.makedirs(base_path, exist_ok=True)
        filename = f"{date_str}_{interval}_{symbol}.parquet"
        return os.path.join(base_path, filename)

    def save_to_parquet(self, data, data_type, interval, symbol):
        """Save data to parquet files organized by symbol, date, exchange, and interval"""
        if not data:
            return
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Create date column for grouping
        df['date'] = df['timestamp'].dt.strftime('%Y%m%d')
        
        # Group by date and exchange
        grouped = df.groupby(['date', 'exchange'])
        
        for (date_str, exchange_name), group in grouped:
            file_path = self.get_parquet_path(date_str, data_type, exchange_name, interval, symbol)
            group = group.drop(columns=['date'])
            
            # Check if file exists
            if os.path.exists(file_path):
                existing_df = pd.read_parquet(file_path)
                # Combine and remove duplicates
                combined_df = pd.concat([existing_df, group])
                # Sort by timestamp to ensure chronological order
                combined_df = combined_df.sort_values('timestamp')
                # Remove duplicates keeping the last occurrence
                combined_df = combined_df.drop_duplicates(
                    subset=['timestamp'], 
                    keep='last'
                )
                combined_df.to_parquet(file_path, index=False)
            else:
                group.to_parquet(file_path, index=False)
                
            logger.info(f"Saved {len(group)} {symbol} {data_type} records for {exchange_name} ({interval}) to {file_path}")

    def collect_spot_data(self, symbol, start_ts, end_ts, interval):
        """Collect spot market data for a specific symbol"""
        results = []
        for exchange in self.exchanges:
            try:
                # Apply BinanceUS adjustment for BTC-USDT
                if exchange.name == "binanceus" and symbol == "BTC-USDT":
                    binance_start_2020 = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
                    if start_ts < binance_start_2020:
                        if end_ts < binance_start_2020:
                            continue  # Skip entirely before 2020
                        # Adjust start time to 2020 for BinanceUS
                        start_ts = max(start_ts, binance_start_2020)
                
                # Always fetch max candles per request
                data = exchange.get_spot_ohlcv(
                    symbol, 
                    interval, 
                    start_ts, 
                    end_ts,
                    limit=exchange.max_candles_per_request
                )
                if data:
                    for d in data:
                        d["exchange"] = exchange.name
                    results.extend(data)
            except Exception as e:
                logger.error(f"Error collecting {symbol} {interval} spot data from {exchange.name}: {str(e)}")
        return results

    def collect_derivatives_data(self, symbol, start_ts, end_ts, interval):
        """Collect derivatives data for a specific symbol"""
        # Exchanges don't support derivatives - skip
        return []

    def collect_options_data(self, symbol, start_ts, end_ts):
        """Collect options data for a specific symbol"""
        # Exchanges don't support options - skip
        return []

    async def fetch_data_chunk(self, symbol, interval, start_ts, end_ts):
        """Fetch all data types for a time chunk for specific symbol and interval"""
        # Run synchronous collection in thread pool
        loop = asyncio.get_running_loop()
        spot_data = await loop.run_in_executor(None, self.collect_spot_data, symbol, start_ts, end_ts, interval)
        
        # Skip derivatives and options
        deriv_data = []
        options_data = []
        
        # Save to parquet
        self.save_to_parquet(spot_data, "spot", interval, symbol)
        self.save_to_parquet(deriv_data, "derivatives", interval, symbol)
        self.save_to_parquet(options_data, "options", interval, symbol)
        
        logger.info(
            f"Collected {symbol} {interval} chunk: {len(spot_data)} spot candles "
            f"from {start_ts} to {end_ts}"
        )
        
        # Return max timestamp for this interval
        max_ts = max(
            [d['timestamp'] for d in spot_data + deriv_data + options_data]
        ) if any([spot_data, deriv_data, options_data]) else None
        return max_ts

    async def backfill_historical(self):
        """Backfill historical data in optimized chunks for all symbols and intervals"""
        logger.info("Starting historical backfill for symbols: " + ", ".join(self.symbols))
        
        for symbol in self.symbols:
            logger.info(f"Backfilling data for {symbol}")
            for interval in self.intervals:
                logger.info(f"Backfilling {symbol} {interval} data")
                current_ts = self.last_collected_ts[symbol][interval]
                interval_ms = self.get_interval_ms(interval)
                
                # Calculate optimal chunk size in days
                days_per_chunk = min(300 // self.candles_per_day[interval], 300)
                if days_per_chunk < 1:
                    days_per_chunk = 1
                
                while current_ts < self.end_time:
                    # Calculate chunk size in milliseconds
                    chunk_duration = days_per_chunk * 24 * 60 * 60 * 1000
                    chunk_end = min(current_ts + chunk_duration, self.end_time)
                    
                    # Fetch data for this chunk
                    max_ts = await self.fetch_data_chunk(symbol, interval, current_ts, chunk_end)
                    
                    # Move to next chunk
                    current_ts = chunk_end
                    self.last_collected_ts[symbol][interval] = current_ts
                        
                    # Avoid rate limiting
                    await asyncio.sleep(0.5)
                    
                logger.info(f"Completed {symbol} {interval} backfill")
            
    async def live_collection(self):
        """Collect new data at exact interval boundaries for all symbols and timeframes"""
        logger.info("Starting live data collection for symbols: " + ", ".join(self.symbols))
        
        # Calculate next run time aligned to the base interval (15m)
        base_interval_ms = self.get_interval_ms("15m")
        now = time.time()
        next_run = math.ceil(now / (base_interval_ms/1000)) * (base_interval_ms/1000)
        await asyncio.sleep(next_run - now)
        
        while self.running:
            try:
                # Save exact boundary timestamp for this cycle
                current_cycle_boundary = next_run  # Exact boundary time in seconds
                cycle_boundary_timestamp = int(current_cycle_boundary * 1000)  # Convert to ms
                current_time = datetime.now(timezone.utc)
                logger.info(f"Live collection cycle started at {current_time.isoformat()}")
                
                # Collect for all symbols and intervals
                for symbol in self.symbols:
                    for interval in self.intervals:
                        # Check if we should collect this interval at current boundary
                        if not self.should_collect_interval(interval, current_time):
                            continue
                        
                        try:
                            # Get interval in milliseconds
                            interval_ms = self.get_interval_ms(interval)
                            
                            # Use precise cycle boundary for interval calculation
                            interval_end = cycle_boundary_timestamp
                            interval_start = interval_end - interval_ms
                            
                            # Fetch data for this interval
                            max_ts = await self.fetch_data_chunk(symbol, interval, interval_start, interval_end)
                            
                            if max_ts:
                                logger.info(f"Live collected {symbol} {interval} data up to {max_ts}")
                            else:
                                logger.warning(f"No {symbol} {interval} data collected in this cycle")
                                
                        except Exception as e:
                            logger.error(f"Error during {symbol} {interval} live collection: {str(e)}")
                
                # Check file rotation
                current_date = datetime.now(timezone.utc).strftime('%Y%m%d')
                if current_date != self.current_file_date:
                    self.current_file_date = current_date
                    logger.info(f"New date detected: {current_date}")
                
            except Exception as e:
                logger.error(f"Live collection error: {str(e)}")
            
            # Calculate sleep time for next 15m boundary
            now = time.time()
            next_run = math.ceil(now / (base_interval_ms/1000)) * (base_interval_ms/1000)
            sleep_time = max(1, next_run - now)
            logger.info(f"Sleeping for {sleep_time:.2f} seconds until next collection cycle")
            await asyncio.sleep(sleep_time)

    def should_collect_interval(self, interval, current_time):
        """Determine if we should collect data for this interval at current UTC time"""
        if interval == '15m':
            # Always collect at every 15m boundary
            return True
        elif interval == '1h':
            # Collect only at hour boundaries (minute 0)
            return current_time.minute == 0
        elif interval == '4h':
            # Collect at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
            return current_time.hour % 4 == 0 and current_time.minute == 0
        elif interval == '6h':
            # Collect at 00:00, 06:00, 12:00, 18:00 UTC
            return current_time.hour % 6 == 0 and current_time.minute == 0
        elif interval == '1d':
            # Collect at 00:00 UTC
            return current_time.hour == 0 and current_time.minute == 0
        else:
            # Unknown interval - skip with warning
            logger.warning(f"Unknown interval '{interval}', skipping collection")
            return False

    def get_interval_ms(self, interval):
        """Convert interval string to milliseconds"""
        unit = interval[-1]
        value = int(interval[:-1])
        
        conversions = {
            'm': value * 60 * 1000,
            'h': value * 60 * 60 * 1000,
            'd': value * 24 * 60 * 60 * 1000,
            'w': value * 7 * 24 * 60 * 60 * 1000
        }
        return conversions.get(unit, 15 * 60 * 1000)  # Default 15m

    async def run(self):
        """Main collection workflow"""
        logger.info("Starting crypto data collector")
        
        try:
            # Backfill historical data first
            await self.backfill_historical()

            # Continue with live collection
            await self.live_collection()
            
        except KeyboardInterrupt:
            self.running = False
            logger.info("Stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")

def main():
    # Define all symbols to collect (using USD instead of USDT for exchanges that don't support USDT)
    symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT", "SUI-USDT", "XRP-USDT"]
    intervals = ["15m", "1h", "4h", "6h", "1d"]
    
    symbol_start_times = {
        "BTC-USDT": int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    }
    
    collector = CryptoCollector(
        symbols=symbols,
        intervals=intervals,
        symbol_start_times=symbol_start_times
    )
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("Stopped crypto collector")

if __name__ == "__main__":
    main()