import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Optional

from src.core.interfaces import MarketDataProvider
from src.core.models import MarketData, TimeFrame

class YFinanceDataProvider(MarketDataProvider):
    """
    MarketDataProvider implementation using the yfinance library.
    Perfect for historical data and low-frequency (daily) live trading.
    """
    def __init__(self):
        self._connected = False

    def connect(self) -> bool:
        # yfinance doesn't require a persistent websocket connection, 
        # so we just mark it as conceptually connected.
        self._connected = True
        return True

    def disconnect(self) -> bool:
        self._connected = False
        return True

    def get_latest_quote(self, symbol: str) -> MarketData:
        if not self._connected:
            raise ConnectionError("Provider is not connected")
        
        # Fetch the most recent day's data to get the latest close
        ticker = yf.Ticker(symbol)
        todays_data = ticker.history(period='1d')
        
        if todays_data.empty:
            raise ValueError(f"No recent data found for symbol: {symbol}")
            
        last_row = todays_data.iloc[-1]
        
        return MarketData(
            symbol=symbol,
            timestamp=datetime.now(), # Note: This is request time, not exchange time for yfinance
            last_price=float(last_row['Close']),
            bid_price=None, # yfinance doesn't provide reliable L1 order book data
            ask_price=None,
            volume=float(last_row['Volume'])
        )

    def get_historical_candlesticks(
        self, 
        symbol: str | list[str], 
        start: datetime, 
        end: datetime, 
        timeframe: TimeFrame
    ) -> pd.DataFrame:
        """
        Download historical candlestick data for one or more symbols.
        
        Returns a DataFrame with MultiIndex columns:
        - Level 0: OHLCV (Open, High, Low, Close, Volume)
        - Level 1: Symbol names
        
        This allows easy access like: data['Close']['AAPL']
        """
        if not self._connected:
            raise ConnectionError("Provider is not connected")
            
        interval_map = {
            TimeFrame.MINUTE: "1m",
            TimeFrame.HOUR: "1h",
            TimeFrame.DAY: "1d"
        }
        interval = interval_map.get(timeframe, "1d")
        
        # Convert datetime to string if needed
        start_str = start.strftime('%Y-%m-%d') if isinstance(start, datetime) else start
        end_str = end.strftime('%Y-%m-%d') if isinstance(end, datetime) else end
        
        # Download data, suppressing the progress bar for cleaner logs
        df = yf.download(symbol, start=start_str, end=end_str, interval=interval, progress=False, auto_adjust=False)
        
        if isinstance(df, pd.Series):
            df = df.to_frame()
            
        # If the dataframe is empty or missing data, return an empty DataFrame
        if df.empty or df.dropna(how='all').empty:
            symbols = [symbol] if isinstance(symbol, str) else symbol
            # Return empty DataFrame with expected structure
            return pd.DataFrame(columns=pd.MultiIndex.from_product([['Open', 'High', 'Low', 'Close', 'Volume'], symbols]))
            
        df = df.dropna(how='all')
        return df
