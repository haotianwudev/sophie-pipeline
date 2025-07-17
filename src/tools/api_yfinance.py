"""
Simple API interface for Polygon.io and Yahoo Finance
"""

import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import sys
import importlib.util

# Check if yfinance is available in a more reliable way
try:
    # Try direct import first
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    # Check if it's available but not in the current path
    yf_spec = importlib.util.find_spec("yfinance")
    if yf_spec is not None:
        # Module exists but couldn't be imported - add to path
        sys.path.append(yf_spec.submodule_search_locations[0])
        try:
            import yfinance as yf
            HAS_YFINANCE = True
        except ImportError:
            HAS_YFINANCE = False
    else:
        HAS_YFINANCE = False
        
if not HAS_YFINANCE:
    print("Warning: yfinance package not found. Yahoo Finance data source will not be available.")


def get_price_yahoofinance(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get historical price data for a ticker from Yahoo Finance
    
    Args:
        ticker: The ticker symbol (e.g., "AAPL", "SPY", "^VIX")
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        
    Returns:
        DataFrame containing the historical price data
    """
    if not HAS_YFINANCE:
        raise ImportError("yfinance is not installed. Install it with 'pip install yfinance'")
    
    # Download data from Yahoo Finance
    print(f"Fetching data from Yahoo Finance for ticker: {ticker}")
    df = yf.download(ticker, start=start_date, end=end_date, progress=False)
    
    if df.empty:
        print(f"No data found for {ticker} between {start_date} and {end_date}")
        return pd.DataFrame()
    
    # Handle column names which may be multi-index for some tickers like ^VIX
    if isinstance(df.columns, pd.MultiIndex):
        print(f"Multi-index columns detected for {ticker}, flattening columns")
        # Extract just the first level of the multi-index (e.g., 'Open', 'High', etc.)
        new_columns = [col[0] for col in df.columns]
        df.columns = new_columns
    
    # Rename columns to match our convention (lower case)
    df.columns = [col.lower() for col in df.columns]
    
    # Reset index to make date a column (named 'Date' by default in yfinance)
    df = df.reset_index()
    
    # Debug print column names after reset
    print(f"Columns after reset: {df.columns.tolist()}")
    
    # Fix for case sensitivity - the column might be 'Date' not 'date'
    date_col = 'Date' if 'Date' in df.columns else 'date'
    
    # Create biz_date column using the correct date column name
    df['biz_date'] = df[date_col].dt.strftime('%Y-%m-%d')
    
    # Drop the original date column
    df = df.drop([date_col], axis=1)
    
    # Set biz_date as index
    df.set_index('biz_date', inplace=True)
    
    print(f"Successfully fetched {len(df)} records for {ticker}")
    return df
