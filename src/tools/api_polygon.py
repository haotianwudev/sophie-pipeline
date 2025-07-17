"""
Simple API interface for Polygon.io and Yahoo Finance
"""

import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Get API key from environment
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')


def get_price_polygon(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get historical price data for a ticker from Polygon.io
    
    Args:
        ticker: The ticker symbol (e.g., "AAPL", "SPY")
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        
    Returns:
        DataFrame containing the historical price data
    """
    # Check if API key is available
    if not POLYGON_API_KEY:
        raise ValueError("POLYGON_API_KEY not found in environment variables")
    
    # Build the URL
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&apiKey={POLYGON_API_KEY}"
    
    # Make the request
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"API request failed with status code {response.status_code}: {response.text}")
    
    data = response.json()
    
    if data.get('status') not in ['OK', 'DELAYED'] or 'results' not in data:
        raise ValueError(f"API returned an error: {data}")
    
    if not data['results']:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(data['results'])
    
    # Convert timestamp to datetime and format as YYYY-MM-DD
    df['biz_date'] = pd.to_datetime(df['t'], unit='ms').dt.strftime('%Y-%m-%d')
    
    # Rename columns
    df = df.rename(columns={
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume',
        'vw': 'vwap',
        'n': 'transactions'
    })
    
    # Set biz_date as index and drop unnecessary columns
    df.set_index('biz_date', inplace=True)
    if 't' in df.columns:
        df = df.drop(['t'], axis=1)
    
    return df

