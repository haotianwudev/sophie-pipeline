import os
import logging
import requests
import csv
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

def get_daily_prices(ticker: str, days: int = 30) -> Optional[Dict]:
    """Fetch daily price data for a ticker from Finnhub API.
    
    Args:
        ticker: Stock ticker symbol (e.g. 'AAPL')
        days: Number of days of data to fetch (max 365)
    
    Returns:
        Dictionary containing price data or None if request fails
    """
    api_key = os.getenv('FINNHUB_API_KEY')
    if not api_key:
        logger.error("FINNHUB_API_KEY environment variable not set")
        return None

    url = "https://finnhub.io/api/v1/stock/candle"
    params = {
        'symbol': ticker,
        'resolution': 'D',
        'count': min(days, 365),  # Finnhub max is 365 days
        'token': api_key
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('s') != 'ok':
            logger.error(f"No price data found for {ticker}: {data.get('error', 'Unknown error')}")
            return None
            
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch prices for {ticker}: {str(e)}")
        return None

def save_prices_to_csv(data: Dict, ticker: str):
    """Save price data to CSV file in logs directory.
    
    Args:
        data: Dictionary containing price data from get_daily_prices()
        ticker: Ticker symbol processed
    
    CSV Format:
    - One row per date with OHLCV data
    """
    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"logs/finnhub_prices_{ticker}_{timestamp}.csv"
    
    fieldnames = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        if data and 't' in data and 'o' in data:
            for i in range(len(data['t'])):
                date = datetime.fromtimestamp(data['t'][i]).strftime('%Y-%m-%d')
                writer.writerow({
                    'date': date,
                    'open': data['o'][i],
                    'high': data['h'][i],
                    'low': data['l'][i],
                    'close': data['c'][i],
                    'volume': data['v'][i]
                })

if __name__ == "__main__":
    # Example usage
    ticker = "AAPL"
    logger.info(f"Fetching daily prices for {ticker}")
    data = get_daily_prices(ticker, days=90)
    if data:
        save_prices_to_csv(data, ticker)
        logger.info(f"Saved price data for {ticker} to CSV in logs directory")
