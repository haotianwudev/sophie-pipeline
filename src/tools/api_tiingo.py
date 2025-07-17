import os
import logging
import requests
import csv
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)

def get_daily_prices(ticker: str, years: int = 1) -> Optional[Dict]:
    """Fetch daily price data for a ticker from Tiingo API.
    
    Args:
        ticker: Stock ticker symbol (e.g. 'AAPL')
        years: Number of years of data to fetch (default 1)
    
    Returns:
        Dictionary containing price data or None if request fails
    """
    api_key = os.getenv('TINNGO_API_KEY')
    if not api_key:
        logger.error("TINNGO_API_KEY environment variable not set")
        return None

    start_date = (datetime.now() - timedelta(days=365*years)).strftime('%Y-%m-%d')
    
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start_date}&token={api_key}"
    headers = {
        'Content-Type': 'application/json'
    }
    
    try:
        logger.debug(f"Making request to: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        logger.debug(f"Raw response: {response.text[:500]}...")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch prices for {ticker}: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.debug(f"Error response: {e.response.text[:500]}...")
        return None

def save_prices_to_csv(data: Dict, ticker: str):
    """Save price data to CSV file in logs directory.
    
    Args:
        data: List of dictionaries containing price data from get_daily_prices()
        ticker: Ticker symbol processed
    
    CSV Format:
    - One row per date with OHLCV data
    """
    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"logs/tiingo_prices_{ticker}_{timestamp}.csv"
    
    fieldnames = [
        'biz_date', 'open', 'high', 'low', 'close', 'volume',
        'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'adjVolume',
        'divCash', 'splitFactor'
    ]
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        if data:
            for day in data:
                writer.writerow({
                    'biz_date': day['date'][:10],
                    'open': day['open'],
                    'high': day['high'],
                    'low': day['low'],
                    'close': day['close'],
                    'volume': day['volume'],
                    'adjOpen': day.get('adjOpen', ''),
                    'adjHigh': day.get('adjHigh', ''),
                    'adjLow': day.get('adjLow', ''),
                    'adjClose': day.get('adjClose', ''),
                    'adjVolume': day.get('adjVolume', ''),
                    'divCash': day.get('divCash', ''),
                    'splitFactor': day.get('splitFactor', '')
                })

if __name__ == "__main__":
    # Example usage - get 1 year of AAPL data
    ticker = "AAPL"
    logger.info(f"Fetching daily prices for {ticker}")
    data = get_daily_prices(ticker, years=1)
    if data:
        save_prices_to_csv(data, ticker)
        logger.info(f"Saved price data for {ticker} to CSV in logs directory")
