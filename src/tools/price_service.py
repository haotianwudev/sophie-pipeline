"""
Price Service that integrates cache and database functions.
This service provides a unified interface for retrieving price data,
with automatic caching to both memory and database.
"""

import datetime
from src.data.models import Price
from src.data.cache import get_cache
from src.tools.api_db import get_prices_db

class PriceService:
    """Service for retrieving and managing price data."""
    
    def __init__(self):
        """Initialize the service with cache."""
        self._cache = get_cache()
    
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> list[Price]:
        """
        Get price data for the given ticker and date range.
        
        This function implements a two-level caching strategy:
        1. First, check in-memory cache
        2. Then, check the database
        
        Args:
            ticker: The stock ticker symbol
            start_date: The start date for the price data range (YYYY-MM-DD)
            end_date: The end date for the price data range (YYYY-MM-DD)
            
        Returns:
            A list of Price objects for the specified ticker and date range
        """
        ticker = ticker.upper()
        
        # 1. Check in-memory cache first (fastest)
        if cached_data := self._cache.get_prices(ticker):
            # Filter cached data by date range and convert to Price objects
            filtered_data = [Price(**price) for price in cached_data if start_date <= price["time"] <= end_date]
            if filtered_data:
                return filtered_data
        
        # 2. Check the database (slower than memory, faster than API)
        if db_data := get_prices_db(ticker, start_date, end_date):
            # Cache in memory for future requests
            prices_to_cache = [price.model_dump() for price in db_data]
            self._cache.set_prices(ticker, prices_to_cache)
            return db_data
        
        # If data is not found in cache or database, return empty list
        return []
    
    def prices_to_df(self, prices: list[Price]) -> 'pd.DataFrame':
        """Convert prices to a DataFrame."""
        import pandas as pd
        
        # Check if we have any prices
        if not prices:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([p.model_dump() for p in prices])
        
        # Format the dataframe
        df["Date"] = pd.to_datetime(df["biz_date"])
        df.set_index("Date", inplace=True)
        
        # Ensure columns are numeric
        numeric_cols = ["open", "close", "high", "low", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        # Sort by date (oldest to newest)
        df.sort_index(inplace=True)
        
        return df
    
    def get_price_data(self, ticker: str, start_date: str, end_date: str) -> 'pd.DataFrame':
        """Get price data as a DataFrame."""
        prices = self.get_prices(ticker, start_date, end_date)
        return self.prices_to_df(prices)

# Create a singleton instance
price_service = PriceService()

# Convenience functions that use the service
def get_prices(ticker: str, start_date: str, end_date: str) -> list[Price]:
    """Get prices for the given ticker and date range."""
    return price_service.get_prices(ticker, start_date, end_date)

def prices_to_df(prices: list[Price]) -> 'pd.DataFrame':
    """Convert prices to a DataFrame."""
    return price_service.prices_to_df(prices)

def get_price_data(ticker: str, start_date: str, end_date: str) -> 'pd.DataFrame':
    """Get price data as a DataFrame."""
    return price_service.get_price_data(ticker, start_date, end_date) 