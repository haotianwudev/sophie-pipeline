"""
Insider Trades Service that integrates cache and database functions.
This service provides a unified interface for retrieving insider trading data,
using only cached data and database storage without direct API calls.
"""

import pandas as pd
from src.data.models import InsiderTrade
from src.data.cache import get_cache
from src.tools.api_db import get_insider_trades_db, save_insider_trades

class InsiderTradesService:
    """Service for retrieving and managing insider trades data."""
    
    def __init__(self):
        """Initialize the service with cache."""
        self._cache = get_cache()
    
    def get_insider_trades(
        self, 
        ticker: str, 
        end_date: str,
        start_date: str | None = None,
        limit: int = 1000
    ) -> list[InsiderTrade]:
        """
        Get insider trades data for the given ticker.
        
        This function implements a two-level caching strategy:
        1. First, check in-memory cache
        2. Then, check the database
        
        Args:
            ticker: The stock ticker symbol
            end_date: The end date for filtering trades
            start_date: Optional start date for filtering trades
            limit: Maximum number of records to return
            
        Returns:
            A list of InsiderTrade objects for the specified ticker and date range
        """
        ticker = ticker.upper()
        
        # 1. Check in-memory cache first (fastest)
        if cached_data := self._cache.get_insider_trades(ticker):
            # Filter cached data by date range and convert to InsiderTrade objects
            filtered_data = [
                InsiderTrade(**trade) 
                for trade in cached_data 
                if ((start_date is None or 
                    (trade.get("transaction_date") or trade.get("filing_date")) >= start_date) and
                    (trade.get("transaction_date") or trade.get("filing_date")) <= end_date)
            ]
            
            # Sort by transaction date (or filing date if no transaction date) in descending order
            filtered_data.sort(
                key=lambda x: x.transaction_date or x.filing_date, 
                reverse=True
            )
            
            if filtered_data:
                return filtered_data[:limit]
        
        # 2. Check the database (slower than memory, faster than API)
        if db_data := get_insider_trades_db(ticker, end_date, start_date, limit):
            # Cache in memory for future requests
            trades_to_cache = [trade.model_dump() for trade in db_data]
            self._cache.set_insider_trades(ticker, trades_to_cache)
            return db_data
        
        # If data is not found in cache or database, return empty list
        return []
    
    def insider_trades_to_df(self, trades: list[InsiderTrade]) -> pd.DataFrame:
        """Convert insider trades to a DataFrame."""
        # Check if we have any trades
        if not trades:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([trade.model_dump() for trade in trades])
        
        # Format the dataframe
        date_columns = ['transaction_date', 'filing_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Set transaction_date as index, falling back to filing_date if needed
        if 'transaction_date' in df.columns and not df['transaction_date'].isna().all():
            df.set_index('transaction_date', inplace=True)
        elif 'filing_date' in df.columns:
            df.set_index('filing_date', inplace=True)
            
        # Sort by date (newest to oldest)
        df.sort_index(ascending=False, inplace=True)
        
        return df

# Create a singleton instance
insider_trades_service = InsiderTradesService()

# Convenience functions that use the service
def get_insider_trades(
    ticker: str, 
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000
) -> list[InsiderTrade]:
    """Get insider trades for the given ticker and date range."""
    return insider_trades_service.get_insider_trades(ticker, end_date, start_date, limit)

def get_insider_trades_df(
    ticker: str, 
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000
) -> pd.DataFrame:
    """Get insider trades as a DataFrame."""
    trades = insider_trades_service.get_insider_trades(ticker, end_date, start_date, limit)
    return insider_trades_service.insider_trades_to_df(trades)
