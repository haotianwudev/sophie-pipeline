"""
Line Items Service that integrates cache and database functions.
This service provides a unified interface for retrieving financial line items data,
using only cached data and database storage without direct API calls.
"""

import pandas as pd
from src.data.models import LineItem
from src.data.cache import get_cache
from src.tools.api_db import get_line_items_db, save_line_items
from src.cfg.line_items_list import LINE_ITEMS

# Default line items to query when none are specified
DEFAULT_LINE_ITEMS = [
    'revenue', 
    'net_income', 
    'earnings_per_share', 
    'ebit',
    'total_assets', 
    'total_liabilities', 
    'shareholders_equity'
]

class LineItemsService:
    """Service for retrieving and managing line items data."""
    
    def __init__(self):
        """Initialize the service with cache."""
        self._cache = get_cache()
    
    def get_line_items(
        self, 
        ticker: str,
        line_items: list[str],
        end_date: str, 
        period: str = "ttm", 
        limit: int = 10
    ) -> list[LineItem]:
        """
        Get line items data for the given ticker.
        
        This function implements a two-level caching strategy:
        1. First, check in-memory cache
        2. Then, check the database
        
        Args:
            ticker: The stock ticker symbol
            line_items: List of line item names to fetch (must match database column names)
            end_date: The end date for filtering line items (YYYY-MM-DD format)
            period: The reporting period (e.g., "ttm", "annual", "quarterly")
            limit: Maximum number of records to return
            
        Returns:
            A list of LineItem objects for the specified ticker and line items
        """
        ticker = ticker.upper()
        
        # Validate requested line items against known database columns
        valid_line_items = []
        for item in line_items:
            # Convert to snake_case if needed (e.g. "cashAndEquivalents" -> "cash_and_equivalents")
            normalized_item = item.lower().replace(' ', '_')
            if normalized_item in LINE_ITEMS:
                valid_line_items.append(normalized_item)
        
        if not valid_line_items:
            return []
        
        # 1. Check in-memory cache first (fastest)
        cached_items = []
        if cached_data := self._cache.get_line_items(ticker):
            # Filter cached items by date and period
            for item in cached_data:
                if (item.get("report_period") <= end_date and 
                    item.get("period") == period and
                    all(item.get(field) is not None for field in valid_line_items)):
                    
                    # Create LineItem with only the requested fields
                    line_item_data = {
                        "ticker": ticker,
                        "report_period": item.get("report_period"),
                        "period": period,
                        "currency": item.get("currency", "USD")
                    }
                    for field in valid_line_items:
                        value = item.get(field)
                        # Convert Decimal to float for compatibility
                        if hasattr(value, 'to_eng_string'):  # Check if it's a Decimal
                            value = float(value)
                        line_item_data[field] = value
                    
                    cached_items.append(LineItem(**line_item_data))
            
            # Sort by report_period in descending order and limit
            cached_items.sort(key=lambda x: x.report_period, reverse=True)
            if len(cached_items) >= limit:
                return cached_items[:limit]
        
        # 2. Check the database (slower than memory, faster than API)
        if db_items := get_line_items_db(ticker, valid_line_items, end_date, period, limit):
            # Convert Decimal values to float in database results
            for item in db_items:
                data = item.model_dump()
                for field in valid_line_items:
                    if field in data and hasattr(data[field], 'to_eng_string'):
                        setattr(item, field, float(data[field]))
            
            # Cache in memory for future requests
            self._cache_line_items(ticker, db_items)
            return db_items
        
        # Return cached items if we have any (even if fewer than requested)
        return cached_items
    
    def _cache_line_items(self, ticker: str, items: list[LineItem]):
        """
        Cache line items in memory.
        This special function handles the unique structure of line items in the cache.
        """
        # Convert line items to cache format
        cache_items = []
        for item in items:
            data = item.model_dump()
            # Extract standard fields
            std_fields = {
                'ticker': data.get('ticker'),
                'report_period': data.get('report_period'),
                'period': data.get('period'),
                'currency': data.get('currency')
            }
            
            # Each line item becomes a separate entry in the cache
            for field_name, field_value in data.items():
                if field_name in std_fields:
                    continue  # Skip standard fields
                
                # Create a cache entry with the line item
                cache_entry = std_fields.copy()
                cache_entry[field_name] = field_value
                cache_items.append(cache_entry)
        
        # Store in cache
        self._cache.set_line_items(ticker, cache_items)
    
    def line_items_to_df(self, items: list[LineItem]) -> pd.DataFrame:
        """Convert line items to a DataFrame."""
        # Check if we have any items
        if not items:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([item.model_dump() for item in items])
        
        # Format the dataframe
        if 'report_period' in df.columns:
            df['report_period'] = pd.to_datetime(df['report_period'])
            df.set_index('report_period', inplace=True)
            
        # Sort by date (newest to oldest)
        df.sort_index(ascending=False, inplace=True)
        
        return df

# Create a singleton instance
line_items_service = LineItemsService()

# Convenience functions that use the service
def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str, 
    period: str = "ttm", 
    limit: int = 10
) -> list[LineItem]:
    """Search for line items for the given ticker."""
    return line_items_service.get_line_items(ticker, line_items, end_date, period, limit)

def get_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str, 
    period: str = "ttm", 
    limit: int = 10
) -> list[LineItem]:
    """Get line items for the given ticker."""
    return line_items_service.get_line_items(ticker, line_items, end_date, period, limit)

def get_line_items_df(
    ticker: str,
    line_items: list[str],
    end_date: str, 
    period: str = "ttm", 
    limit: int = 10
) -> pd.DataFrame:
    """Get line items as a DataFrame."""
    items = line_items_service.get_line_items(ticker, line_items, end_date, period, limit)
    return line_items_service.line_items_to_df(items)
