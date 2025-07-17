"""
Financial Metrics Service that integrates cache and database functions.
This service provides a unified interface for retrieving financial metrics data,
with automatic caching to both memory and database.
"""

import pandas as pd
from src.data.models import FinancialMetrics
from src.data.cache import get_cache
from src.tools.api_db import get_financial_metrics_db

class FinancialMetricsService:
    """Service for retrieving and managing financial metrics data."""
    
    def __init__(self):
        """Initialize the service with cache."""
        self._cache = get_cache()
    
    def get_financial_metrics(
        self, 
        ticker: str, 
        end_date: str, 
        period: str = "ttm", 
        limit: int = 10
    ) -> list[FinancialMetrics]:
        """
        Get financial metrics data for the given ticker.
        
        This function implements a two-level caching strategy:
        1. First, check in-memory cache
        2. Then, check the database
        
        Args:
            ticker: The stock ticker symbol
            end_date: The end date for filtering metrics (only metrics with report_period <= end_date)
            period: The reporting period (e.g., "ttm", "annual", "quarterly")
            limit: Maximum number of records to return
            
        Returns:
            A list of FinancialMetrics objects for the specified ticker
        """
        ticker = ticker.upper()
        
        # 1. Check in-memory cache first (fastest)
        if cached_data := self._cache.get_financial_metrics(ticker):
            # Filter cached data by date and period, and convert to FinancialMetrics objects
            filtered_data = [
                FinancialMetrics(**metric) 
                for metric in cached_data 
                if metric["report_period"] <= end_date and metric["period"] == period
            ]
            # Sort by report_period in descending order and limit
            filtered_data.sort(key=lambda x: x.report_period, reverse=True)
            if filtered_data:
                return filtered_data[:limit]
        
        # 2. Check the database (slower than memory, faster than API)
        if db_data := get_financial_metrics_db(ticker, end_date, period, limit):
            # Cache in memory for future requests
            # We cache all metrics for this ticker, not just the ones returned by this query
            metrics_to_cache = [metric.model_dump() for metric in db_data]
            self._cache.set_financial_metrics(ticker, metrics_to_cache)
            return db_data
        
        # If data is not found in cache or database, return empty list
        return []
    
    def financial_metrics_to_df(self, metrics: list[FinancialMetrics]) -> pd.DataFrame:
        """Convert financial metrics to a DataFrame."""
        # Check if we have any metrics
        if not metrics:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([m.model_dump() for m in metrics])
        
        # Format the dataframe
        if 'report_period' in df.columns:
            df['report_period'] = pd.to_datetime(df['report_period'])
            df.set_index('report_period', inplace=True)
            
        # Sort by date (newest to oldest, since financial metrics are usually used this way)
        df.sort_index(ascending=False, inplace=True)
        
        return df

# Create a singleton instance
financial_metrics_service = FinancialMetricsService()

# Convenience functions that use the service
def get_financial_metrics(
    ticker: str, 
    end_date: str, 
    period: str = "ttm", 
    limit: int = 10
) -> list[FinancialMetrics]:
    """Get financial metrics for the given ticker."""
    return financial_metrics_service.get_financial_metrics(ticker, end_date, period, limit)

def get_financial_metrics_df(
    ticker: str, 
    end_date: str, 
    period: str = "ttm", 
    limit: int = 10
) -> pd.DataFrame:
    """Get financial metrics as a DataFrame."""
    metrics = financial_metrics_service.get_financial_metrics(ticker, end_date, period, limit)
    return financial_metrics_service.financial_metrics_to_df(metrics) 