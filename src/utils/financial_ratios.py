"""
Financial ratio calculation utilities.

This module provides centralized functions for calculating financial ratios
to ensure consistency across all agents.
"""

from typing import Optional
from tools.line_items_service import get_line_items
from tools.financial_metrics_service import get_financial_metrics


def calculate_debt_to_equity_ratio(
    ticker: str, 
    end_date: str = None, 
    fallback_to_financial_metrics: bool = True
) -> Optional[float]:
    """
    Calculate debt-to-equity ratio using line_items data.
    
    This function provides a centralized, consistent way to calculate D/E ratios
    across all agents. It uses line_items data for accuracy and falls back to
    financial_metrics if needed.
    
    Args:
        ticker: Stock ticker symbol
        end_date: End date for data retrieval (YYYY-MM-DD format). If None, gets latest available data.
        fallback_to_financial_metrics: Whether to use financial_metrics as fallback
        
    Returns:
        float: Debt-to-equity ratio, or None if data unavailable
        
    Note:
        - Uses total_debt / shareholders_equity calculation
        - Returns None if shareholders_equity is 0 or negative
        - Prioritizes line_items data over financial_metrics for accuracy
        - Always gets the most recent available data
    """
    try:
        # Primary method: Calculate from line_items
        # Use a future date to ensure we get the latest available data
        search_end_date = end_date if end_date else "2030-12-31"
        
        line_items = get_line_items(
            ticker=ticker,
            line_items=['total_debt', 'shareholders_equity'],
            end_date=search_end_date,
            limit=1
        )
        
        if line_items and len(line_items) > 0:
            item = line_items[0]
            total_debt = item.total_debt
            shareholders_equity = item.shareholders_equity
            
            if (total_debt is not None and 
                shareholders_equity is not None and 
                shareholders_equity > 0):
                return total_debt / shareholders_equity
        
        # Fallback method: Use financial_metrics if line_items unavailable
        if fallback_to_financial_metrics:
            metrics = get_financial_metrics(ticker, search_end_date, limit=1)
            if metrics and len(metrics) > 0:
                return metrics[0].debt_to_equity
                
        return None
        
    except Exception as e:
        # Log error but don't crash - return None for graceful handling
        print(f"Error calculating debt-to-equity for {ticker}: {str(e)}")
        return None


def get_debt_to_equity_with_details(
    ticker: str, 
    end_date: str = None, 
    fallback_to_financial_metrics: bool = True
) -> dict:
    """
    Calculate debt-to-equity ratio with detailed information about the calculation.
    
    Args:
        ticker: Stock ticker symbol
        end_date: End date for data retrieval (YYYY-MM-DD format). If None, gets latest available data.
        fallback_to_financial_metrics: Whether to use financial_metrics as fallback
        
    Returns:
        dict: Contains ratio, source, and component values
    """
    try:
        # Primary method: Calculate from line_items
        # Use a future date to ensure we get the latest available data
        search_end_date = end_date if end_date else "2030-12-31"
        
        line_items = get_line_items(
            ticker=ticker,
            line_items=['total_debt', 'shareholders_equity'],
            end_date=search_end_date,
            limit=1
        )
        
        if line_items and len(line_items) > 0:
            item = line_items[0]
            total_debt = item.total_debt
            shareholders_equity = item.shareholders_equity
            
            if (total_debt is not None and 
                shareholders_equity is not None and 
                shareholders_equity > 0):
                return {
                    'ratio': total_debt / shareholders_equity,
                    'source': 'line_items',
                    'total_debt': total_debt,
                    'shareholders_equity': shareholders_equity,
                    'calculation_method': 'total_debt / shareholders_equity'
                }
        
        # Fallback method: Use financial_metrics if line_items unavailable
        if fallback_to_financial_metrics:
            metrics = get_financial_metrics(ticker, search_end_date, limit=1)
            if metrics and len(metrics) > 0 and metrics[0].debt_to_equity is not None:
                return {
                    'ratio': metrics[0].debt_to_equity,
                    'source': 'financial_metrics',
                    'total_debt': None,
                    'shareholders_equity': None,
                    'calculation_method': 'from_financial_metrics_api'
                }
                
        return {
            'ratio': None,
            'source': 'unavailable',
            'total_debt': None,
            'shareholders_equity': None,
            'calculation_method': 'data_not_available'
        }
        
    except Exception as e:
        return {
            'ratio': None,
            'source': 'error',
            'total_debt': None,
            'shareholders_equity': None,
            'calculation_method': f'error: {str(e)}'
        } 