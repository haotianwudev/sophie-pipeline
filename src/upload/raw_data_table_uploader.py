#!/usr/bin/env python
"""
Load Financial Data Script

This script fetches company facts, price data, company news, financial metrics, 
insider trades, and line items for specified tickers and saves it to the PostgreSQL database.
It uses the API functions in tools/api.py to get the data and stores it in the database.

The various service modules can then access this data from the cache and database, but won't
make API calls directly. This script is responsible for populating the database with all financial data.

Example usage:
    poetry run python src/load_financial_data.py --tickers AAPL,MSFT,NVDA
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
from colorama import Fore, Style, init
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import API functions to fetch data
from src.tools.api import get_company_facts, get_prices, get_company_news, get_financial_metrics, get_insider_trades, search_line_items
# Import DB functions to save data
from src.cfg.sql_table_upload import TABLE_UPLOAD_CONFIG
from src.tools.db_upload import (
    save_to_db,  # Generic function we'll use
    upload_company_facts,
    upload_prices,
    upload_company_news,
    upload_financial_metrics,
    upload_insider_trades,
    save_company_facts_to_db,
    upload_prices_financialdatasets,
    save_company_news_to_db,
    save_financial_metrics_to_db,
    save_insider_trades_to_db,
    save_line_items_to_db
)

# Initialize colorama
init(autoreset=True)

def load_financial_data(tickers, start_date, end_date, verbose=False, table_name=None):
    """
    Load financial data for the specified tickers and date range using batch operations.
    
    Args:
        tickers (list): List of ticker symbols
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        verbose (bool): Whether to print verbose output
        table_name (str): Specific table to process (None for all)
    
    Returns:
        dict: Results of data loading with success/failed lists per table
    """
    if not tickers:
        return {}
        
    results = {
        f"{table}_success": [] for table in TABLE_UPLOAD_CONFIG.keys()
    }
    results.update({
        f"{table}_failed": [] for table in TABLE_UPLOAD_CONFIG.keys()
    })
    
    print(f"\n{Fore.CYAN}Loading financial data for {len(tickers)} tickers{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Date range: {start_date} to {end_date}{Style.RESET_ALL}\n")
    
    try:
        if table_name:
            # Process only the specified table
            config = TABLE_UPLOAD_CONFIG.get(table_name)
            if not config:
                print(f"{Fore.RED}Invalid table name: {table_name}{Style.RESET_ALL}")
                return results
                
            print(f"Processing {table_name} for {len(tickers)} tickers...")
            # Only pass params that are not None
            param_values = {param: locals().get(param) for param in config['params'] if locals().get(param) is not None}
            batch_result = config['upload_function'](
                tickers,
                **param_values
            )
            
            if batch_result:
                results[f"{table_name}_success"] = batch_result['success']
                results[f"{table_name}_failed"] = batch_result['failed']
                print(f"{Fore.GREEN}Completed {table_name}: {len(batch_result['success'])}/{len(tickers)} succeeded{Style.RESET_ALL}")
                
    except Exception as e:
        print(f"{Fore.RED}Error processing {table_name}: {str(e)}{Style.RESET_ALL}")
        results[f"{table_name}_failed"] = tickers
        
    
    return results

def get_date_range(start_date, end_date):
    """
    Get the date range for the data fetching.
    If start_date or end_date is not provided, use defaults.
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    if not start_date:
        # Default to 3 months before end date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=450)
        start_date = start_dt.strftime("%Y-%m-%d")
    
    return start_date, end_date

def main():
    """Main entry point for the script."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Load financial data into the database")
    parser.add_argument("--tickers", type=str, required=True, help="Comma-separated list of stock ticker symbols")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to 5 years before end date",
    )
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD). Defaults to today")
    parser.add_argument("--verbose", action="store_true", help="Show detailed information")
    parser.add_argument("--table", type=str, 
                      help="Specify which table config to use (e.g. 'prices')")
    
    args = parser.parse_args()
    
    # Parse tickers from comma-separated string
    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",")]
    
    # Get date range
    start_date, end_date = get_date_range(args.start_date, args.end_date)
    
    # Load financial data
    results = load_financial_data(
        tickers, 
        start_date, 
        end_date, 
        verbose=args.verbose,
        table_name=args.table
    )
    
    # Print summary
    print(f"\n{Fore.CYAN}Summary:{Style.RESET_ALL}")
    print(f"  Total tickers processed: {len(tickers)}")
    
    if args.table:
        # Single table summary
        print(f"  {Fore.GREEN}{args.table} - Successful:{Style.RESET_ALL} {len(results[f'{args.table}_success'])}")
        print(f"  {Fore.RED}{args.table} - Failed:{Style.RESET_ALL} {len(results[f'{args.table}_failed'])}")
    else:
        # Full summary for all tables
        for table in TABLE_UPLOAD_CONFIG.keys():
            print(f"  {Fore.GREEN}{table} - Successful:{Style.RESET_ALL} {len(results[f'{table}_success'])}")
            print(f"  {Fore.RED}{table} - Failed:{Style.RESET_ALL} {len(results[f'{table}_failed'])}")

if __name__ == "__main__":
    main()
