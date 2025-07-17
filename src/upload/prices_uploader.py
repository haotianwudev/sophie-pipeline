#!/usr/bin/env python
"""
Upload Price Data Using Multiple Sources Script

This script demonstrates the 'waterfall' logic in the upload_prices function
for fetching price data from multiple sources (Yahoo Finance, Polygon.io, Financial Datasets).

Example usage:
    python src/tools/upload_all_prices.py --tickers AAPL,MSFT,NVDA,SPY,VIX
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from colorama import Fore, Style, init
from dotenv import load_dotenv

# Add project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.tools.db_upload import upload_prices

# Initialize colorama
init(autoreset=True)

def get_date_range(start_date, end_date):
    """
    Get the date range for data fetching.
    If start_date or end_date is not provided, use defaults.
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    if not start_date:
        # Default to 1 year before end date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=365)
        start_date = start_dt.strftime("%Y-%m-%d")
    
    return start_date, end_date

def main():
    """Main entry point for the script."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Upload price data using multiple sources")
    parser.add_argument("--tickers", type=str, required=True, help="Comma-separated list of ticker symbols")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD). Defaults to 1 year before end date")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD). Defaults to today")
    parser.add_argument("--mode", type=str, default="auto", choices=["auto", "financial_datasets", "polygon", "yfinance"],
                       help="Data source mode to use (default: auto - uses waterfall logic)")
    
    args = parser.parse_args()
    
    # Parse tickers from comma-separated string
    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",")]
    
    # Get date range
    start_date, end_date = get_date_range(args.start_date, args.end_date)
    
    print(f"\n{Fore.CYAN}Uploading price data for {len(tickers)} tickers using {args.mode} mode{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Date range: {start_date} to {end_date}{Style.RESET_ALL}\n")
    
    # Process all tickers
    results = upload_prices(tickers, start_date, end_date, verbose=True, data_source=args.mode)
    
    # Print summary
    print(f"\n{Fore.CYAN}Summary:{Style.RESET_ALL}")
    print(f"  Total tickers processed: {len(tickers)}")
    print(f"  {Fore.GREEN}Successful:{Style.RESET_ALL} {len(results['success'])}")
    print(f"  {Fore.RED}Failed:{Style.RESET_ALL} {len(results['failed'])}")
    
    if results['failed']:
        print(f"\n{Fore.RED}Failed tickers:{Style.RESET_ALL} {', '.join(results['failed'])}")

if __name__ == "__main__":
    main() 