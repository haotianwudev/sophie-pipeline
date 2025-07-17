#!/usr/bin/env python
"""
Database Upload Functions

This module contains all functions related to uploading financial data to the PostgreSQL database.
Moved from src/upload/load_financial_data.py as part of refactoring.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from colorama import Fore, Style
from datetime import datetime
from src.cfg.line_items_list import LINE_ITEMS
from src.tools.api_alphavantage import get_news_sentiment_multi

def save_to_db(data, upload_func, table_name=None, verbose=False):
    """Generic function to save data to database with standardized logging"""
    try:
        if verbose:
            print(f"Saving data to {table_name or 'default'} table...")
        
        result = upload_func(data, table_name=table_name)
        
        if verbose:
            record_count = len(data) if hasattr(data, '__len__') else 1
            print(f"Successfully saved {record_count} records to {table_name or 'default'} table")
            
        return result
    except Exception as e:
        if verbose:
            print(f"{Fore.RED}Failed to save data: {e}{Style.RESET_ALL}")
        return False

def upload_company_facts(tickers, verbose=False):
    """Load and save company facts for multiple tickers to the PostgreSQL database."""
    if not tickers:
        return False
        
    try:
        from src.tools.api import get_company_facts
        
        success = []
        failed = []
        
        for ticker in tickers:
            if verbose:
                print(f"Loading company facts for {ticker}...", end=" ", flush=True)
            
            try:
                facts = get_company_facts(ticker)
                if not facts:
                    if verbose:
                        print(f"{Fore.YELLOW}No data{Style.RESET_ALL}")
                    failed.append(ticker)
                    continue
                    
                if save_company_facts_to_db(facts):
                    if verbose:
                        print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                    success.append(ticker)
                else:
                    if verbose:
                        print(f"{Fore.RED}Failed to save{Style.RESET_ALL}")
                    failed.append(ticker)
            except Exception as e:
                if verbose:
                    print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                failed.append(ticker)
                
        return {'success': success, 'failed': failed}
        
    except Exception as e:
        print(f"{Fore.RED}Error in batch company facts loading: {e}{Style.RESET_ALL}")
        return False

def save_company_facts_to_db(company_facts, table_name=None):
    """Save company facts to the PostgreSQL database."""
    try:
        # Get database URL from environment
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            print(f"{Fore.RED}Error: DATABASE_URL environment variable not set{Style.RESET_ALL}")
            return False

        # Connect to PostgreSQL
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Convert to dictionary
        data = company_facts.model_dump()
        
        # Fields to insert/update
        fields = [
            'ticker', 'name', 'cik', 'industry', 'sector', 'category', 
            'exchange', 'is_active', 'listing_date', 'location', 'market_cap',
            'number_of_employees', 'sec_filings_url', 'sic_code', 
            'sic_industry', 'sic_sector', 'website_url', 'weighted_average_shares'
        ]
        
        # Build the SQL query
        placeholders = ', '.join(['%s'] * len(fields))
        field_list = ', '.join(fields)
        update_list = ', '.join([f"{field} = EXCLUDED.{field}" for field in fields])
        update_list += ", updated_at = CURRENT_TIMESTAMP"
        
        sql = f"""
        INSERT INTO company_facts ({field_list})
        VALUES ({placeholders})
        ON CONFLICT (ticker) DO UPDATE SET {update_list}
        """
        
        # Execute the query
        cursor.execute(sql, [data.get(field) for field in fields])
        
        # Commit the transaction
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"{Fore.RED}Error saving company facts to database: {e}{Style.RESET_ALL}")
        return False

def upload_prices_financialdatasets(ticker, start_date, end_date):
    """
    Fetch price data using the API and save it to the PostgreSQL database.
    Takes ticker, start_date, and end_date as parameters, fetches the data,
    and saves it to the database.
    
    Returns:
        dict: Status object with 'success' (bool) and 'no_data' (bool) fields
    """
    try:
        from src.tools.api import get_prices
        
        prices = get_prices(ticker, start_date, end_date)
        if not prices:
            return {'success': False, 'no_data': True}
        
        from src.tools.api_db import save_prices
        result = save_prices(ticker, prices)
        return {'success': result, 'no_data': False}
    except Exception as e:
        print(f"{Fore.RED}Error saving price data to database: {e}{Style.RESET_ALL}")
        return {'success': False, 'no_data': False}

def upload_prices(tickers, start_date, end_date, verbose=False, data_source='auto'):
    """
    Load and save price data for multiple tickers to the PostgreSQL database.
    
    Implements waterfall logic for choosing data source:
    - If ticker is "VIX" or "SPY", use Yahoo Finance (with special handling for VIX -> ^VIX)
    - If end_date is within last 2 years, try Polygon.io first
    - Otherwise or if previous methods fail, use Financial Datasets (fallback)
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        verbose: Whether to print verbose output
        data_source: The data source to use ('auto', 'financial_datasets', 'polygon', 'yfinance')
                    'auto' implements the waterfall logic
    
    Returns:
        dict: Results with 'success' and 'failed' lists of tickers
    """
    if not tickers:
        return {'success': [], 'failed': []}
    
    print(f"\n{'='*30}")
    print(f"UPLOAD PRICES: {len(tickers)} tickers, mode: {data_source}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"{'='*30}\n")
        
    try:
        success = []
        failed = []
        
        for ticker in tickers:
            print(f"\n{'-'*50}")
            if verbose:
                print(f"Processing {ticker}...", end=" ", flush=True)
            else:
                print(f"Processing {ticker}...")
            
            # Determine data source to use
            selected_source = data_source
            result = None
            
            # Apply waterfall logic if 'auto' is specified
            if data_source.lower() == 'auto':
                print(f"Using waterfall logic for data source selection")
                
                # Check if ticker is "VIX" or "SPY"
                if ticker in ["VIX", "SPY", "VVIX"]:
                    selected_source = 'yfinance'
                    print(f"STRATEGY: Ticker is {ticker}, using Yahoo Finance as primary source")
                    if verbose:
                        print(f"Using Yahoo Finance for {ticker}...", end=" ", flush=True)
                    else:
                        print(f"Using Yahoo Finance for {ticker}...")
                    
                    try:
                        # For VIX, use ^VIX ticker symbol with Yahoo Finance
                        yf_ticker = f"^{ticker}" if ticker in ["VIX", "VVIX"] else ticker
                        print(f"API CALL: Yahoo Finance - Fetching {yf_ticker} from {start_date} to {end_date}")
                        result = upload_prices_yfinance(yf_ticker, start_date, end_date)
                        
                        # Special handling: Save to database with the original ticker
                        if result['success'] and ticker == "VIX":
                            print(f"{Fore.GREEN}Success - Using ^VIX data saved as VIX{Style.RESET_ALL}")
                        
                        if result['success']:
                            if verbose and ticker != "VIX":  # Already printed success for VIX above
                                print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                            success.append(ticker)
                            print(f"SUCCESS: Yahoo Finance data for {ticker} saved to database")
                            continue
                    except Exception as e:
                        if verbose:
                            print(f"{Fore.YELLOW}Failed with Yahoo Finance: {str(e)}, trying next source...{Style.RESET_ALL}")
                        print(f"ERROR: Yahoo Finance failed - {str(e)}")
                        # Fall through to next data source
                
                # Check if end date is within 2 years
                try:
                    today = datetime.now()
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    two_years_ago = today.replace(year=today.year - 2)
                    
                    if end_dt >= two_years_ago:
                        selected_source = 'polygon'
                        print(f"STRATEGY: Date range is within 2 years, using Polygon as primary/next source")
                        if verbose:
                            print(f"Using Polygon for {ticker}...", end=" ", flush=True)
                        else:
                            print(f"Using Polygon for {ticker}...")
                        
                        try:
                            print(f"API CALL: Polygon - Fetching {ticker} from {start_date} to {end_date}")
                            result = upload_prices_polygon(ticker, start_date, end_date)
                            if result['success']:
                                if verbose:
                                    print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                                success.append(ticker)
                                print(f"SUCCESS: Polygon data for {ticker} saved to database")
                                continue
                            elif result['no_data']:
                                print(f"INFO: Polygon returned no data for {ticker}, trying next source")
                        except Exception as e:
                            if verbose:
                                print(f"{Fore.YELLOW}Failed with Polygon: {str(e)}, trying next source...{Style.RESET_ALL}")
                            print(f"ERROR: Polygon failed - {str(e)}")
                            # Fall through to next data source
                except ValueError:
                    # Invalid date format, continue to fallback
                    print(f"WARNING: Invalid date format, skipping date check")
                    pass
                    
                # Fallback to financial datasets
                selected_source = 'financial_datasets'
                print(f"STRATEGY: Using Financial Datasets as fallback/next source")
                if verbose:
                    print(f"Using Financial Datasets for {ticker}...", end=" ", flush=True)
                else:
                    print(f"Using Financial Datasets for {ticker}...")
            else:
                print(f"STRATEGY: Using {selected_source} as specified data source")
            
            # If we're here, either 'auto' logic didn't succeed yet, or a specific source was requested
            if selected_source.lower() == 'yfinance':
                # For VIX, use ^VIX ticker symbol with Yahoo Finance
                yf_ticker = f"^{ticker}" if ticker == "VIX" and selected_source.lower() == 'yfinance' else ticker
                print(f"API CALL: Yahoo Finance - Fetching {yf_ticker} from {start_date} to {end_date}")
                upload_func = lambda t, s, e: upload_prices_yfinance(yf_ticker, s, e)
            elif selected_source.lower() == 'polygon':
                print(f"API CALL: Polygon - Fetching {ticker} from {start_date} to {end_date}")
                upload_func = upload_prices_polygon
            else:  # Default to financial_datasets
                print(f"API CALL: Financial Datasets - Fetching {ticker} from {start_date} to {end_date}")
                upload_func = upload_prices_financialdatasets
                
            try:
                if result is None:  # Only call if not already called above
                    if selected_source.lower() == 'yfinance':
                        # Special handling for direct call with specific source
                        result = upload_func(ticker, start_date, end_date)
                    else:
                        result = upload_func(ticker, start_date, end_date)
                    
                if result['no_data']:
                    if verbose:
                        print(f"{Fore.YELLOW}No data{Style.RESET_ALL}")
                    failed.append(ticker)
                    print(f"INFO: No data found for {ticker} using {selected_source}")
                elif result['success']:
                    if verbose:
                        print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                    success.append(ticker)
                    print(f"SUCCESS: {selected_source} data for {ticker} saved to database")
                else:
                    if verbose:
                        print(f"{Fore.RED}Failed to save{Style.RESET_ALL}")
                    failed.append(ticker)
                    print(f"ERROR: Failed to save {ticker} data from {selected_source}")
            except Exception as e:
                if verbose:
                    print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                failed.append(ticker)
                print(f"EXCEPTION: Error processing {ticker} with {selected_source}: {str(e)}")
            
            print(f"{'-'*50}\n")
                
        print(f"\n{'='*30}")
        print(f"UPLOAD SUMMARY:")
        print(f"Total processed: {len(tickers)}")
        print(f"Success: {len(success)} - {', '.join(success) if success else 'None'}")
        print(f"Failed: {len(failed)} - {', '.join(failed) if failed else 'None'}")
        print(f"{'='*30}\n")
        
        return {'success': success, 'failed': failed}
        
    except Exception as e:
        print(f"{Fore.RED}Error in batch price loading: {e}{Style.RESET_ALL}")
        print(f"CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'success': [], 'failed': tickers}

def upload_prices_polygon(ticker, start_date, end_date):
    """
    Fetch price data using Polygon.io API and save it to the PostgreSQL database.
    Takes ticker, start_date, and end_date as parameters, fetches the data,
    and saves it to the database.
    
    Returns:
        dict: Status object with 'success' (bool) and 'no_data' (bool) fields
    """
    try:
        from src.tools.api_polygon import get_price_polygon
        from src.data.models import Price
        import pandas as pd
        
        print(f"POLYGON API: Initializing request for {ticker}")
        print(f"POLYGON API: Date range {start_date} to {end_date}")
        
        # Check if we have the API key
        import os
        polygon_api_key = os.environ.get('POLYGON_API_KEY')
        if not polygon_api_key:
            print(f"POLYGON API: ERROR - POLYGON_API_KEY not found in environment variables!")
            return {'success': False, 'no_data': False}
        else:
            masked_key = polygon_api_key[:4] + '****' + polygon_api_key[-4:] if len(polygon_api_key) > 8 else '****'
            print(f"POLYGON API: Using API key {masked_key}")
        
        # Get price data from Polygon
        print(f"POLYGON API: Sending request to Polygon.io API")
        price_df = get_price_polygon(ticker, start_date, end_date)
        
        if price_df.empty:
            print(f"POLYGON API: No data returned for {ticker}")
            return {'success': False, 'no_data': True}
        
        print(f"POLYGON API: Received {len(price_df)} records for {ticker}")
        print(f"POLYGON API: First date: {price_df.index[0]}, Last date: {price_df.index[-1]}")
        
        # Convert DataFrame to list of Price objects
        print(f"POLYGON API: Converting data to Price objects")
        prices = []
        for index, row in price_df.iterrows():
            # index is the biz_date in YYYY-MM-DD format
            biz_date = index  # Use the index directly as biz_date
            
            # Generate timestamp with timezone info (UTC)
            timestamp = datetime.strptime(biz_date, '%Y-%m-%d')
            time_str = timestamp.strftime('%Y-%m-%dT00:00:00Z')
            
            price = Price(
                ticker=ticker,
                time=time_str,
                biz_date=biz_date,  # Include explicit biz_date
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=int(row['volume'])
            )
            prices.append(price)
        
        print(f"POLYGON API: Created {len(prices)} Price objects")
        
        # Save to database
        from src.tools.api_db import save_prices
        print(f"POLYGON API: Saving data to database")
        result = save_prices(ticker, prices)
        
        if result:
            print(f"POLYGON API: Successfully saved {len(prices)} records to database")
        else:
            print(f"POLYGON API: Failed to save data to database")
            
        return {'success': result, 'no_data': False}
    except Exception as e:
        print(f"{Fore.RED}Error saving Polygon price data to database: {e}{Style.RESET_ALL}")
        print(f"POLYGON API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'no_data': False}

def upload_prices_yfinance(ticker, start_date, end_date):
    """
    Fetch price data using Yahoo Finance API and save it to the PostgreSQL database.
    Takes ticker, start_date, and end_date as parameters, fetches the data,
    and saves it to the database.
    
    Special handling for VIX ticker:
    - If ticker starts with '^', it's a Yahoo Finance special ticker
    - The data will be saved to the database with the original ticker name (without '^')
    
    Returns:
        dict: Status object with 'success' (bool) and 'no_data' (bool) fields
    """
    try:
        from src.tools.api_yfinance import get_price_yahoofinance
        from src.data.models import Price
        import pandas as pd
        
        # Extract original ticker (for database storage) if it's a special YF ticker
        original_ticker = ticker
        if ticker.startswith('^'):
            original_ticker = ticker[1:]  # Remove the '^' for database storage
            print(f"Will save Yahoo Finance data for {ticker} as {original_ticker} in database")
        
        # Get price data from Yahoo Finance
        price_df = get_price_yahoofinance(ticker, start_date, end_date)
        
        if price_df.empty:
            return {'success': False, 'no_data': True}
        
        # Convert DataFrame to list of Price objects
        prices = []
        for index, row in price_df.iterrows():
            # index is the biz_date in YYYY-MM-DD format
            biz_date = index  # Use the index directly as biz_date
            
            # Generate timestamp with timezone info (UTC)
            timestamp = datetime.strptime(biz_date, '%Y-%m-%d')
            time_str = timestamp.strftime('%Y-%m-%dT00:00:00Z')
            
            price = Price(
                ticker=original_ticker,  # Use the original ticker for storage
                time=time_str,
                biz_date=biz_date,  # Include explicit biz_date
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=int(row['volume'])
            )
            prices.append(price)
        
        # Save to database
        from src.tools.api_db import save_prices
        result = save_prices(original_ticker, prices)
        return {'success': result, 'no_data': False}
    except Exception as e:
        print(f"{Fore.RED}Error saving Yahoo Finance price data to database: {e}{Style.RESET_ALL}")
        return {'success': False, 'no_data': False}

def upload_company_news(tickers, end_date, verbose=False):
    """Load and save company news for multiple tickers to the PostgreSQL database."""
    if not tickers:
        return False
        
    try:
        from src.tools.api import get_company_news
        
        success = []
        failed = []
        
        for ticker in tickers:
            if verbose:
                print(f"Loading news for {ticker}...", end=" ", flush=True)
            
            try:
                news = get_company_news(ticker, end_date)
                if not news:
                    if verbose:
                        print(f"{Fore.YELLOW}No data{Style.RESET_ALL}")
                    failed.append(ticker)
                    continue
                    
                if save_company_news_to_db(ticker, news):
                    if verbose:
                        print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                    success.append(ticker)
                else:
                    if verbose:
                        print(f"{Fore.RED}Failed to save{Style.RESET_ALL}")
                    failed.append(ticker)
            except Exception as e:
                if verbose:
                    print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                failed.append(ticker)
                
        return {'success': success, 'failed': failed}
        
    except Exception as e:
        print(f"{Fore.RED}Error in batch news loading: {e}{Style.RESET_ALL}")
        return False

def save_company_news_to_db(ticker, news):
    """Save company news to the PostgreSQL database using the api_db module."""
    if not news:
        return False
    
    try:
        from src.tools.api_db import save_company_news
        return save_company_news(news)
    except Exception as e:
        print(f"{Fore.RED}Error saving company news to database: {e}{Style.RESET_ALL}")
        return False

def upload_financial_metrics(tickers, end_date, verbose=False):
    """Load and save financial metrics for multiple tickers to the PostgreSQL database."""
    if not tickers:
        return False
        
    try:
        from src.tools.api import get_financial_metrics
        
        success = []
        failed = []
        
        for ticker in tickers:
            if verbose:
                print(f"Loading metrics for {ticker}...", end=" ", flush=True)
            
            try:
                metrics = get_financial_metrics(ticker, end_date)
                if not metrics:
                    if verbose:
                        print(f"{Fore.YELLOW}No data{Style.RESET_ALL}")
                    failed.append(ticker)
                    continue
                    
                if save_financial_metrics_to_db(metrics):
                    if verbose:
                        print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                    success.append(ticker)
                else:
                    if verbose:
                        print(f"{Fore.RED}Failed to save{Style.RESET_ALL}")
                    failed.append(ticker)
            except Exception as e:
                if verbose:
                    print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                failed.append(ticker)
                
        return {'success': success, 'failed': failed}
        
    except Exception as e:
        print(f"{Fore.RED}Error in batch metrics loading: {e}{Style.RESET_ALL}")
        return False

def save_financial_metrics_to_db(metrics):
    """Save financial metrics to the PostgreSQL database using the api_db module."""
    if not metrics:
        return False
    
    try:
        from src.tools.api_db import save_financial_metrics
        return save_financial_metrics(metrics)
    except Exception as e:
        print(f"{Fore.RED}Error saving financial metrics to database: {e}{Style.RESET_ALL}")
        return False

def upload_insider_trades(tickers, end_date, verbose=False):
    """Load and save insider trades for multiple tickers to the PostgreSQL database."""
    if not tickers:
        return False
        
    try:
        from src.tools.api import get_insider_trades
        
        success = []
        failed = []
        
        for ticker in tickers:
            if verbose:
                print(f"Loading insider trades for {ticker}...", end=" ", flush=True)
            
            try:
                trades = get_insider_trades(ticker, end_date)
                if not trades:
                    if verbose:
                        print(f"{Fore.YELLOW}No data{Style.RESET_ALL}")
                    failed.append(ticker)
                    continue
                    
                if save_insider_trades_to_db(ticker, trades):
                    if verbose:
                        print(f"{Fore.GREEN}Success{Style.RESET_ALL}")
                    success.append(ticker)
                else:
                    if verbose:
                        print(f"{Fore.RED}Failed to save{Style.RESET_ALL}")
                    failed.append(ticker)
            except Exception as e:
                if verbose:
                    print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                failed.append(ticker)
                
        return {'success': success, 'failed': failed}
        
    except Exception as e:
        print(f"{Fore.RED}Error in batch insider trades loading: {e}{Style.RESET_ALL}")
        return False

def save_insider_trades_to_db(ticker, trades):
    """Save insider trades to the PostgreSQL database using the api_db module."""
    if not trades:
        return False
    
    try:
        from src.tools.api_db import save_insider_trades
        return save_insider_trades(trades)
    except Exception as e:
        print(f"{Fore.RED}Error saving insider trades to database: {e}{Style.RESET_ALL}")
        return False

def upload_line_items(tickers, end_date, verbose=False):
    """Batch load line items for multiple tickers to PostgreSQL database."""
    if not tickers:
        return {'success': [], 'failed': []}
        
    try:
        from src.tools.api import search_line_items
        
        if verbose:
            print(f"Loading line items for {len(tickers)} tickers...")
        
        all_line_items = search_line_items(
            tickers=tickers,
            line_items=LINE_ITEMS,
            end_date=end_date
        )

        if not all_line_items:
            if verbose:
                print(f"{Fore.YELLOW}No line items found{Style.RESET_ALL}")
            return {'success': [], 'failed': tickers}
        
        # Group by ticker
        ticker_items = {}
        for item in all_line_items:
            ticker_items.setdefault(item.ticker, []).append(item)
        
        # Process results
        success = []
        failed = []
        
        for ticker in tickers:
            items = ticker_items.get(ticker, [])
            if not items:
                if verbose:
                    print(f"{Fore.YELLOW}No items for {ticker}{Style.RESET_ALL}")
                failed.append(ticker)
                continue
                
            if save_line_items_to_db(ticker, items):
                success.append(ticker)
            else:
                failed.append(ticker)
                
        if verbose:
            print(f"{Fore.GREEN}Saved line items for {len(success)}/{len(tickers)} tickers{Style.RESET_ALL}")
            
        return {'success': success, 'failed': failed}
        
    except Exception as e:
        print(f"{Fore.RED}Error in line items batch load: {e}{Style.RESET_ALL}")
        return {'success': [], 'failed': tickers}

def save_line_items_to_db(ticker, line_items):
    """Save line items to the PostgreSQL database using the api_db module."""
    if not line_items:
        return False
    
    try:
        from src.tools.api_db import save_line_items
        return save_line_items(ticker, line_items)
    except Exception as e:
        print(f"{Fore.RED}Error saving line items to database: {e}{Style.RESET_ALL}")
        return False

def upload_ai_analysis_result(agent_name, analysis_data, biz_date, state=None):
    """Save AI agent analysis data to unified ai_analysis table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for ticker, data in analysis_data.items():
            if data["confidence"] == 0:
                continue
                
            cursor.execute(
                """
                INSERT INTO ai_analysis (
                    ticker, agent, signal, confidence, reasoning, 
                    model_name, model_display_name, biz_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, agent, biz_date, model_display_name) 
                DO UPDATE SET
                    signal = EXCLUDED.signal,
                    confidence = EXCLUDED.confidence,
                    reasoning = EXCLUDED.reasoning,
                    model_name = EXCLUDED.model_name,
                    model_display_name = EXCLUDED.model_display_name,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    ticker,
                    agent_name.replace("_agent", ""),  # Remove '_agent' suffix
                    data["signal"],
                    data["confidence"],
                    json.dumps(data["reasoning"]) if isinstance(data["reasoning"], dict) else data["reasoning"],
                    state["metadata"]["model_name"] if state else None,
                    state["metadata"]["model_provider"] if state else None,
                    biz_date
                )
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving AI analysis data: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def upload_valuation_result(valuation_data):
    """Save valuation data to the database with UPSERT functionality."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for ticker, data in valuation_data.items():
            for method in data["detail"]:
                cursor.execute(
                    """
                    INSERT INTO valuation (
                        ticker,
                        valuation_method,
                        intrinsic_value,
                        market_cap,
                        gap,
                        signal,
                        biz_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, valuation_method, biz_date) 
                    DO UPDATE SET
                        intrinsic_value = EXCLUDED.intrinsic_value,
                        market_cap = EXCLUDED.market_cap,
                        gap = EXCLUDED.gap,
                        signal = EXCLUDED.signal,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        ticker,
                        method["valuation_method"],
                        method["intrinsic_value"],
                        method["market_cap"],
                        method["gap"],
                        method["signal"],
                        method["biz_date"]
                    )
                )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving valuation data: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def upload_fundamentals_result(fundamentals_data, biz_date):
    """Save fundamentals analysis data to database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for ticker, data in fundamentals_data.items():
            detail = data["detail"]
            cursor.execute(
                """
                INSERT INTO fundamentals (
                    ticker, biz_date, overall_signal, confidence,
                    return_on_equity, net_margin, operating_margin,
                    profitability_score, profitability_signal,
                    revenue_growth, earnings_growth, book_value_growth,
                    growth_score, growth_signal,
                    current_ratio, debt_to_equity, free_cash_flow_per_share, earnings_per_share,
                    health_score, health_signal,
                    pe_ratio, pb_ratio, ps_ratio,
                    valuation_score, valuation_signal
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (ticker, biz_date) 
                DO UPDATE SET
                    overall_signal = EXCLUDED.overall_signal,
                    confidence = EXCLUDED.confidence,
                    return_on_equity = EXCLUDED.return_on_equity,
                    net_margin = EXCLUDED.net_margin,
                    operating_margin = EXCLUDED.operating_margin,
                    profitability_score = EXCLUDED.profitability_score,
                    profitability_signal = EXCLUDED.profitability_signal,
                    revenue_growth = EXCLUDED.revenue_growth,
                    earnings_growth = EXCLUDED.earnings_growth,
                    book_value_growth = EXCLUDED.book_value_growth,
                    growth_score = EXCLUDED.growth_score,
                    growth_signal = EXCLUDED.growth_signal,
                    current_ratio = EXCLUDED.current_ratio,
                    debt_to_equity = EXCLUDED.debt_to_equity,
                    free_cash_flow_per_share = EXCLUDED.free_cash_flow_per_share,
                    earnings_per_share = EXCLUDED.earnings_per_share,
                    health_score = EXCLUDED.health_score,
                    health_signal = EXCLUDED.health_signal,
                    pe_ratio = EXCLUDED.pe_ratio,
                    pb_ratio = EXCLUDED.pb_ratio,
                    ps_ratio = EXCLUDED.ps_ratio,
                    valuation_score = EXCLUDED.valuation_score,
                    valuation_signal = EXCLUDED.valuation_signal,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    ticker, biz_date, data["signal"], data["confidence"],
                    detail["profitability"]["return_on_equity"],
                    detail["profitability"]["net_margin"],
                    detail["profitability"]["operating_margin"],
                    detail["profitability"]["score"],
                    detail["profitability"]["signal"],
                    detail["growth"]["revenue_growth"],
                    detail["growth"]["earnings_growth"],
                    detail["growth"]["book_value_growth"],
                    detail["growth"]["score"],
                    detail["growth"]["signal"],
                    detail["financial_health"]["current_ratio"],
                    detail["financial_health"]["debt_to_equity"],
                    detail["financial_health"]["free_cash_flow_per_share"],
                    detail["financial_health"]["earnings_per_share"],
                    detail["financial_health"]["score"],
                    detail["financial_health"]["signal"],
                    detail["valuation"]["pe_ratio"],
                    detail["valuation"]["pb_ratio"],
                    detail["valuation"]["ps_ratio"],
                    detail["valuation"]["score"],
                    detail["valuation"]["signal"]
                )
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving fundamentals data: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def upload_sentiment_result(sentiment_data):
    """Save sentiment data to the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for ticker, data in sentiment_data.items():
            detail = data["detail"]
            cursor.execute(
                """
                INSERT INTO sentiment (
                    ticker, biz_date, overall_signal, confidence,
                    insider_total, insider_bullish, insider_bearish,
                    insider_value_total, insider_value_bullish, insider_value_bearish,
                    insider_weight, news_total, news_bullish, news_bearish, news_neutral,
                    news_weight, weighted_bullish, weighted_bearish
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (ticker, biz_date) 
                DO UPDATE SET
                    overall_signal = EXCLUDED.overall_signal,
                    confidence = EXCLUDED.confidence,
                    insider_total = EXCLUDED.insider_total,
                    insider_bullish = EXCLUDED.insider_bullish,
                    insider_bearish = EXCLUDED.insider_bearish,
                    insider_value_total = EXCLUDED.insider_value_total,
                    insider_value_bullish = EXCLUDED.insider_value_bullish,
                    insider_value_bearish = EXCLUDED.insider_value_bearish,
                    news_total = EXCLUDED.news_total,
                    news_bullish = EXCLUDED.news_bullish,
                    news_bearish = EXCLUDED.news_bearish,
                    news_neutral = EXCLUDED.news_neutral,
                    weighted_bullish = EXCLUDED.weighted_bullish,
                    weighted_bearish = EXCLUDED.weighted_bearish,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    ticker, detail["biz_date"], data["signal"], data["confidence"],
                    detail["insider_total"], detail["insider_bullish"], detail["insider_bearish"],
                    detail["insider_value_total"], detail["insider_value_bullish"], detail["insider_value_bearish"],
                    detail["insider_weight"], detail["news_total"], detail["news_bullish"], detail["news_bearish"], detail["news_neutral"],
                    detail["news_weight"], detail["weighted_bullish"], detail["weighted_bearish"]
                )
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving sentiment data: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def upload_technical_result(technical_data: dict, biz_date: str):
    """Save technical analysis data to database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for ticker, data in technical_data.items():
            strategies = data["strategy_signals"]
            
            cursor.execute(
                """
                INSERT INTO technicals (
                    ticker, biz_date, signal, confidence,
                    -- Trend Following
                    trend_signal, trend_confidence, trend_score,
                    trend_adx_threshold, trend_ema_crossover_threshold,
                    ema_8, ema_21, ema_55, adx, di_plus, di_minus,
                    -- Mean Reversion
                    mr_signal, mr_confidence, mr_score,
                    mr_z_score_threshold, mr_rsi_low_threshold, mr_rsi_high_threshold,
                    z_score, bb_upper, bb_lower, rsi_14, rsi_28,
                    -- Momentum
                    momentum_signal, momentum_confidence, momentum_score,
                    momentum_min_strength, momentum_volume_ratio_threshold,
                    mom_1m, mom_3m, mom_6m, volume_ratio,
                    -- Volatility
                    volatility_signal, volatility_confidence, volatility_score,
                    volatility_low_regime, volatility_high_regime, volatility_z_threshold,
                    hist_vol_21d, vol_regime, vol_z_score, atr_ratio,
                    -- Statistical Arbitrage
                    stat_arb_signal, stat_arb_confidence, stat_arb_score,
                    stat_arb_hurst_threshold, stat_arb_skew_threshold,
                    hurst_exp, skewness, kurtosis
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (ticker, biz_date) 
                DO UPDATE SET
                    signal = EXCLUDED.signal,
                    confidence = EXCLUDED.confidence,
                    -- Trend Following
                    trend_signal = EXCLUDED.trend_signal,
                    trend_confidence = EXCLUDED.trend_confidence,
                    trend_score = EXCLUDED.trend_score,
                    ema_8 = EXCLUDED.ema_8,
                    ema_21 = EXCLUDED.ema_21,
                    ema_55 = EXCLUDED.ema_55,
                    adx = EXCLUDED.adx,
                    di_plus = EXCLUDED.di_plus,
                    di_minus = EXCLUDED.di_minus,
                    -- Mean Reversion
                    mr_signal = EXCLUDED.mr_signal,
                    mr_confidence = EXCLUDED.mr_confidence,
                    mr_score = EXCLUDED.mr_score,
                    z_score = EXCLUDED.z_score,
                    bb_upper = EXCLUDED.bb_upper,
                    bb_lower = EXCLUDED.bb_lower,
                    rsi_14 = EXCLUDED.rsi_14,
                    rsi_28 = EXCLUDED.rsi_28,
                    -- Momentum
                    momentum_signal = EXCLUDED.momentum_signal,
                    momentum_confidence = EXCLUDED.momentum_confidence,
                    momentum_score = EXCLUDED.momentum_score,
                    mom_1m = EXCLUDED.mom_1m,
                    mom_3m = EXCLUDED.mom_3m,
                    mom_6m = EXCLUDED.mom_6m,
                    volume_ratio = EXCLUDED.volume_ratio,
                    -- Volatility
                    volatility_signal = EXCLUDED.volatility_signal,
                    volatility_confidence = EXCLUDED.volatility_confidence,
                    volatility_score = EXCLUDED.volatility_score,
                    hist_vol_21d = EXCLUDED.hist_vol_21d,
                    vol_regime = EXCLUDED.vol_regime,
                    vol_z_score = EXCLUDED.vol_z_score,
                    atr_ratio = EXCLUDED.atr_ratio,
                    -- Statistical Arbitrage
                    stat_arb_signal = EXCLUDED.stat_arb_signal,
                    stat_arb_confidence = EXCLUDED.stat_arb_confidence,
                    stat_arb_score = EXCLUDED.stat_arb_score,
                    hurst_exp = EXCLUDED.hurst_exp,
                    skewness = EXCLUDED.skewness,
                    kurtosis = EXCLUDED.kurtosis,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    ticker, biz_date, data["signal"], data["confidence"],
                    # Trend Following
                    strategies["trend_following"]["signal"],
                    strategies["trend_following"]["confidence"],
                    strategies["trend_following"]["metrics"].get("trend_strength", 0),
                    25.0, True,  # Default thresholds
                    strategies["trend_following"]["metrics"].get("ema_8", 0),
                    strategies["trend_following"]["metrics"].get("ema_21", 0),
                    strategies["trend_following"]["metrics"].get("ema_55", 0),
                    strategies["trend_following"]["metrics"].get("adx", 0),
                    strategies["trend_following"]["metrics"].get("di_plus", 0),
                    strategies["trend_following"]["metrics"].get("di_minus", 0),
                    # Mean Reversion
                    strategies["mean_reversion"]["signal"],
                    strategies["mean_reversion"]["confidence"],
                    strategies["mean_reversion"]["metrics"].get("z_score", 0),
                    2.0, 30.0, 70.0,  # Default thresholds
                    strategies["mean_reversion"]["metrics"].get("z_score", 0),
                    strategies["mean_reversion"]["metrics"].get("bb_upper", 0),
                    strategies["mean_reversion"]["metrics"].get("bb_lower", 0),
                    strategies["mean_reversion"]["metrics"].get("rsi_14", 0),
                    strategies["mean_reversion"]["metrics"].get("rsi_28", 0),
                    # Momentum
                    strategies["momentum"]["signal"],
                    strategies["momentum"]["confidence"],
                    strategies["momentum"]["metrics"].get("momentum_6m", 0),
                    0.05, 1.0,  # Default thresholds
                    strategies["momentum"]["metrics"].get("momentum_1m", 0),
                    strategies["momentum"]["metrics"].get("momentum_3m", 0),
                    strategies["momentum"]["metrics"].get("momentum_6m", 0),
                    strategies["momentum"]["metrics"].get("volume_momentum", 0),
                    # Volatility
                    strategies["volatility"]["signal"],
                    strategies["volatility"]["confidence"],
                    strategies["volatility"]["metrics"].get("volatility_z_score", 0),
                    0.8, 1.2, 1.0,  # Default thresholds
                    strategies["volatility"]["metrics"].get("historical_volatility", 0),
                    strategies["volatility"]["metrics"].get("volatility_regime", 0),
                    strategies["volatility"]["metrics"].get("volatility_z_score", 0),
                    strategies["volatility"]["metrics"].get("atr_ratio", 0),
                    # Statistical Arbitrage
                    strategies["statistical_arbitrage"]["signal"],
                    strategies["statistical_arbitrage"]["confidence"],
                    strategies["statistical_arbitrage"]["metrics"].get("hurst_exponent", 0.5),
                    0.4, 1.0,  # Default thresholds
                    strategies["statistical_arbitrage"]["metrics"].get("hurst_exponent", 0.5),
                    strategies["statistical_arbitrage"]["metrics"].get("skewness", 0),
                    strategies["statistical_arbitrage"]["metrics"].get("kurtosis", 0)
                )
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error saving technical data: {e}")
    finally:
        cursor.close()
        conn.close()

def upload_company_news_alphavantage(tickers, time_from=None, time_to=None, limit=1000):
    """
    Fetch and upload Alpha Vantage news sentiment for a list of tickers to the company_news_alphavantage table.
    Ensures no duplicate news per (ticker, url) using ON CONFLICT.
    Returns a dict with 'success' and 'failed' keys for upload status.
    """
    import pprint
    if not tickers:
        print("[LOG] No tickers provided.")
        return {'success': [], 'failed': []}
    
    # Map GOOGL to GOOG for API call, but keep a reverse map for result remapping
    api_tickers = []
    ticker_map = {}
    for t in tickers:
        if t == 'GOOGL':
            api_tickers.append('GOOG')
            ticker_map['GOOG'] = 'GOOGL'
        else:
            api_tickers.append(t)
            ticker_map[t] = t
    print(f"[LOG] Fetching news sentiment for tickers: {api_tickers} (from {time_from} to {time_to}, limit={limit})")
    news_data = get_news_sentiment_multi(api_tickers, time_from, time_to, limit)
    print(f"[LOG] News data: {len(news_data)}")
    all_news = []
    results = {}
    for api_ticker, data in news_data.items():
        orig_ticker = ticker_map.get(api_ticker, api_ticker)
        print(f"[LOG] Ticker: {orig_ticker} (API: {api_ticker}) - API response keys: {list(data.keys()) if data else data}")
        if not data or 'feed' not in data or not data['feed']:
            print(f"[LOG] No news feed found for {orig_ticker}. Data: {pprint.pformat(data)}")
            results[orig_ticker] = False
            continue
        print(f"[LOG] {orig_ticker}: {len(data['feed'])} news items found.")
        for item in data['feed']:
            ticker_sentiments = item.get('ticker_sentiment', [])
            if not ticker_sentiments:
                all_news.append({
                    'ticker': orig_ticker,
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'time_published': _format_time_db(item.get('time_published', '')),
                    'date': _format_date_db(item.get('time_published', '')),
                    'source': item.get('source', ''),
                    'author': "; ".join(item.get('authors', [])),
                    'summary': item.get('summary', ''),
                    'overall_sentiment_score': item.get('overall_sentiment_score', 0),
                    'overall_sentiment_label': item.get('overall_sentiment_label', ''),
                    'ticker_sentiment_score': 0,
                    'ticker_relevance_score': 0,
                    'sentiment': '',
                })
            else:
                for ticker_sent in ticker_sentiments:
                    # Always map back to orig_ticker for DB
                    if ticker_sent.get('ticker') == api_ticker:
                        all_news.append({
                            'ticker': orig_ticker,
                            'title': item.get('title', ''),
                            'url': item.get('url', ''),
                            'time_published': _format_time_db(item.get('time_published', '')),
                            'date': _format_date_db(item.get('time_published', '')),
                            'source': item.get('source', ''),
                            'author': "; ".join(item.get('authors', [])),
                            'summary': item.get('summary', ''),
                            'overall_sentiment_score': float(item.get('overall_sentiment_score', 0)),
                            'overall_sentiment_label': item.get('overall_sentiment_label', ''),
                            'ticker_sentiment_score': float(ticker_sent.get('ticker_sentiment_score', '0')),
                            'ticker_relevance_score': float(ticker_sent.get('relevance_score', '0')),
                            'sentiment': ticker_sent.get('ticker_sentiment_label', ''),
                        })
        results[orig_ticker] = True
    if all_news:
        print(f"[LOG] Uploading {len(all_news)} news items to DB...")
        upload_result = _upload_company_news_alphavantage_rows(all_news)
        print(f"[LOG] DB upload result: {upload_result}")
    else:
        print("[LOG] No news to upload to DB.")
    # Return in the expected format for batch upload
    return {
        'success': [ticker for ticker, ok in results.items() if ok],
        'failed': [ticker for ticker, ok in results.items() if not ok],
    }

def _format_time_db(time_str):
    """Convert API time format (YYYYMMDDTHHMMSS or YYYYMMDDTHHMM) to DB timestamp string or original string if parsing fails."""
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            continue
    return time_str if time_str else None

def _format_date_db(time_str):
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    # If already in YYYY-MM-DD format, return as is
    if isinstance(time_str, str) and len(time_str) == 10 and time_str[4] == '-' and time_str[7] == '-':
        return time_str
    return None

def _upload_company_news_alphavantage_rows(news_list):
    # This is the original upload logic for a list of news dicts
    if not news_list:
        return False
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        fields = [
            'ticker', 'title', 'url', 'time_published', 'date', 'source', 'author', 'summary',
            'overall_sentiment_score', 'overall_sentiment_label', 'ticker_sentiment_score',
            'ticker_relevance_score', 'sentiment'
        ]
        values = [
            tuple(news.get(field) for field in fields)
            for news in news_list
        ]
        sql = f"""
        INSERT INTO company_news_alphavantage ({', '.join(fields)})
        VALUES %s
        ON CONFLICT (ticker, url) DO UPDATE SET
            title = EXCLUDED.title,
            time_published = EXCLUDED.time_published,
            date = EXCLUDED.date,
            source = EXCLUDED.source,
            author = EXCLUDED.author,
            summary = EXCLUDED.summary,
            overall_sentiment_score = EXCLUDED.overall_sentiment_score,
            overall_sentiment_label = EXCLUDED.overall_sentiment_label,
            ticker_sentiment_score = EXCLUDED.ticker_sentiment_score,
            ticker_relevance_score = EXCLUDED.ticker_relevance_score,
            sentiment = EXCLUDED.sentiment,
            updated_at = CURRENT_TIMESTAMP
        """
        execute_values(cursor, sql, values)
        conn.commit()
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error uploading Alpha Vantage news: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_db_connection():
    """Get a database connection using environment variables."""
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        return psycopg2.connect(db_url)
    except Exception as e:
        print(f"{Fore.RED}Error connecting to database: {e}{Style.RESET_ALL}")
        raise
