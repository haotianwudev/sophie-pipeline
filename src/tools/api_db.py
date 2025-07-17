"""
API functions that use the PostgreSQL database instead of external APIs.
These functions can be used as replacements or fallbacks for the original API functions.
"""

import os
import datetime
import json
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor, execute_values
from src.data.models import CompanyFacts, Price, FinancialMetrics, LineItem, InsiderTrade, CompanyNews
from dotenv import load_dotenv
from colorama import Fore, Style

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get a connection to the database."""
    # Get database connection parameters from environment variables
    db_user = os.environ.get("DB_USER", "")
    db_password = os.environ.get("DB_PASSWORD", "")
    db_host = os.environ.get("DB_HOST", "")
    db_name = os.environ.get("DB_NAME", "")
    db_sslmode = os.environ.get("DB_SSLMODE", "require")
    
    # Build connection string
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode={db_sslmode}"
    
    # Fallback to direct connection string if provided (for backward compatibility)
    connection_string = os.environ.get("DATABASE_URL", connection_string)
    
    return psycopg2.connect(connection_string)

def get_company_facts_db(ticker: str) -> CompanyFacts | None:
    """Fetch company facts from the PostgreSQL database."""
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query the database
        cursor.execute("SELECT * FROM company_facts WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not result:
            return None
        
        # Convert date strings to the format expected by the model
        if result.get('listing_date'):
            result['listing_date'] = result['listing_date'].isoformat()
            
        # Convert the result to a CompanyFacts object
        return CompanyFacts(**result)
        
    except Exception as e:
        print(f"Error fetching company facts from database: {e}")
        return None

def get_market_cap_db(ticker: str) -> float | None:
    """Fetch market cap from the PostgreSQL database."""
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query the database
        cursor.execute("SELECT market_cap FROM company_facts WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not result:
            return None
        
        return result[0]
        
    except Exception as e:
        print(f"Error fetching market cap from database: {e}")
        return None

def save_company_facts(company_facts: CompanyFacts) -> bool:
    """Save company facts to the PostgreSQL database."""
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Prepare data for insert/update
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
        print(f"Error saving company facts to database: {e}")
        return False

def get_prices_db(ticker: str, start_date: str, end_date: str) -> list[Price] | None:
    """Fetch price data from the PostgreSQL database for a specific date range."""
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query the database
        cursor.execute(
            "SELECT * FROM prices WHERE ticker = %s AND biz_date >= %s AND biz_date <= %s ORDER BY time DESC", 
            (ticker, start_date, end_date)
        )
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
        
        # Format dates and convert to Price objects
        prices = []
        for result in results:
            # Convert datetime to ISO format string
            result['time'] = result['time'].isoformat()
            # Create a Price object
            prices.append(Price(**{
                'ticker': result['ticker'], 
                'open': float(result['open']),
                'close': float(result['close']),
                'high': float(result['high']),
                'low': float(result['low']),
                'volume': int(result['volume']),
                'time': result['time'],
                'biz_date': result['biz_date'].isoformat() if result['biz_date'] else None  
            }))
        
        return prices
        
    except Exception as e:
        print(f"Error fetching price data from database: {e}")
        return None

def save_prices(ticker: str, prices: list[Price]) -> bool:
    """Save price data to the PostgreSQL database."""
    if not prices:
        return False
        
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert records
        insert_count = 0
        for price in prices:
            try:
                # Use biz_date from the object if available, otherwise extract from time
                if hasattr(price, 'biz_date') and price.biz_date:
                    # Use the provided biz_date
                    biz_date = price.biz_date
                    if isinstance(biz_date, str):
                        # Convert string to date object if needed
                        biz_date = datetime.datetime.strptime(biz_date, '%Y-%m-%d').date()
                else:
                    # Extract date from time for biz_date (legacy method)
                    time_obj = datetime.datetime.fromisoformat(price.time.replace('Z', '+00:00'))
                    biz_date = time_obj.date()
                
                sql = """
                INSERT INTO prices (ticker, time, biz_date, open, close, high, low, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, biz_date) DO UPDATE SET
                    time = EXCLUDED.time,
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(sql, (
                    ticker,
                    price.time,
                    biz_date,
                    price.open,
                    price.close,
                    price.high,
                    price.low,
                    price.volume
                ))
                insert_count += 1
            except Exception as inner_e:
                print(f"Error inserting price for {ticker} on {price.time}: {inner_e}")
        
        # Commit the transaction
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        print(f"Successfully saved {insert_count} price records for {ticker}")
        return True
        
    except Exception as e:
        print(f"Error saving price data to database: {e}")
        return False

def get_financial_metrics_db(
    ticker: str, 
    end_date: str, 
    period: str = "ttm", 
    limit: int = 10
) -> list[FinancialMetrics] | None:
    """
    Fetch financial metrics from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering metrics (only metrics with report_period <= end_date)
        period: The reporting period (e.g., "ttm", "annual", "quarterly")
        limit: Maximum number of records to return
        
    Returns:
        A list of FinancialMetrics objects or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query the database
        cursor.execute(
            """
            SELECT * FROM financial_metrics 
            WHERE ticker = %s 
              AND report_period <= %s 
              AND period = %s 
            ORDER BY report_period DESC 
            LIMIT %s
            """, 
            (ticker, end_date, period, limit)
        )
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
        
        # Convert to FinancialMetrics objects
        metrics = []
        for result in results:
            # Convert dates to string format
            result['report_period'] = result['report_period'].isoformat()
            
            # Create a FinancialMetrics object
            metrics.append(FinancialMetrics(**result))
        
        return metrics
        
    except Exception as e:
        print(f"Error fetching financial metrics from database: {e}")
        return None

def save_financial_metrics(metrics: list[FinancialMetrics]) -> bool:
    """
    Save financial metrics to the PostgreSQL database.
    
    Args:
        metrics: A list of FinancialMetrics objects to save
        
    Returns:
        Boolean indicating success or failure
    """
    if not metrics:
        return False
        
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert records
        insert_count = 0
        for metric in metrics:
            try:
                data = metric.model_dump()
                
                # Build field lists
                fields = list(data.keys())
                
                # Generate placeholders
                placeholders = ', '.join(['%s'] * len(fields))
                fields_str = ', '.join(fields)
                update_fields = ', '.join([f"{field} = EXCLUDED.{field}" for field in fields])
                update_fields += ", updated_at = CURRENT_TIMESTAMP"
                
                # Build SQL query
                sql = f"""
                INSERT INTO financial_metrics ({fields_str})
                VALUES ({placeholders})
                ON CONFLICT (ticker, report_period, period) DO UPDATE SET {update_fields}
                """
                
                # Execute query
                cursor.execute(sql, [data[field] for field in fields])
                insert_count += 1
                
            except Exception as inner_e:
                print(f"Error inserting financial metrics for {metric.ticker} on {metric.report_period}: {inner_e}")
        
        # Commit the transaction
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        print(f"Successfully saved {insert_count} financial metrics records")
        return True
        
    except Exception as e:
        print(f"Error saving financial metrics to database: {e}")
        return False

def get_line_items_db(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[LineItem] | None:
    """
    Fetch line items from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        line_items: List of line item names to fetch
        end_date: The end date for filtering line items
        period: The reporting period (e.g., "ttm", "annual", "quarterly")
        limit: Maximum number of records to return
        
    Returns:
        A list of LineItem objects or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # With the new schema, we need to select specific columns from the line_items table
        # Convert the line_items list to a comma-separated string of column names
        line_items_columns = ', '.join(line_items)
        
        # Build the SQL query to select all requested columns
        sql = f"""
        SELECT 
            ticker, report_period, period, currency, 
            {line_items_columns}
        FROM line_items 
        WHERE ticker = %s 
          AND report_period <= %s 
          AND period = %s
        ORDER BY report_period DESC
        LIMIT %s
        """
        
        # Execute the query
        cursor.execute(sql, (ticker, end_date, period, limit))
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
        
        # Convert the results directly to LineItem objects
        # Each row already has the structure we need
        line_items_objects = []
        for result in results:
            # Convert date to string format
            result['report_period'] = result['report_period'].isoformat()
            
            # Create a LineItem object
            line_items_objects.append(LineItem(**result))
        
        # Sort by report_period in descending order (should already be sorted by the query)
        line_items_objects.sort(key=lambda x: x.report_period, reverse=True)
        
        return line_items_objects
        
    except Exception as e:
        print(f"Error fetching line items from database: {e}")
        return None

def save_line_items(ticker: str, line_items: list[LineItem]) -> bool:
    """
    Save line items to the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        line_items: List of LineItem objects to save
        
    Returns:
        Boolean indicating success or failure
    """
    if not line_items:
        return False
        
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert records
        insert_count = 0
        for item in line_items:
            try:
                # Convert object to dictionary
                data = item.model_dump()
                
                # Extract standard fields that are part of the table schema
                ticker_value = data.get('ticker', ticker)  # Use provided ticker if not in item
                report_period = data.get('report_period')
                period = data.get('period')
                currency = data.get('currency')
                
                # Start building the SQL query - add all known financial metrics
                fields = ['ticker', 'report_period', 'period', 'currency']
                values = [ticker_value, report_period, period, currency]
                
                # Add financial metrics that exist in the data
                for field_name, field_value in data.items():
                    if field_name in ['ticker', 'report_period', 'period', 'currency']:
                        continue  # Skip standard fields already added
                    
                    fields.append(field_name)
                    values.append(field_value)
                
                # Build the SQL query with fields that exist
                placeholders = ', '.join(['%s'] * len(fields))
                fields_str = ', '.join(fields)
                
                # Create update clause for the fields
                update_clauses = []
                for field in fields:
                    if field not in ['ticker', 'report_period', 'period']:  # Skip key fields
                        update_clauses.append(f"{field} = EXCLUDED.{field}")
                update_str = ', '.join(update_clauses)
                update_str += ", updated_at = CURRENT_TIMESTAMP"
                
                sql = f"""
                INSERT INTO line_items ({fields_str})
                VALUES ({placeholders})
                ON CONFLICT (ticker, report_period, period) DO UPDATE SET
                    {update_str}
                """
                
                cursor.execute(sql, values)
                insert_count += 1
                    
            except Exception as inner_e:
                print(f"Error inserting line items for {ticker} on {item.report_period}: {inner_e}")
        
        # Commit the transaction
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        print(f"Successfully saved {insert_count} line items records")
        return True
        
    except Exception as e:
        print(f"Error saving line items to database: {e}")
        return False

def get_insider_trades_db(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade] | None:
    """
    Fetch insider trades from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering trades
        start_date: Optional start date for filtering trades
        limit: Maximum number of records to return
        
    Returns:
        A list of InsiderTrade objects or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build the SQL query
        sql = """
        SELECT * FROM insider_trades
        WHERE ticker = %s AND filing_date <= %s
        """
        params = [ticker, end_date]
        
        if start_date:
            sql += " AND filing_date >= %s"
            params.append(start_date)
            
        sql += " ORDER BY filing_date DESC LIMIT %s"
        params.append(limit)
        
        # Query the database
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
        
        # Convert to InsiderTrade objects
        trades = []
        for result in results:
            # Convert dates to string format
            if result.get('transaction_date'):
                result['transaction_date'] = result['transaction_date'].isoformat()
            result['filing_date'] = result['filing_date'].isoformat()
            
            # Create an InsiderTrade object
            trades.append(InsiderTrade(**result))
        
        return trades
        
    except Exception as e:
        print(f"Error fetching insider trades from database: {e}")
        return None

def save_insider_trades(trades: list[InsiderTrade]) -> bool:
    """
    Save insider trades to the PostgreSQL database.
    
    Args:
        trades: List of InsiderTrade objects to save
        
    Returns:
        Boolean indicating success or failure
    """
    if not trades:
        return False
        
    try:
        # Convert all trades to dictionaries
        trades_dicts = [trade.model_dump() for trade in trades]
        
        # Create a pandas DataFrame from the list of dictionaries
        df = pd.DataFrame(trades_dicts)
        
        # Filter rows with valid transaction dates
        df = df[df['transaction_date'].notna()]
        
        if df.empty:
            print(f"No valid insider trades to save after filtering")
            return False
        
        # Define the columns to group by
        group_cols = [
            'ticker', 'issuer', 'name', 'title', 'is_board_director', 
            'transaction_date', 'security_title', 'filing_date'
        ]
        
        # Define the columns to aggregate
        agg_cols = [
            'transaction_shares', 'transaction_price_per_share', 
            'transaction_value', 'shares_owned_before_transaction', 
            'shares_owned_after_transaction'
        ]
        
        # Make sure all required columns exist in the dataframe
        for col in group_cols + agg_cols:
            if col not in df.columns:
                df[col] = None
        
        # Group by the specified columns and aggregate the transaction data
        # For transaction_price_per_share, use mean; for others, use sum
        agg_dict = {col: 'sum' for col in agg_cols}
        agg_dict['transaction_price_per_share'] = 'mean'  # Use mean for price
        
        # Perform grouping and aggregation
        df_grouped = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
        
        # Add created_at and updated_at fields
        current_time = datetime.datetime.now()
        df_grouped['created_at'] = current_time
        df_grouped['updated_at'] = current_time
        
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # For each ticker, delete existing records for that ticker
        # This is a safer and more reliable approach than trying to match on all fields
        tickers = df_grouped['ticker'].unique()
        
        for ticker in tickers:
            cursor.execute("DELETE FROM insider_trades WHERE ticker = %s", (ticker,))
            print(f"Deleted existing insider trades for {ticker}")
        
        # Now insert all the new records
        columns = list(df_grouped.columns)
        
        # Convert DataFrame to list of tuples for insertion
        values = [tuple(x) for x in df_grouped.values]
        
        # Insert all records at once using execute_values
        insert_sql = f"INSERT INTO insider_trades ({', '.join(columns)}) VALUES %s"
        execute_values(cursor, insert_sql, values)
        
        # Commit the transaction
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Print summary
        print(f"Saved {len(df_grouped)} insider trade records after deduplication")
        return True
        
    except Exception as e:
        print(f"{Fore.RED}Error saving insider trades: {e}{Style.RESET_ALL}")
        return False

def get_company_news_db(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews] | None:
    """
    Fetch company news from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering news
        start_date: Optional start date for filtering news
        limit: Maximum number of records to return
        
    Returns:
        A list of CompanyNews objects or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build the SQL query
        sql = """
        SELECT * FROM company_news
        WHERE ticker = %s AND date <= %s
        """
        params = [ticker, end_date]
        
        if start_date:
            sql += " AND date >= %s"
            params.append(start_date)
            
        sql += " ORDER BY date DESC LIMIT %s"
        params.append(limit)
        
        # Query the database
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
        
        # Convert to CompanyNews objects
        news_list = []
        for result in results:
            # Convert date to string format
            result['date'] = result['date'].isoformat()
            
            # Create a CompanyNews object
            news_list.append(CompanyNews(**result))
        
        return news_list
        
    except Exception as e:
        print(f"Error fetching company news from database: {e}")
        return None

def get_valuation_db(
    ticker: str,
    end_date: str
) -> list[dict] | None:
    """
    Fetch valuation data from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering valuations
        
    Returns:
        A list of valuation records with all methods for latest date,
        including weighted combined result, or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # First get the latest valuation date
        cursor.execute(
            """
            SELECT MAX(biz_date) as latest_date 
            FROM valuation
            WHERE ticker = %s AND biz_date <= %s
            """,
            (ticker, end_date))
        latest_date = cursor.fetchone()['latest_date']
        
        if not latest_date:
            return None
            
        # Get all valuation methods for latest date including weighted result
        cursor.execute(
            """
            SELECT * FROM valuation
            WHERE ticker = %s AND biz_date = %s
            ORDER BY 
                CASE valuation_method 
                    WHEN 'weighted' THEN 999
                    ELSE 0
                END,
                valuation_method
            """,
            (ticker, latest_date))
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
            
        return results
            
    except Exception as e:
        print(f"Error fetching valuation from database: {e}")
        return None

def get_technicals_db(
    ticker: str,
    end_date: str,
    limit: int = 1
) -> list[dict] | None:
    """
    Fetch technical analysis data from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering technicals
        limit: Maximum number of records to return
        
    Returns:
        A list of technical analysis records or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query the database
        cursor.execute(
            """
            SELECT * FROM technicals
            WHERE ticker = %s AND biz_date <= %s
            ORDER BY biz_date DESC, created_at DESC
            LIMIT %s
            """,
            (ticker, end_date, limit))
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
            
        return results
            
    except Exception as e:
        print(f"Error fetching technicals from database: {e}")
        return None

def get_sentiment_db(
    ticker: str,
    end_date: str,
    limit: int = 1
) -> list[dict] | None:
    """
    Fetch sentiment analysis data from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering sentiment
        limit: Maximum number of records to return
        
    Returns:
        A list of sentiment analysis records or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query the database
        cursor.execute(
            """
            SELECT * FROM sentiment
            WHERE ticker = %s AND biz_date <= %s
            ORDER BY biz_date DESC, created_at DESC
            LIMIT %s
            """,
            (ticker, end_date, limit))
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
            
        return results
            
    except Exception as e:
        print(f"Error fetching sentiment from database: {e}")
        return None

def get_fundamentals_db(
    ticker: str,
    end_date: str,
    limit: int = 1
) -> list[dict] | None:
    """
    Fetch fundamental analysis data from the PostgreSQL database.
    
    Args:
        ticker: The stock ticker symbol
        end_date: The end date for filtering fundamentals
        limit: Maximum number of records to return
        
    Returns:
        A list of fundamental analysis records or None if not found
    """
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query the database
        cursor.execute(
            """
            SELECT * FROM fundamentals
            WHERE ticker = %s AND biz_date <= %s
            ORDER BY biz_date DESC, created_at DESC
            LIMIT %s
            """,
            (ticker, end_date, limit))
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        # Return None if no data found
        if not results:
            return None
            
        return results
            
    except Exception as e:
        print(f"Error fetching fundamentals from database: {e}")
        return None

def save_sophie_analysis(
    ticker: str,
    signal: str,
    confidence: int,
    overall_score: int,
    reasoning: str,
    time_horizon_analysis: dict,
    bullish_factors: list,
    bearish_factors: list,
    risks: list,
    model_name: str,
    model_display_name: str = None,
    biz_date: datetime.date = datetime.date.today()
) -> bool:
    """Save Sophie agent analysis to database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        sql = """
        INSERT INTO sophie_analysis (
            ticker, biz_date, signal, confidence, overall_score, reasoning,
            short_term_outlook, medium_term_outlook, long_term_outlook,
            bullish_factors, bearish_factors, risks,
            model_name, model_display_name
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s::jsonb, %s::jsonb, %s::jsonb,
            %s, %s
        )
        ON CONFLICT (ticker, biz_date, model_display_name) DO UPDATE SET
            signal = EXCLUDED.signal,
            confidence = EXCLUDED.confidence,
            overall_score = EXCLUDED.overall_score,
            reasoning = EXCLUDED.reasoning,
            short_term_outlook = EXCLUDED.short_term_outlook,
            medium_term_outlook = EXCLUDED.medium_term_outlook,
            long_term_outlook = EXCLUDED.long_term_outlook,
            bullish_factors = EXCLUDED.bullish_factors,
            bearish_factors = EXCLUDED.bearish_factors,
            risks = EXCLUDED.risks,
            model_name = EXCLUDED.model_name,
            model_display_name = EXCLUDED.model_display_name,
            updated_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(sql, (
            ticker, biz_date, signal, confidence, overall_score, reasoning,
            time_horizon_analysis.get('short_term', ''),
            time_horizon_analysis.get('medium_term', ''),
            time_horizon_analysis.get('long_term', ''),
            json.dumps(bullish_factors), json.dumps(bearish_factors), json.dumps(risks),
            model_name, model_display_name
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error saving Sophie analysis: {e}")
        return False

def save_company_news(news_list: list[CompanyNews]) -> bool:
    """
    Save company news to the PostgreSQL database.
    
    Args:
        news_list: List of CompanyNews objects to save
        
    Returns:
        Boolean indicating success or failure
    """
    if not news_list:
        return False
        
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert records
        insert_count = 0
        for news in news_list:
            try:
                data = news.model_dump()
                
                # Build field lists
                fields = list(data.keys())
                
                # Generate placeholders
                placeholders = ', '.join(['%s'] * len(fields))
                fields_str = ', '.join(fields)
                update_fields = ', '.join([f"{field} = EXCLUDED.{field}" for field in fields])
                update_fields += ", updated_at = CURRENT_TIMESTAMP"
                
                # Build SQL query
                sql = f"""
                INSERT INTO company_news ({fields_str})
                VALUES ({placeholders})
                ON CONFLICT (ticker, url) DO UPDATE SET {update_fields}
                """
                
                # Execute query
                cursor.execute(sql, [data[field] for field in fields])
                insert_count += 1
                
            except Exception as inner_e:
                print(f"Error inserting company news for {news.ticker} on {news.date}: {inner_e}")
        
        # Commit the transaction
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        print(f"Successfully saved {insert_count} company news records")
        return True
        
    except Exception as e:
        print(f"Error saving company news to database: {e}")
        return False
