#!/usr/bin/env python
"""
Database setup utility functions.
This module provides functions to set up and manage the PostgreSQL database.
"""

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

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

def execute_sql_file(file_path):
    """Execute SQL commands from a file."""
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: SQL file not found: {file_path}")
        return False
    
    # Read SQL commands from file
    with open(file_path, 'r') as f:
        sql_commands = f.read()
    
    # Execute SQL commands
    try:
        # Connect to PostgreSQL
        conn = get_db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Execute SQL commands
        cursor.execute(sql_commands)
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        print(f"Successfully executed SQL file: {file_path}")
        return True
        
    except Exception as e:
        print(f"Error executing SQL file {file_path}: {e}")
        
        # Try to close cursor and connection
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass
            
        return False

def setup_database():
    """Set up the database schema and tables."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create company_facts table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_facts (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) UNIQUE NOT NULL,
            name VARCHAR(255),
            cik VARCHAR(20),
            industry VARCHAR(255),
            sector VARCHAR(255),
            category VARCHAR(255),
            exchange VARCHAR(50),
            is_active BOOLEAN,
            listing_date DATE,
            location VARCHAR(255),
            market_cap NUMERIC(20, 2),
            number_of_employees INTEGER,
            sec_filings_url VARCHAR(255),
            sic_code VARCHAR(20),
            sic_industry VARCHAR(255),
            sic_sector VARCHAR(255),
            website_url VARCHAR(255),
            weighted_average_shares BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_company_facts_ticker ON company_facts(ticker);
        CREATE INDEX IF NOT EXISTS idx_company_facts_sector ON company_facts(sector);
        CREATE INDEX IF NOT EXISTS idx_company_facts_industry ON company_facts(industry);
        """)
        
        sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'sql')
        
        # Create prices table
        with open(os.path.join(sql_dir, 'create_prices_table.sql'), 'r') as f:
            create_prices_sql = f.read()
            cursor.execute(create_prices_sql)
        
        # Create financial_metrics table
        with open(os.path.join(sql_dir, 'create_financial_metrics_table.sql'), 'r') as f:
            create_financial_metrics_sql = f.read()
            cursor.execute(create_financial_metrics_sql)
        
        # Create line_items table
        with open(os.path.join(sql_dir, 'create_line_items_table.sql'), 'r') as f:
            create_line_items_sql = f.read()
            cursor.execute(create_line_items_sql)
        
        # Create insider_trades table
        with open(os.path.join(sql_dir, 'create_insider_trades_table.sql'), 'r') as f:
            create_insider_trades_sql = f.read()
            cursor.execute(create_insider_trades_sql)
        
        # Create company_news table
        with open(os.path.join(sql_dir, 'create_company_news_table.sql'), 'r') as f:
            create_company_news_sql = f.read()
            cursor.execute(create_company_news_sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database setup completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        return False

if __name__ == "__main__":
    setup_database() 