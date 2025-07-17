import subprocess
import sys
import logging
from datetime import datetime
import os

def setup_logging(tickers):
    """Configure logging to file and console with timestamped filename"""
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    tickers_str = '_'.join(tickers)
    log_file = f'logs/upload_free_{timestamp}_{tickers_str}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger()

def run_command(cmd, logger):
    """Execute command with logging"""
    try:
        logger.info(f"Starting command: {' '.join(cmd)}")
        start_time = datetime.now()
        
        subprocess.run(cmd, check=True)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Command completed successfully in {duration:.2f} seconds")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e}")
        sys.exit(1)

def main():
    tickers = ["AAPL", "MSFT", "NVDA"]  # Can be modified to read from file/config
    logger = setup_logging(tickers)
    
    logger.info(f"Starting free tickers upload process for: {', '.join(tickers)}")
    
    tickers_str = ",".join(tickers)
    commands = [
        ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "company_facts"],
        ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "prices"],
        ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "company_news"],
        #["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "company_news_alphavantage"],
        ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "financial_metrics"],
        ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "insider_trades"],
        ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", tickers_str, "--table", "line_items"],
        ["poetry", "run", "python", "src/upload/analysis_table_uploader.py", "--tickers", tickers_str],
        ["poetry", "run", "python", "src/upload/sophie_analysis_table_uploader.py", "--tickers", tickers_str]
    ]

    for i, cmd in enumerate(commands, 1):
        logger.info(f"Running job {i}/{len(commands)}")
        run_command(cmd, logger)
    
    logger.info("All free tickers uploads completed successfully!")

if __name__ == "__main__":
    main()
