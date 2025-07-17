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
    log_file = f'logs/upload_{timestamp}_{tickers_str}.log'
    
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
    tickers = ["AMZN","GOOGL","META","TSLA","JPM","GS","KO","PYPL","ADBE","PEP"]  
    logger = setup_logging(tickers)
    
    logger.info(f"Starting upload process for tickers: {', '.join(tickers)}")
    
    # First run insider_trades for all tickers
    logger.info("Processing insider_trades for all tickers")
    line_items_cmd = [
        "poetry", "run", "python", "src/upload/raw_data_table_uploader.py",
        "--tickers", ",".join(tickers),
        "--table", "line_items"
    ]
    run_command(line_items_cmd, logger)

    # logger.info("Processing company_news_alphavantage for all tickers")
    # company_news_alphavantage_cmd = [
    #     "poetry", "run", "python", "src/upload/raw_data_table_uploader.py",
    #     "--tickers", ",".join(tickers),
    #     "--table", "company_news_alphavantage"
    # ]
    # run_command(company_news_alphavantage_cmd, logger)

    # Then run other jobs one ticker at a time
    for ticker in tickers:
        logger.info(f"Starting processing for ticker: {ticker}")
        
        commands = [
            ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", ticker, "--table", "company_facts"],
            ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", ticker, "--table", "prices"],
            ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", ticker, "--table", "company_news"],
            ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", ticker, "--table", "financial_metrics"],
            ["poetry", "run", "python", "src/upload/raw_data_table_uploader.py", "--tickers", ticker, "--table", "insider_trades"],
            ["poetry", "run", "python", "src/upload/analysis_table_uploader.py", "--tickers", ticker],
            ["poetry", "run", "python", "src/upload/sophie_analysis_table_uploader.py", "--tickers", ticker]
        ]

        for i, cmd in enumerate(commands, 1):
            logger.info(f"Running job {i}/{len(commands)} for {ticker}")
            run_command(cmd, logger)
    
    logger.info("All uploads completed successfully!")

if __name__ == "__main__":
    main()
