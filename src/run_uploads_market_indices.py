import subprocess
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Always write logs to <repo_root>/logs/ regardless of working directory
REPO_ROOT = Path(__file__).parent.parent
LOGS_DIR = REPO_ROOT / "logs"

TICKERS = ["SPX", "VIX"]
TRAILING_DAYS = 7  # self-heals any missed daily runs


def setup_logging(tickers):
    """Configure logging to file and console with timestamped filename"""
    LOGS_DIR.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    tickers_str = '_'.join(tickers)
    log_file = LOGS_DIR / f'upload_market_indices_{timestamp}_{tickers_str}.log'

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
    logger = setup_logging(TICKERS)

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=TRAILING_DAYS)).strftime("%Y-%m-%d")

    logger.info(f"Starting market indices upload for: {', '.join(TICKERS)} ({start_date} to {end_date})")

    tickers_str = ",".join(TICKERS)
    cmd = [
        "poetry", "run", "python", "src/upload/raw_data_table_uploader.py",
        "--tickers", tickers_str, "--table", "prices",
        "--start-date", start_date, "--end-date", end_date, "--verbose",
    ]
    run_command(cmd, logger)

    logger.info("Market indices (SPX, VIX) upload completed successfully!")


if __name__ == "__main__":
    main()
