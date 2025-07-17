import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Create a logger for LLM interactions
llm_logger = logging.getLogger("llm")
llm_logger.setLevel(logging.INFO)

# Create a logger for portfolio management errors
portfolio_logger = logging.getLogger("portfolio")
portfolio_logger.setLevel(logging.INFO)

# Global variable to control logging behavior
save_logs_to_file = False

def configure_logging(save_logs: bool = False):
    """Configure logging settings
    
    Args:
        save_logs: Whether to save logs to file
    """
    global save_logs_to_file
    save_logs_to_file = save_logs
    
    # Clear existing handlers
    logging.root.handlers.clear()
    
    # Create logs directory and add file handler if saving logs
    if save_logs:
        os.makedirs("logs", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/backtest_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logging.root.addHandler(file_handler)

def log_llm_interaction(
    model_name: str, 
    model_provider: str, 
    prompt: Any, 
    response: Any, 
    agent_name: Optional[str] = None,
    error: Optional[Exception] = None
):
    """Log an LLM interaction
    
    Args:
        model_name: The name of the LLM model
        model_provider: The provider of the LLM model
        prompt: The prompt sent to the LLM
        response: The response received from the LLM
        agent_name: Optional name of the agent making the request
        error: Optional exception if an error occurred
    """
    agent_info = f" by {agent_name}" if agent_name else ""
    
    if error:
        llm_logger.error(f"LLM Error with {model_provider} {model_name}{agent_info}: {error}")
        return
    
    # Always log full interaction details to console
    llm_logger.info(f"LLM Request to {model_provider} {model_name}{agent_info}")
    llm_logger.info(f"Full Prompt:\n{str(prompt)}\n")
    
    # Also log to file if saving logs
    if save_logs_to_file:
        llm_logger.info(f"Full Response:\n{str(response)}\n")

def log_portfolio_error(error_message: str, ticker: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
    """Log portfolio management errors
    
    Args:
        error_message: The error message
        ticker: Optional ticker symbol related to the error
        details: Optional dictionary with additional error details
    """
    ticker_info = f" for {ticker}" if ticker else ""
    details_str = f": {details}" if details else ""
    
    portfolio_logger.error(f"Portfolio error{ticker_info} - {error_message}{details_str}")

def truncate_str(s: str, max_length: int = 100) -> str:
    """Truncate a string to a maximum length
    
    Args:
        s: The string to truncate
        max_length: Maximum length before truncation
        
    Returns:
        Truncated string
    """
    if len(s) <= max_length:
        return s
    return s[:max_length] + "..."
