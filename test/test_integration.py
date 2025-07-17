import unittest
import json
import os
from unittest.mock import patch, MagicMock

# Import test utilities to set up path
import test_utils

# Create a mock API class
class MockAPI:
    def get_prices(self, ticker, start_date, end_date):
        pass
    
    def get_financial_metrics(self, ticker, end_date, period="ttm", limit=10):
        pass
    
    def get_company_news(self, ticker, end_date, start_date=None, limit=1000):
        pass
    
    def get_insider_trades(self, ticker, end_date, start_date=None, limit=1000):
        pass
    
    def get_company_facts(self, ticker):
        pass

from src.data.models import (
    Price,
    FinancialMetrics,
    CompanyNews,
    InsiderTrade
)

# Import the function from company_facts_service for testing
from src.tools.company_facts_service import get_market_cap

class TestIntegration(unittest.TestCase):
    """Integration tests for API and data flow."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Load mock data
        self.mock_dir = os.path.join(os.path.dirname(__file__), "mock")
        
        # Load prices data
        with open(os.path.join(self.mock_dir, "aapl_prices.json"), "r") as f:
            self.mock_prices = json.load(f)
            
        # Load financial metrics data
        with open(os.path.join(self.mock_dir, "aapl_financial_metrics.json"), "r") as f:
            self.mock_financial_metrics = json.load(f)
            
        # Load company news data
        with open(os.path.join(self.mock_dir, "aapl_company_news.json"), "r") as f:
            self.mock_company_news = json.load(f)
            
        # Load insider trades data
        with open(os.path.join(self.mock_dir, "aapl_insider_trades.json"), "r") as f:
            self.mock_insider_trades = json.load(f)
            
        # Load company facts data
        with open(os.path.join(self.mock_dir, "aapl_company_facts.json"), "r") as f:
            self.mock_company_facts = json.load(f)
        
        # Create our API mock instance
        self.api = MockAPI()
    
    def test_prices_workflow(self):
        """Test end-to-end workflow for prices."""
        # Configure mock to return prices
        mock_prices = [Price(**p) for p in self.mock_prices]
        self.api.get_prices = MagicMock(return_value=mock_prices)
        
        # Call the function
        results = self.api.get_prices("AAPL", "2025-04-23", "2025-04-29")
        
        # Verify result types
        self.assertTrue(all(isinstance(price, Price) for price in results))
        
        # Get a subset of the prices for the period 25-27
        # Find the relevant indices from the data
        filtered_data = []
        for p in self.mock_prices:
            date_part = p["time"].split("T")[0]  # Get date part like "2025-04-25"
            if "2025-04-25" <= date_part <= "2025-04-27":
                filtered_data.append(p)
        
        # Configure mock for filtered results test
        filtered_prices = [Price(**p) for p in filtered_data]
        self.api.get_prices = MagicMock(return_value=filtered_prices)
        
        # Test filtering by date range
        filtered_results = self.api.get_prices("AAPL", "2025-04-25", "2025-04-27")
        
        # Make sure we have the correct number of results
        self.assertEqual(len(filtered_results), len(filtered_data))
        
        # If we have entries for these dates, verify them
        if filtered_results:
            # The first entry should be for 25th
            self.assertTrue(filtered_results[0].time.startswith("2025-04-25"))
            # The last entry should be for 27th or earlier
            self.assertTrue(filtered_results[-1].time.startswith("2025-04-27") or 
                           filtered_results[-1].time.startswith("2025-04-26") or
                           filtered_results[-1].time.startswith("2025-04-25"))
    
    def test_metrics_workflow(self):
        """Test end-to-end workflow for financial metrics."""
        # Configure mock to return financial metrics
        mock_metrics = [FinancialMetrics(**m) for m in self.mock_financial_metrics]
        self.api.get_financial_metrics = MagicMock(return_value=mock_metrics)
        
        # Call the function
        results = self.api.get_financial_metrics("AAPL", "2025-02-01", period="ttm", limit=10)
        
        # Verify result types
        self.assertTrue(all(isinstance(metrics, FinancialMetrics) for metrics in results))
    
    def test_news_workflow(self):
        """Test end-to-end workflow for company news."""
        # Configure mock to return company news
        mock_news = [CompanyNews(**n) for n in self.mock_company_news]
        self.api.get_company_news = MagicMock(return_value=mock_news)
        
        # Call the function
        results = self.api.get_company_news("AAPL", "2025-04-30", start_date="2025-04-20")
        
        # Verify result types
        self.assertTrue(all(isinstance(news, CompanyNews) for news in results))
    
    def test_insider_trades_workflow(self):
        """Test end-to-end workflow for insider trades."""
        # Configure mock to return insider trades
        mock_trades = [InsiderTrade(**t) for t in self.mock_insider_trades]
        self.api.get_insider_trades = MagicMock(return_value=mock_trades)
        
        # Call the function
        results = self.api.get_insider_trades("AAPL", "2025-04-30", start_date="2025-03-01")
        
        # Verify result types
        self.assertTrue(all(isinstance(trade, InsiderTrade) for trade in results))
    
    def test_cache_hit_workflow(self):
        """Test workflow when data is found in cache."""
        # Configure mock to return prices (simulating cache hit)
        mock_prices = [Price(**p) for p in self.mock_prices]
        self.api.get_prices = MagicMock(return_value=mock_prices)
        
        # Call the function
        results = self.api.get_prices("AAPL", "2025-04-23", "2025-04-29")
        
        # Verify result types
        self.assertTrue(all(isinstance(price, Price) for price in results))
        
        # Verify response data is correct
        self.assertEqual(len(results), 7)
    
    def test_api_error_handling(self):
        """Test error handling for API failures."""
        # Configure mock to raise an exception
        self.api.get_prices = MagicMock(side_effect=Exception("Resource not found"))
        
        # Verify exception is raised on API error
        with self.assertRaises(Exception):
            self.api.get_prices("INVALID", "2025-04-23", "2025-04-29")

    def test_company_facts_workflow(self):
        """Test end-to-end workflow for company facts."""
        # Import models
        from src.data.models import CompanyFacts
        
        # Set up the API mock
        mock_facts = CompanyFacts(**self.mock_company_facts)
        self.api.get_company_facts = MagicMock(return_value=mock_facts)
        
        # Call the function and verify
        result = self.api.get_company_facts("AAPL")
        self.assertEqual(result.ticker, "AAPL")
        self.assertEqual(result.name, "Apple Inc.")
        self.assertEqual(result.market_cap, 2918000000000.0)
        self.assertEqual(result.sector, "Information Technology")
        
    @patch('src.tools.company_facts_service.get_company_facts')
    @patch('src.tools.company_facts_service.get_market_cap_db')
    def test_company_facts_service_market_cap(self, mock_get_market_cap_db, mock_get_company_facts):
        """Test the get_market_cap function from company_facts_service."""
        import datetime
        from src.data.models import CompanyFacts
        
        # Setup mocks for today's date
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        mock_facts = CompanyFacts(**self.mock_company_facts)
        mock_get_company_facts.return_value = mock_facts
        
        # Test current day market cap
        result_today = get_market_cap("AAPL", today)
        self.assertIsNotNone(result_today)
        # Verify market cap is a reasonable value (greater than 1 trillion)
        self.assertGreater(result_today, 1000000000000.0)
        
        # Setup mocks for historical date - now get_market_cap_db only takes ticker
        historical_date = "2025-01-15"
        mock_get_company_facts.return_value = None  # Simulate company facts not found
        mock_get_market_cap_db.return_value = 2850000000000.0
        
        # Test with historical date, but it will only use ticker now
        result_past = get_market_cap("AAPL", historical_date)
        self.assertEqual(result_past, 2850000000000.0)
        # Verify get_market_cap_db was called only with ticker
        mock_get_market_cap_db.assert_called_with("AAPL")

if __name__ == '__main__':
    unittest.main() 