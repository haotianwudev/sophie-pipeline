import unittest
import json
import os
from src.data.cache import Cache


class TestCache(unittest.TestCase):
    """Test case for the Cache class."""
    
    def setUp(self):
        """Set up test fixtures, including loading mock data."""
        # Get a clean cache instance for testing
        self.cache = Cache()
        
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
    
    def test_prices_cache(self):
        """Test caching of price data."""
        # Initially cache should be empty
        self.assertIsNone(self.cache.get_prices("AAPL"))
        
        # Store data in cache
        self.cache.set_prices("AAPL", self.mock_prices)
        
        # Verify data was cached
        cached_data = self.cache.get_prices("AAPL")
        self.assertIsNotNone(cached_data)
        self.assertEqual(len(cached_data), 7)
        self.assertEqual(cached_data[0]["open"], 173.25)
        
        # Test cache merging with new data
        new_price = {
            "open": 183.50,
            "close": 185.25,
            "high": 186.00,
            "low": 183.20,
            "volume": 58745230,
            "time": "2025-04-30T00:00:00.000Z"
        }
        
        self.cache.set_prices("AAPL", [new_price])
        merged_data = self.cache.get_prices("AAPL")
        
        # Should now have 8 price entries
        self.assertEqual(len(merged_data), 8)
        
        # Verify last entry is the new one
        self.assertEqual(merged_data[-1]["time"], "2025-04-30T00:00:00.000Z")
        
        # Test that duplicates are not added
        duplicate_price = self.mock_prices[0].copy()
        self.cache.set_prices("AAPL", [duplicate_price])
        
        # Size should remain the same
        merged_data = self.cache.get_prices("AAPL")
        self.assertEqual(len(merged_data), 8)
    
    def test_financial_metrics_cache(self):
        """Test caching of financial metrics data."""
        # Initially cache should be empty
        self.assertIsNone(self.cache.get_financial_metrics("AAPL"))
        
        # Store data in cache
        self.cache.set_financial_metrics("AAPL", self.mock_financial_metrics)
        
        # Verify data was cached
        cached_data = self.cache.get_financial_metrics("AAPL")
        self.assertIsNotNone(cached_data)
        self.assertEqual(len(cached_data), 2)
        self.assertEqual(cached_data[0]["report_period"], "2025-01-30")
        
        # Test cache merging with new data
        new_metric = {
            "ticker": "AAPL",
            "report_period": "2025-04-30",
            "period": "ttm",
            "currency": "USD",
            "market_cap": 2900000000000.0,
            "price_to_earnings_ratio": 33.5
        }
        
        self.cache.set_financial_metrics("AAPL", [new_metric])
        merged_data = self.cache.get_financial_metrics("AAPL")
        
        # Should now have 3 metrics entries
        self.assertEqual(len(merged_data), 3)
        
        # Verify new entry exists
        newest_entry = next((item for item in merged_data if item["report_period"] == "2025-04-30"), None)
        self.assertIsNotNone(newest_entry)
        self.assertEqual(newest_entry["market_cap"], 2900000000000.0)
    
    def test_company_news_cache(self):
        """Test caching of company news data."""
        # Initially cache should be empty
        self.assertIsNone(self.cache.get_company_news("AAPL"))
        
        # Store data in cache
        self.cache.set_company_news("AAPL", self.mock_company_news)
        
        # Verify data was cached
        cached_data = self.cache.get_company_news("AAPL")
        self.assertIsNotNone(cached_data)
        self.assertEqual(len(cached_data), 5)
        
        # Test cache merging with new data
        new_news = {
            "ticker": "AAPL",
            "title": "Apple Acquires AI Startup for $500M",
            "author": "Jane Doe",
            "source": "Reuters",
            "date": "2025-04-30",
            "url": "https://example.com/apple-ai-acquisition",
            "sentiment": "positive"
        }
        
        self.cache.set_company_news("AAPL", [new_news])
        merged_data = self.cache.get_company_news("AAPL")
        
        # Should now have 6 news entries
        self.assertEqual(len(merged_data), 6)
    
    def test_insider_trades_cache(self):
        """Test caching of insider trades data."""
        # Initially cache should be empty
        self.assertIsNone(self.cache.get_insider_trades("AAPL"))
        
        # Store data in cache
        self.cache.set_insider_trades("AAPL", self.mock_insider_trades)
        
        # Verify data was cached
        cached_data = self.cache.get_insider_trades("AAPL")
        self.assertIsNotNone(cached_data)
        self.assertEqual(len(cached_data), 4)
        
        # Test cache merging with new data
        new_trade = {
            "ticker": "AAPL",
            "issuer": "Apple Inc.",
            "name": "Craig Federighi",
            "title": "Senior VP, Software Engineering",
            "is_board_director": False,
            "transaction_date": "2025-04-28",
            "transaction_shares": 15000.0,
            "transaction_price_per_share": 182.50,
            "transaction_value": 2737500.0,
            "shares_owned_before_transaction": 95000.0,
            "shares_owned_after_transaction": 80000.0,
            "security_title": "Common Stock",
            "filing_date": "2025-04-29"
        }
        
        self.cache.set_insider_trades("AAPL", [new_trade])
        merged_data = self.cache.get_insider_trades("AAPL")
        
        # Should now have 5 trades entries
        self.assertEqual(len(merged_data), 5)
        
        # Verify new entry exists
        newest_entry = next((item for item in merged_data if item["filing_date"] == "2025-04-29"), None)
        self.assertIsNotNone(newest_entry)
        self.assertEqual(newest_entry["name"], "Craig Federighi")
    
    def test_company_facts_cache(self):
        """Test caching of company facts."""
        # Verify cache is empty initially
        self.assertIsNone(self.cache.get_company_facts("AAPL"))
        
        # Set company facts in cache
        self.cache.set_company_facts("AAPL", self.mock_company_facts)
        
        # Verify we can retrieve the facts
        cached_facts = self.cache.get_company_facts("AAPL")
        self.assertIsNotNone(cached_facts)
        self.assertEqual(cached_facts["ticker"], "AAPL")
        self.assertEqual(cached_facts["name"], "Apple Inc.")
        self.assertEqual(cached_facts["market_cap"], 2918000000000.0)
        
        # Update with new data
        updated_facts = self.mock_company_facts.copy()
        updated_facts["market_cap"] = 3000000000000.0
        self.cache.set_company_facts("AAPL", updated_facts)
        
        # Verify updated data is cached
        updated_cached = self.cache.get_company_facts("AAPL")
        self.assertEqual(updated_cached["market_cap"], 3000000000000.0)
        
        # Test multiple tickers
        msft_facts = self.mock_company_facts.copy()
        msft_facts["ticker"] = "MSFT"
        msft_facts["name"] = "Microsoft Corporation"
        self.cache.set_company_facts("MSFT", msft_facts)
        
        # Verify both tickers are cached separately
        self.assertEqual(self.cache.get_company_facts("AAPL")["ticker"], "AAPL")
        self.assertEqual(self.cache.get_company_facts("MSFT")["ticker"], "MSFT")


if __name__ == '__main__':
    unittest.main() 