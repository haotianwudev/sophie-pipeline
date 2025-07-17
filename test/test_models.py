import unittest
import json
import os
from src.data.models import (
    Price, PriceResponse,
    FinancialMetrics, FinancialMetricsResponse,
    CompanyNews, CompanyNewsResponse,
    InsiderTrade, InsiderTradeResponse,
    LineItem, LineItemResponse
)


class TestDataModels(unittest.TestCase):
    """Test case for the Pydantic data models."""
    
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
            
        # Load line items data
        with open(os.path.join(self.mock_dir, "aapl_line_items.json"), "r") as f:
            self.mock_line_items = json.load(f)
    
    def test_price_model(self):
        """Test Price model."""
        # Create Price instance from mock data
        price = Price(**self.mock_prices[0])
        
        # Verify attributes
        self.assertEqual(price.open, 173.25)
        self.assertEqual(price.close, 174.79)
        self.assertEqual(price.high, 175.1)
        self.assertEqual(price.low, 172.55)
        self.assertEqual(price.volume, 53382510)
        self.assertEqual(price.time, "2025-04-23T00:00:00.000Z")
        
        # Test price response model
        price_response = PriceResponse(
            ticker="AAPL",
            prices=[Price(**p) for p in self.mock_prices]
        )
        
        self.assertEqual(price_response.ticker, "AAPL")
        self.assertEqual(len(price_response.prices), 7)
        
    def test_financial_metrics_model(self):
        """Test FinancialMetrics model."""
        # Create FinancialMetrics instance from mock data
        metrics = FinancialMetrics(**self.mock_financial_metrics[0])
        
        # Verify attributes
        self.assertEqual(metrics.ticker, "AAPL")
        self.assertEqual(metrics.report_period, "2025-01-30")
        self.assertEqual(metrics.period, "ttm")
        self.assertEqual(metrics.currency, "USD")
        self.assertEqual(metrics.market_cap, 2850000000000.0)
        self.assertEqual(metrics.price_to_earnings_ratio, 32.14)
        
        # Test metrics response model
        metrics_response = FinancialMetricsResponse(
            financial_metrics=[FinancialMetrics(**m) for m in self.mock_financial_metrics]
        )
        
        self.assertEqual(len(metrics_response.financial_metrics), 2)
        
    def test_company_news_model(self):
        """Test CompanyNews model."""
        # Create CompanyNews instance from mock data
        news = CompanyNews(**self.mock_company_news[0])
        
        # Verify attributes
        self.assertEqual(news.ticker, "AAPL")
        self.assertEqual(news.title, "Apple Reports Record Q2 Earnings, Announces $90B Share Repurchase Program")
        self.assertEqual(news.author, "John Smith")
        self.assertEqual(news.source, "Financial Times")
        self.assertEqual(news.date, "2025-04-28")
        self.assertEqual(news.url, "https://example.com/apple-earnings-q2")
        self.assertEqual(news.sentiment, "positive")
        
        # Test news response model
        news_response = CompanyNewsResponse(
            news=[CompanyNews(**n) for n in self.mock_company_news]
        )
        
        self.assertEqual(len(news_response.news), 5)
        
    def test_insider_trade_model(self):
        """Test InsiderTrade model."""
        # Create InsiderTrade instance from mock data
        trade = InsiderTrade(**self.mock_insider_trades[0])
        
        # Verify attributes
        self.assertEqual(trade.ticker, "AAPL")
        self.assertEqual(trade.name, "Timothy D. Cook")
        self.assertEqual(trade.title, "Chief Executive Officer")
        self.assertTrue(trade.is_board_director)
        self.assertEqual(trade.transaction_date, "2025-04-15")
        self.assertEqual(trade.transaction_shares, 25000.0)
        self.assertEqual(trade.transaction_price_per_share, 180.75)
        self.assertEqual(trade.transaction_value, 4518750.0)
        
        # Test trade response model
        trade_response = InsiderTradeResponse(
            insider_trades=[InsiderTrade(**t) for t in self.mock_insider_trades]
        )
        
        self.assertEqual(len(trade_response.insider_trades), 4)
        
    def test_line_item_model(self):
        """Test LineItem model."""
        # Create LineItem instance from mock data
        line_item = LineItem(**self.mock_line_items[0])
        
        # Verify base attributes
        self.assertEqual(line_item.ticker, "AAPL")
        self.assertEqual(line_item.report_period, "2025-01-30")
        self.assertEqual(line_item.period, "ttm")
        self.assertEqual(line_item.currency, "USD")
        
        # Verify dynamic attributes
        self.assertEqual(getattr(line_item, "revenue"), 385000000000.0)
        self.assertEqual(getattr(line_item, "free_cash_flow"), 106700000000.0)
        self.assertEqual(getattr(line_item, "total_debt"), 118400000000.0)
        
        # Test line item response model
        line_item_response = LineItemResponse(
            search_results=[LineItem(**l) for l in self.mock_line_items]
        )
        
        self.assertEqual(len(line_item_response.search_results), 2)


if __name__ == '__main__':
    unittest.main() 