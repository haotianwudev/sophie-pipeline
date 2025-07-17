"""
SQL Table Upload Configuration

Contains configuration for financial data uploads including:
- Upload function to call
- Target SQL table name
- API function to get data
- Required parameters
"""

from src.tools.db_upload import (
    upload_company_facts,
    upload_prices,
    upload_company_news,
    upload_company_news_alphavantage,
    upload_financial_metrics,
    upload_insider_trades,
    upload_line_items,
    upload_ai_analysis_result,
    upload_valuation_result,
    upload_fundamentals_result,
    upload_sentiment_result,
    upload_technical_result
)

TABLE_UPLOAD_CONFIG = {
    'company_facts': {
        'upload_function': upload_company_facts,
        'sql_table_name': 'company_facts',
        'params': {}
    },
    'prices': {
        'upload_function': upload_prices,
        'sql_table_name': 'prices',
        'params': ['start_date', 'end_date']
    },
    'company_news': {
        'upload_function': upload_company_news,
        'sql_table_name': 'company_news',
        'params': ['end_date']
    },
    'company_news_alphavantage': {
        'upload_function': upload_company_news_alphavantage,
        'sql_table_name': 'company_news_alphavantage',
        'params': ['time_from', 'time_to', 'limit']
    },
    'financial_metrics': {
        'upload_function': upload_financial_metrics,
        'sql_table_name': 'financial_metrics',
        'params': ['end_date']
    },
    'insider_trades': {
        'upload_function': upload_insider_trades,
        'sql_table_name': 'insider_trades',
        'params': ['end_date']
    },
    'line_items': {
        'upload_function': upload_line_items,
        'sql_table_name': 'line_items',
        'params': ['end_date']
    },
    'ai_analysis': {
        'upload_function': upload_ai_analysis_result,
        'sql_table_name': 'ai_analysis',
        'params': ['biz_date']
    },
    'valuation': {
        'upload_function': upload_valuation_result,
        'sql_table_name': 'valuation',
        'params': ['biz_date']
    },
    'fundamentals': {
        'upload_function': upload_fundamentals_result,
        'sql_table_name': 'fundamentals',
        'params': ['biz_date']
    },
    'sentiment': {
        'upload_function': upload_sentiment_result,
        'sql_table_name': 'sentiment',
        'params': ['biz_date']
    },
    'technicals': {
        'upload_function': upload_technical_result,
        'sql_table_name': 'technicals',
        'params': ['biz_date']
    }
}
