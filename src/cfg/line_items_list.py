LINE_ITEMS = [
    # Income Statement Items
    "revenue",
    "net_income",
    "operating_income",
    "ebitda",
    "ebit",
    "earnings_per_share",
    "research_and_development",
    
    # Cash Flow Items
    "free_cash_flow",
    "capital_expenditure",
    "depreciation_and_amortization",
    "working_capital",
    "dividends_and_other_cash_distributions",
    
    # Balance Sheet Items
    "total_assets",
    "total_liabilities",
    "current_assets",
    "current_liabilities",
    "cash_and_equivalents",
    "total_debt",
    "shareholders_equity",
    "book_value_per_share",
    "outstanding_shares",
    "goodwill_and_intangible_assets",
    
    # Financial Ratios
    "return_on_invested_capital",
    "gross_margin",
    "operating_margin"
]

# Function to get all line items
def get_all_line_items():
    """Get the full list of available line items."""
    return LINE_ITEMS.copy()