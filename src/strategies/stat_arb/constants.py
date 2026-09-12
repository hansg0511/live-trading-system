# Centralized project constants

# S&P 500 Mega-cap universe categorized by GICS sectors
SECTOR_MAP = {
    'Tech': ['AAPL', 'MSFT', 'CRM', 'ADBE', 'TXN', 'NVDA', 'INTC', 'CSCO', 'ORCL', 'AVGO'],
    'Financials': ['JPM', 'V', 'MA', 'BAC', 'GS', 'MS', 'AXP', 'PYPL'],
    'CommServices': ['GOOGL', 'META', 'NFLX', 'VZ', 'DIS', 'CMCSA', 'TMUS', 'T'],
    'Staples': ['PG', 'KO', 'PEP', 'WMT', 'COST', 'MDLZ', 'CL', 'MO'],
    'Discretionary': ['AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'LOW', 'SBUX', 'TJX'],
    'Healthcare': ['JNJ', 'UNH', 'LLY', 'ABT', 'DHR', 'TMO', 'PFE', 'MRK', 'AMGN', 'GILD'],
    'Energy': ['XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX'],
    'Industrials': ['GE', 'HON', 'UPS', 'UNP', 'CAT', 'BA', 'LMT', 'RTX']
}

TICKERS = [t for sector in SECTOR_MAP.values() for t in sector]
