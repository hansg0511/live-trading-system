from src.core.models import TimeFrame
from src.data.yfinance.provider import YFinanceDataProvider
from src.strategies.stat_arb.signal import compute_residuals, compute_zscore
import pandas as pd
from datetime import datetime

# Optional: MooMoo broker adapter (only needed if using MooMoo broker)
try:
    from src.brokers.moomoo.adapter import MooMooAdapter
    MooMooAdapterAvailable = True
except ImportError:
    MooMooAdapterAvailable = False


def extract_tickers_from_pairs(pairs_series):
    """Extract unique tickers from a series of pair strings (e.g., 'AAPL-MSFT')."""
    tickers = set()
    for pair in pairs_series:
        parts = pair.split('-')
        if len(parts) == 2:
            tickers.add(parts[0])
            tickers.add(parts[1])
    return list(tickers)


# Initialize the YFinance provider
provider = YFinanceDataProvider()
provider.connect()

# Read pairs from the CSV file (already analyzed and sorted by cointegration_pvalue_log)
df = pd.read_csv(
    "pair_selection_results.csv",
    dtype={"symbol": str},       # force dtypes where inference is risky
)

# Get top 5 ranking pairs (sorted by cointegration_pvalue_log ascending - most significant first)
top_5_pairs = df.nsmallest(5, 'cointegration_pvalue_log')['pair'].tolist()
print(f"Top 5 ranking pairs: {top_5_pairs}")

# Extract tickers from top 5 pairs
tickers_for_top_pairs = extract_tickers_from_pairs(top_5_pairs)
print(f"Tickers for top pairs: {tickers_for_top_pairs}")

# Download candlestick data for tickers from top 5 pairs
# Get enough data for 90-day lookback (at least 100 days to be safe)
data = provider.get_historical_candlesticks(
    symbol=tickers_for_top_pairs,
    start='2025-01-01',  # Get 100+ days of data for 90-day lookback
    end= datetime.today(),
    timeframe=TimeFrame.DAY
)

for pair in top_5_pairs:
    ticker1, ticker2 = pair.split('-')
    print(f"\n--- Pair: {pair} ---")
    
    # Get pair info from CSV
    pair_info = df[df['pair'] == pair].iloc[0]
    hedge_ratio_csv = pair_info['hedge_ratio_log']
    half_life_log = pair_info['half_life_log']
    
    # Get close prices
    s1 = data['Close'][ticker1]
    s2 = data['Close'][ticker2]
    
    # Determine appropriate lookback (use smaller of 90 or available data - 10)
    available_days = len(s1)
    lookback = min(90, available_days - 10)
    
    # Compute residuals with lookback window (using log prices)
    residuals_df = compute_residuals(s1, s2, lookback=lookback, log_space=True)
    
    # Compute zscore with lookback = max(half_life_log, 10)
    zscore_lookback = max(int(half_life_log), 10)
    zscore = compute_zscore(residuals_df['residual'], lookback=zscore_lookback)
    

    print(f"Hedge ratio (CSV): {hedge_ratio_csv:.4f}")
    print(f"Latest hedge ratio: {residuals_df['hedge_ratio'].iloc[-1]:.4f}")
    print(f"Latest residual: {residuals_df['residual'].iloc[-1]:.4f}")
    print(f"Latest z-score: {zscore.iloc[-1]:.4f}")
    print(f"Phi (AR1): {residuals_df['phi'].iloc[-1]:.4f}")

# broker = MooMooAdapter()
