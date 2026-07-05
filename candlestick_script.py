from src.core.models import TimeFrame
from src.data.yfinance.provider import YFinanceDataProvider
from src.strategies.stat_arb.pair_selection import PairSelector
from src.strategies.stat_arb.signal import compute_residuals, compute_zscore
from src.strategies.stat_arb.constants import SECTOR_MAP, TICKERS
from src.brokers.moomoo.adapter import MooMooAdapter
import pandas as pd


# Initialize the YFinance provider
provider = YFinanceDataProvider()
provider.connect()

# Download candlestick data for AAPL from 2026-05-01 to 2026-06-30
data = provider.get_historical_candlesticks(
    symbol=TICKERS,  # You can provide a list of symbols
    start='2026-05-01',
    end='2026-06-30',
    timeframe=TimeFrame.DAY
)
# print(data)
# print(PairSelector.analyze_pair(data['Close']['AAPL'], data['Close']['MSFT']))

# Display the downloaded data

# results = PairSelector(pvalue_threshold=0.05).select_pairs(data, SECTOR_MAP, start='2026-05-01', end='2026-06-30')
# results.to_csv('pair_selection_results.csv', index=False)
# print(results)

df = pd.read_csv(
    "pair_selection_results.csv",
    dtype={"symbol": str},       # force dtypes where inference is risky
)

pairs = df.loc[0:4,'pair']
print(pairs)


# broker = MooMooAdapter()
