import yfinance as yf

# Bitcoin trades 24/7/365
crypto_data = yf.download("BTC-USD", period="1d", interval="1d")
print("Live, unclosed weekend candle for BTC:")
print(crypto_data)