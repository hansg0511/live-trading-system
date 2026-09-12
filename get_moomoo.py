from moomoo import *

# Initialize the quote context
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

# Request historical kline data for AAPL (US.AAPL)
ret, data, page_req_key = quote_ctx.request_history_kline(
    'US.AAPL',
    start='2026-06-01',
    ktype=KLType.K_DAY,
    max_count=5  # 5 data points per page
)

if ret == RET_OK:
    print("First page data:")
    print(data)
    print(f"Stock code: {data['code'][0]}")  # Extract first stock code
    print(f"Closing prices: {data['close'].values.tolist()}")  # Convert to list
else:
    print('Error fetching data:', data)

# Handle pagination to get all results
while page_req_key is not None:
    print('\n*************************************')
    ret, data, page_req_key = quote_ctx.request_history_kline(
        'US.AAPL',
        start='2026-06-01',
        ktype=KLType.K_DAY,
        max_count=5,
        page_req_key=page_req_key
    )

    if ret == RET_OK:
        print("Next page data:")
        print(data)
    else:
        print('Error fetching next page:', data)

print('\nAll pages fetched successfully!')
quote_ctx.close()