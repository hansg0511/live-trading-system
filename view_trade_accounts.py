from moomoo import *

# open a trade context for US market, paper trading
trd_ctx = OpenSecTradeContext(
    host='127.0.0.1', 
    port=11111,
    is_encrypt=False,
    filter_trdmarket=TrdMarket.US
)

# check paper account info
ret, data = trd_ctx.get_acc_list()
if ret == RET_OK:
    print(data)
else:
    print('Error:', data)

# ret, data = trd_ctx.place_order(price=0, qty=1, code="US.AAPL", order_type=OrderType.MARKET, trd_side=TrdSide.BUY, trd_env=TrdEnv.SIMULATE, session=Session.NONE)

ret, data = trd_ctx.order_list_query(trd_env=TrdEnv.SIMULATE)
# print(data[['create_time', 'updated_time']])  # Get the first order creation time of the order list today

print(data)




trd_ctx.close()