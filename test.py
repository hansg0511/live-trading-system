from moomoo import *
trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSG)
ret, data = trd_ctx.order_list_query(trd_env=TrdEnv.SIMULATE)
if ret == RET_OK:
    print(data)
    if data.shape[0] > 0:  # If the order list is not empty
        print(data['order_id'][0])  # Get the first order ID of the order list today
        print(data['order_id'].values.tolist())  # Convert to list
else:
    print('order_list_query error: ', data)
trd_ctx.close()
