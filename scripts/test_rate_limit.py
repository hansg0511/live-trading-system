"""Test rate limit directly without any throttling."""
import moomoo as moo
import time

ctx = moo.OpenSecTradeContext(host="127.0.0.1", port=11111, is_encrypt=False, filter_trdmarket=moo.TrdMarket.US)

for i in range(15):
    t0 = time.time()
    ret, data = ctx.order_list_query(trd_env=moo.TrdEnv.SIMULATE, acc_id=5077333, refresh_cache=True)
    elapsed = time.time() - t0
    status = "OK" if ret == 0 else f"RET_ERROR ({data})"
    print(f"{i+1:2d}: ret={ret}  {status}  ({elapsed:.2f}s)")
    if ret != 0:
        break
    time.sleep(1)

ctx.close()
