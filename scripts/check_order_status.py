import moomoo as moo

ctx = moo.OpenSecTradeContext(host="127.0.0.1", port=11111, is_encrypt=False, filter_trdmarket=moo.TrdMarket.US)
ret, data = ctx.order_list_query(trd_env=moo.TrdEnv.SIMULATE, acc_id=5077333, refresh_cache=True)
if ret == 0 and data is not None:
    for _, r in data.iterrows():
        oid = str(r["order_id"])
        if oid in ("3266354", "3266355"):
            print(f"  {oid} {r['code']} status={r['order_status']} dealt_qty={r['dealt_qty']}")
else:
    print(f"ret={ret}: {data}")
ctx.close()
