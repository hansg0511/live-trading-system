import moomoo as moo

ctx = moo.OpenSecTradeContext(host="127.0.0.1", port=11111, is_encrypt=False, filter_trdmarket=moo.TrdMarket.US)
trd_env = moo.TrdEnv.SIMULATE

# Get all pending orders
ret, data = ctx.order_list_query(trd_env=trd_env, acc_id=5077333, refresh_cache=True)
if ret == 0 and data is not None:
    for _, row in data.iterrows():
        oid = int(row["order_id"])
        status = str(row["order_status"]).upper()
        if status in ("SUBMITTED", "WAITING_SUBMIT", "FILLED_PART"):
            r, d = ctx.modify_order(
                trd_env=trd_env, acc_id=5077333,
                order_id=oid, order_type=moo.OrderType.NORMAL,
                modify_order_op=moo.ModifyOrderOp.CANCEL
            )
            print(f"Cancel {oid} ({status}): ret={r} {'OK' if r==0 else str(d)}")
        else:
            print(f"Skipping {oid} ({status})")

ctx.close()
