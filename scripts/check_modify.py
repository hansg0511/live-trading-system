from moomoo import OpenSecTradeContext, ModifyOrderOp
import inspect

# Check modify_order signature
sig = inspect.signature(OpenSecTradeContext.modify_order)
print('modify_order params:')
for p in sig.parameters.values():
    if p.name != 'self':
        default = p.default if p.default is not inspect.Parameter.empty else 'REQUIRED'
        print(f'  {p.name}: {default}')

# Check if cancel_order exists
print('\ncancel_order exists:', hasattr(OpenSecTradeContext, 'cancel_order'))
print('cancel_all_order exists:', hasattr(OpenSecTradeContext, 'cancel_all_order'))
