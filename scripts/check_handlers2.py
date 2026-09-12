from moomoo import OpenSecTradeContext
import inspect

sig = inspect.signature(OpenSecTradeContext.set_handler)
print('set_handler signature:')
for p in sig.parameters.values():
    default = p.default if p.default is not inspect.Parameter.empty else 'REQUIRED'
    annotation = p.annotation if p.annotation is not inspect.Parameter.empty else 'any'
    print(f'  {p.name}: {annotation} = {default}')

# Check if handler is a property or method
print('\nset_handler doc:', OpenSecTradeContext.set_handler.__doc__)

# Also check what methods exist on TradeOrderHandlerBase
from moomoo import TradeOrderHandlerBase, TradeDealHandlerBase
print('\nTradeOrderHandlerBase methods:', [m for m in dir(TradeOrderHandlerBase) if not m.startswith('__')])
print('TradeDealHandlerBase methods:', [m for m in dir(TradeDealHandlerBase) if not m.startswith('__')])
