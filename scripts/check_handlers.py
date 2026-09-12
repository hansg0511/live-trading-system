from moomoo import OpenSecTradeContext
import inspect

for name in dir(OpenSecTradeContext):
    if 'handler' in name.lower() or 'push' in name.lower() or 'set' in name.lower() or 'callback' in name.lower():
        print(name)

sig = inspect.signature(OpenSecTradeContext.__init__)
print('\n__init__ params:')
for p in sig.parameters.values():
    if p.name != 'self':
        default = p.default if p.default is not inspect.Parameter.empty else 'REQUIRED'
        print(f'  {p.name}: {default}')
