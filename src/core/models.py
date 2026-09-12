from dataclasses import dataclass
from typing import Optional
from enum import Enum
from datetime import datetime

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"

class OrderStatus(Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

class TimeFrame(Enum):
    MINUTE = "1m"
    HOUR = "1h"
    DAY = "1d"

@dataclass
class Order:
    symbol: str
    quantity: float
    side: OrderSide
    order_type: OrderType
    price: Optional[float] = None
    order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.PENDING

@dataclass
class Position:
    symbol: str
    quantity: float
    average_price: float
    current_price: Optional[float] = None
    pnl: Optional[float] = None

@dataclass
class AccountBalance:
    cash: float
    buying_power: float
    equity: float

@dataclass
class MarketData:
    symbol: str
    timestamp: datetime
    last_price: float
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    volume: Optional[float] = None

@dataclass
class Candlestick:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
