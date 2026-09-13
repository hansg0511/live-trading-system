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
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class OperationType(Enum):
    ENTRY = "entry"
    EXIT = "exit"


class OperationStatus(Enum):
    CREATED = "created"
    LEG1_SUBMITTING = "leg1_submitting"
    LEG1_SUBMITTED = "leg1_submitted"
    LEG2_SUBMITTING = "leg2_submitting"
    LEG2_SUBMITTED = "leg2_submitted"
    PARTIALLY_FILLED = "partially_filled"
    OPEN = "open"
    FAILED = "failed"
    REQUIRES_RECONCILIATION = "requires_reconciliation"
    CLOSED = "closed"


class LegStatus(Enum):
    CREATED = "created"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"
    REQUIRES_RECONCILIATION = "requires_reconciliation"

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
    remark: Optional[str] = None

@dataclass
class Position:
    symbol: str
    quantity: float
    average_price: float
    current_price: Optional[float] = None
    pnl: Optional[float] = None
    side: Optional[str] = None
    broker_position_id: Optional[str] = None

@dataclass
class AccountBalance:
    cash: float
    buying_power: float
    equity: float
    initial_margin: Optional[float] = None
    maintenance_margin: Optional[float] = None
    account_id: Optional[str] = None

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
