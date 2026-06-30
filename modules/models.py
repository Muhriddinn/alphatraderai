"""
CRYPTO MONITOR PRO — Core Data Models
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class Exchange(str, Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    MEXC = "mexc"
    GATE = "gate"


class MarketType(str, Enum):
    FUTURES = "futures"
    SPOT = "spot"


class AlertLevel(str, Enum):
    NOTICE = "notice"           # 50-69
    STRONG = "strong"           # 70-84
    EXTREME = "extreme"         # 85-100


class Direction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    NEUTRAL = "neutral"


# ─────────────────────────────────────────
# MARKET DATA
# ─────────────────────────────────────────

@dataclass
class TickerData:
    symbol: str
    exchange: Exchange
    market_type: MarketType
    price: float
    volume_24h: float
    volume_base: float          # current candle/window volume
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class OIData:
    symbol: str
    exchange: Exchange
    open_interest: float        # in contracts or USDT
    open_interest_usdt: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FundingData:
    symbol: str
    exchange: Exchange
    funding_rate: float
    next_funding_time: datetime
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class LiquidationData:
    symbol: str
    exchange: Exchange
    side: Direction             # BUY = short liq, SELL = long liq
    quantity: float
    price: float
    usdt_value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TradeData:
    symbol: str
    exchange: Exchange
    price: float
    quantity: float
    usdt_value: float
    side: Direction
    is_maker: bool
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class OrderBookLevel:
    price: float
    quantity: float
    usdt_value: float


@dataclass
class OrderBookSnapshot:
    symbol: str
    exchange: Exchange
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    current_price: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ─────────────────────────────────────────
# EVENTS (detected anomalies)
# ─────────────────────────────────────────

@dataclass
class VolumeEvent:
    symbol: str
    exchange: Exchange
    spike_pct: float            # % above baseline
    volume_usdt: float
    start_time: datetime
    duration_seconds: int = 0
    is_whale: bool = False

    @property
    def score(self) -> float:
        if self.spike_pct >= 1000:
            return 20.0
        elif self.spike_pct >= 500:
            return 15.0
        elif self.spike_pct >= 300:
            return 10.0
        elif self.spike_pct >= 200:
            return 7.0
        return 3.0


@dataclass
class OIEvent:
    symbol: str
    exchange: Exchange
    change_pct: float           # + or -
    oi_usdt: float
    start_time: datetime
    speed_per_second: float = 0.0
    duration_seconds: int = 0
    is_rapid: bool = False

    @property
    def score(self) -> float:
        abs_change = abs(self.change_pct)
        base = 0.0
        if abs_change >= 20:
            base = 20.0
        elif abs_change >= 10:
            base = 15.0
        elif abs_change >= 5:
            base = 10.0
        elif abs_change >= 2:
            base = 5.0
        if self.is_rapid:
            base = min(base + 5, 20)
        return base


@dataclass
class LiquidationEvent:
    symbol: str
    exchange: Exchange
    long_liq_usdt: float
    short_liq_usdt: float
    dominant_side: Direction
    start_time: datetime
    duration_seconds: int = 0
    is_wave: bool = False

    @property
    def total_usdt(self) -> float:
        return self.long_liq_usdt + self.short_liq_usdt

    @property
    def score(self) -> float:
        total = self.total_usdt
        if total >= 50_000_000:
            return 15.0
        elif total >= 10_000_000:
            return 12.0
        elif total >= 1_000_000:
            return 8.0
        elif total >= 100_000:
            return 4.0
        return 1.0


@dataclass
class WhaleEvent:
    symbol: str
    exchange: Exchange
    direction: Direction
    volume_usdt: float
    start_time: datetime
    duration_seconds: int = 0
    order_count: int = 1

    @property
    def score(self) -> float:
        if self.volume_usdt >= 10_000_000:
            return 15.0
        elif self.volume_usdt >= 5_000_000:
            return 12.0
        elif self.volume_usdt >= 1_000_000:
            return 8.0
        elif self.volume_usdt >= 500_000:
            return 5.0
        return 2.0


@dataclass
class FundingEvent:
    symbol: str
    exchange: Exchange
    funding_rate: float
    is_extreme: bool
    is_reversal: bool
    previous_rate: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def score(self) -> float:
        abs_rate = abs(self.funding_rate)
        base = 0.0
        if abs_rate >= 0.05:
            base = 15.0
        elif abs_rate >= 0.02:
            base = 12.0
        elif abs_rate >= 0.01:
            base = 10.0
        elif abs_rate >= 0.005:
            base = 6.0
        else:
            base = 3.0
        if self.is_reversal:
            base = min(base + 5, 15)
        return base


@dataclass
class OrderBookEvent:
    symbol: str
    exchange: Exchange
    buy_wall_usdt: float
    sell_wall_usdt: float
    buy_wall_price: float
    sell_wall_price: float
    buy_wall_distance_pct: float
    sell_wall_distance_pct: float
    imbalance_ratio: float
    current_price: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def score(self) -> float:
        ratio = self.imbalance_ratio
        if ratio >= 5.0:
            return 15.0
        elif ratio >= 3.0:
            return 10.0
        elif ratio >= 2.0:
            return 6.0
        return 2.0


@dataclass
class ListingEvent:
    symbol: str
    exchange: Exchange
    market_type: MarketType
    is_listing: bool            # True=listing, False=delisting
    trading_start: Optional[datetime] = None
    trading_stop: Optional[datetime] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ─────────────────────────────────────────
# COMPOSITE ALERT
# ─────────────────────────────────────────

@dataclass
class MarketAlert:
    symbol: str
    exchange: Exchange
    market_type: MarketType
    current_price: float
    level: AlertLevel
    total_score: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Events
    volume_event: Optional[VolumeEvent] = None
    oi_event: Optional[OIEvent] = None
    liq_event: Optional[LiquidationEvent] = None
    whale_event: Optional[WhaleEvent] = None
    funding_event: Optional[FundingEvent] = None
    orderbook_event: Optional[OrderBookEvent] = None

    def get_level_emoji(self) -> str:
        if self.level == AlertLevel.EXTREME:
            return "🔥"
        elif self.level == AlertLevel.STRONG:
            return "🟢"
        return "🟡"
