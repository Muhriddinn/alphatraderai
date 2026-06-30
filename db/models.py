"""
CRYPTO MONITOR PRO — SQLite Database (PostgreSQL o'rniga)
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Boolean,
    DateTime, JSON, ForeignKey, Index, event
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy import select, func


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(100), nullable=True)
    first_name = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    is_premium = Column(Boolean, default=False)
    premium_expires_at = Column(DateTime, nullable=True)
    trial_used = Column(Boolean, default=False)
    trial_expires_at = Column(DateTime, nullable=True)
    signal_count_hour = Column(Integer, default=0)
    signal_count_hour_reset = Column(DateTime, nullable=True)
    settings = relationship("UserSettings", back_populates="user", uselist=False, cascade="all, delete-orphan")
    watchlist = relationship("Watchlist", back_populates="user", cascade="all, delete-orphan")
    alerts_received = relationship("AlertLog", back_populates="user", cascade="all, delete-orphan")


class UserSettings(Base):
    __tablename__ = "user_settings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    volume_enabled = Column(Boolean, default=True)
    oi_enabled = Column(Boolean, default=True)
    liquidation_enabled = Column(Boolean, default=True)
    whale_enabled = Column(Boolean, default=True)
    orderbook_enabled = Column(Boolean, default=True)
    funding_enabled = Column(Boolean, default=True)
    listing_alerts = Column(Boolean, default=True)
    top_report_enabled = Column(Boolean, default=True)
    only_binance = Column(Boolean, default=True)
    futures_enabled = Column(Boolean, default=True)
    spot_enabled = Column(Boolean, default=False)
    coin_filter = Column(String(20), default="all")
    timezone_offset = Column(Integer, default=0)  # UTC ga nisbatan soat (masalan, Tashkent = +5)
    min_alert_level = Column(String(20), default="notice")
    min_volume_spike = Column(Float, default=200.0)
    min_oi_change = Column(Float, default=5.0)
    min_liq_usdt = Column(Float, default=100000.0)
    min_whale_usdt = Column(Float, default=500000.0)
    alerts_paused = Column(Boolean, default=False)
    pause_until = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow)
    user = relationship("User", back_populates="settings")


class Watchlist(Base):
    __tablename__ = "watchlist"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    symbol = Column(String(20), nullable=False)
    exchange = Column(String(20), default="binance")
    added_at = Column(DateTime, default=datetime.utcnow)
    user = relationship("User", back_populates="watchlist")
    __table_args__ = (Index("ix_watchlist_user_symbol", "user_id", "symbol", unique=True),)


class AlertLog(Base):
    __tablename__ = "alert_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    symbol = Column(String(20), nullable=False, index=True)
    exchange = Column(String(20), nullable=False)
    alert_level = Column(String(20), nullable=False)
    score = Column(Float, nullable=False)
    price = Column(Float, nullable=True)
    events_triggered = Column(JSON, nullable=True)
    sent_at = Column(DateTime, default=datetime.utcnow, index=True)
    user = relationship("User", back_populates="alerts_received")


class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    amount_usdt = Column(Float, nullable=False)
    plan = Column(String(20), nullable=False)
    wallet_address = Column(String(100), nullable=False)
    tx_hash = Column(String(100), nullable=True)
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    approved_at = Column(DateTime, nullable=True)
    user = relationship("User", foreign_keys=[user_id])


# SQLite engine
engine = create_async_engine(
    "sqlite+aiosqlite:///crypto_monitor.db",
    echo=False,
)

AsyncSessionFactory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    from loguru import logger
    logger.info("✅ SQLite database ready")


async def get_db():
    async with AsyncSessionFactory() as session:
        yield session
