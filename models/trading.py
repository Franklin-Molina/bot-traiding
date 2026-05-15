from datetime import datetime, UTC
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Enum,
    ForeignKey,
    Text
)
from sqlalchemy.orm import DeclarativeBase, relationship
import enum


class Base(DeclarativeBase):
    pass


# =========================
# ENUMS
# =========================

class SlotStatus(enum.Enum):
    AVAILABLE = "available"
    IN_USE = "in_use"
    LOCKED = "locked"


class OrderStatus(enum.Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class PositionStatus(enum.Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


# =========================
# MODELS
# =========================

class Slot(Base):
    __tablename__ = "slots"

    id = Column(Integer, primary_key=True)

    status = Column(
        Enum(SlotStatus),
        default=SlotStatus.AVAILABLE,
        nullable=False
    )

    assigned_capital = Column(Float, nullable=False)

    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC)
    )

    position = relationship(
        "Position",
        back_populates="slot",
        uselist=False
    )


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)

    client_order_id = Column(
        String,
        unique=True,
        index=True,
        nullable=False
    )

    exchange_order_id = Column(
        String,
        unique=True,
        index=True,
        nullable=True
    )

    symbol = Column(String, nullable=False, index=True)

    side = Column(String, nullable=False)

    type = Column(String, default="MARKET")

    status = Column(
        Enum(OrderStatus),
        default=OrderStatus.PENDING,
        nullable=False
    )

    price = Column(Float)

    fill_price = Column(Float)

    quantity = Column(Float)

    executed_quantity = Column(Float, default=0.0)

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC)
    )

    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC)
    )


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True)

    symbol = Column(String, nullable=False, index=True)

    buy_price = Column(Float, nullable=False)

    quantity = Column(Float, nullable=False)

    take_profit = Column(Float)

    stop_loss = Column(Float)

    highest_price = Column(Float)

    last_sl_update_price = Column(Float)

    status = Column(
        Enum(PositionStatus),
        default=PositionStatus.OPEN,
        nullable=False
    )

    slot_id = Column(
        Integer,
        ForeignKey("slots.id"),
        unique=True
    )

    opened_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC)
    )

    slot = relationship(
        "Slot",
        back_populates="position"
    )


class TradeHistory(Base):
    __tablename__ = "trade_history"

    id = Column(Integer, primary_key=True)

    symbol = Column(String, nullable=False, index=True)

    buy_price = Column(Float)

    sell_price = Column(Float)

    quantity = Column(Float)

    pnl = Column(Float)

    pnl_percent = Column(Float)

    # Journal profesional
    expected_buy_price = Column(Float)

    expected_sell_price = Column(Float)

    slippage_buy_pct = Column(Float)

    slippage_sell_pct = Column(Float)

    latency_ms = Column(Float)

    volatility_regime = Column(String)

    atr_at_execution = Column(Float)

    closed_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC)
    )

    reason = Column(String)


class EventLog(Base):
    """
    WAL (Write Ahead Log)
    """

    __tablename__ = "event_log"

    id = Column(Integer, primary_key=True)

    event_type = Column(String, index=True)

    symbol = Column(String, index=True, nullable=True)

    data = Column(Text)

    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        index=True
    )