import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from models.trading import Position, TradeHistory
from trading.slots import SlotManager


HYSTERESIS_THRESHOLD = Decimal("0.001")  # 0.1%
TRAILING_MULTIPLIER = Decimal("0.985")


@pytest.mark.asyncio
async def test_trailing_stop_progression(executor, db_session):
    """
    Verifica:
    - Trailing stop dinámico
    - Histéresis anti-spam
    - El SL nunca retrocede
    - Highest price siempre avanza
    """

    await SlotManager.initialize_slots(
        count=1,
        capital_per_slot=100.0
    )

    slot = await SlotManager.get_available_slot()

    pos = Position(
        symbol="TRLUSDT",
        buy_price=100.0,
        quantity=1.0,
        slot_id=slot.id,
        stop_loss=98.5,
        take_profit=105.0,
        highest_price=100.0,
        last_sl_update_price=100.0,
        status="OPEN"
    )

    db_session.add(pos)
    await db_session.commit()
    await db_session.refresh(pos)

    executor.active_positions["TRLUSDT"] = pos

    # =========================================================
    # SUBIDA IMPORTANTE -> DEBE ACTUALIZAR SL
    # =========================================================

    await executor.monitor_and_exit("TRLUSDT", 101.0)

    await db_session.refresh(pos)

    expected_sl = float(
        Decimal("101.0") * TRAILING_MULTIPLIER
    )

    assert pos.highest_price == pytest.approx(
        101.0,
        rel=1e-6
    )

    assert pos.stop_loss == pytest.approx(
        expected_sl,
        rel=1e-6
    )

    prev_sl = pos.stop_loss

    # =========================================================
    # MICRO SUBIDA -> NO DEBE ACTUALIZAR SL
    # =========================================================

    await executor.monitor_and_exit("TRLUSDT", 101.05)

    await db_session.refresh(pos)

    # Highest puede actualizarse
    assert pos.highest_price == pytest.approx(
        101.05,
        rel=1e-6
    )

    # PERO el SL no debe moverse
    assert pos.stop_loss == pytest.approx(
        prev_sl,
        rel=1e-6
    )

    # =========================================================
    # BAJADA -> SL NUNCA RETROCEDE
    # =========================================================

    await executor.monitor_and_exit("TRLUSDT", 100.0)

    await db_session.refresh(pos)

    assert pos.stop_loss == pytest.approx(
        prev_sl,
        rel=1e-6
    )

    assert pos.highest_price == pytest.approx(
        101.05,
        rel=1e-6
    )


@pytest.mark.asyncio
async def test_stop_loss_trigger(executor, db_session):
    """
    Verifica cierre correcto por Stop Loss.
    """

    await SlotManager.initialize_slots(
        count=1,
        capital_per_slot=100.0
    )

    slot = await SlotManager.get_available_slot()

    pos = Position(
        symbol="SLUSDT",
        buy_price=100.0,
        quantity=1.0,
        slot_id=slot.id,
        stop_loss=98.0,
        take_profit=110.0,
        highest_price=100.0,
        status="OPEN"
    )

    db_session.add(pos)

    await db_session.commit()
    await db_session.refresh(pos)

    executor.active_positions["SLUSDT"] = pos

    executor.exchange.execute_market_sell = AsyncMock(
        return_value={
            "status": "FILLED",
            "price": "97.5",
            "executedQty": "1.0",
            "cummulativeQuoteQty": "97.5"
        }
    )

    await executor.monitor_and_exit(
        "SLUSDT",
        97.9
    )

    assert "SLUSDT" not in executor.active_positions

    query = select(TradeHistory).where(
        TradeHistory.symbol == "SLUSDT"
    )

    result = await db_session.execute(query)

    history = result.scalar_one()

    assert history.reason == "STOP_LOSS"

    assert history.sell_price == pytest.approx(
        97.5,
        rel=1e-6
    )


@pytest.mark.asyncio
async def test_take_profit_trigger(executor, db_session):
    """
    Verifica cierre correcto por Take Profit.
    """

    await SlotManager.initialize_slots(
        count=1,
        capital_per_slot=100.0
    )

    slot = await SlotManager.get_available_slot()

    pos = Position(
        symbol="TPUSDT",
        buy_price=100.0,
        quantity=1.0,
        slot_id=slot.id,
        stop_loss=90.0,
        take_profit=105.0,
        highest_price=100.0,
        status="OPEN"
    )

    db_session.add(pos)

    await db_session.commit()
    await db_session.refresh(pos)

    executor.active_positions["TPUSDT"] = pos

    executor.exchange.execute_market_sell = AsyncMock(
        return_value={
            "status": "FILLED",
            "price": "105.1",
            "executedQty": "1.0",
            "cummulativeQuoteQty": "105.1"
        }
    )

    await executor.monitor_and_exit(
        "TPUSDT",
        105.5
    )

    # Refrescar estado interno
    assert "TPUSDT" not in executor.active_positions

    query = select(TradeHistory).where(
        TradeHistory.symbol == "TPUSDT"
    )

    result = await db_session.execute(query)

    history = result.scalar_one()

    assert history.reason == "TAKE_PROFIT"

    assert history.sell_price == pytest.approx(
        105.1,
        rel=1e-6
    )