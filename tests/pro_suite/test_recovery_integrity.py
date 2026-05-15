import pytest
import asyncio
from models.trading import Position
from trading.slots import SlotManager
from trading.executor import TradeExecutor
from sqlalchemy import select

@pytest.mark.asyncio
async def test_recovery_persistence_replay(db_session, alert_queue, mock_exchange):
    """
    Simula un crash/reinicio y verifica que load_active_positions
    recupere correctamente el estado de las posiciones abiertas.
    """
    # 1. Preparar slots y posiciones en DB
    await SlotManager.initialize_slots(count=5, capital_per_slot=100.0)
    
    pos1 = Position(
        symbol="BTCUSDT",
        buy_price=50000.0,
        quantity=0.002,
        slot_id=1,
        status="OPEN",
        stop_loss=49000.0,
        take_profit=52000.0,
        highest_price=50500.0
    )
    pos2 = Position(
        symbol="ETHUSDT",
        buy_price=3000.0,
        quantity=0.1,
        slot_id=2,
        status="OPEN",
        stop_loss=2900.0,
        take_profit=3200.0,
        highest_price=3000.0 # highest_price nulo o igual al buy_price
    )
    
    db_session.add(pos1)
    db_session.add(pos2)
    await db_session.commit()
    
    # 2. Crear un nuevo executor (simulando reinicio)
    new_executor = TradeExecutor(alert_queue, exchange=mock_exchange)
    
    # 3. Cargar posiciones
    # Nota: load_active_positions usa async_session() global de infrastructure.database
    # En los tests, conftest.py debería haber configurado el engine/session para usar la de memoria.
    # Pero TradeExecutor.load_active_positions importa async_session de infrastructure.database.
    # Para que funcione, necesitamos que infrastructure.database.async_session esté apuntando al motor de test.
    
    with patch("trading.executor.async_session", return_value=db_session):
        await new_executor.load_active_positions()
    
    # 4. Verificaciones
    assert len(new_executor.active_positions) == 2
    assert "BTCUSDT" in new_executor.active_positions
    assert "ETHUSDT" in new_executor.active_positions
    
    btc_pos = new_executor.active_positions["BTCUSDT"]
    assert btc_pos.stop_loss == 49000.0
    assert btc_pos.highest_price == 50500.0
    
    eth_pos = new_executor.active_positions["ETHUSDT"]
    # Verificar lógica de fallback para highest_price
    assert eth_pos.highest_price == 3000.0

from unittest.mock import patch
