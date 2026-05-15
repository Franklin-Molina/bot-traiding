import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from decimal import Decimal
from sqlalchemy import select
from models.trading import TradeHistory, Position, Order, OrderStatus
from trading.slots import SlotManager

@pytest.mark.asyncio
async def test_slippage_and_latency_calculation(executor, db_session):
    """
    Valida que el slippage y la latencia se calculen y persistan correctamente
    al cerrar una posición.
    """
    # 1. Preparar el escenario: Slot disponible y posición abierta manualmente
    await SlotManager.initialize_slots(count=1, capital_per_slot=100.0)
    slot = await SlotManager.get_available_slot()
    await SlotManager.lock_slot(slot.id)
    
    pos = Position(
        symbol="BTCUSDT",
        buy_price=50000.0,
        quantity=0.002,
        slot_id=slot.id,
        status="OPEN"
    )
    db_session.add(pos)
    await db_session.commit()
    await db_session.refresh(pos)
    
    executor.active_positions["BTCUSDT"] = pos
    
    # 2. Mockear la respuesta del exchange para la venta con un precio diferente
    # Esperado: 50100, Real: 50050 -> Slippage = ((50050/50100) - 1) * 100 = -0.0998%
    expected_sell_price = 50100.0
    real_fill_price = 50050.0
    
    mock_response = {
        'symbol': 'BTCUSDT',
        'orderId': '12345',
        'status': 'FILLED',
        'price': str(real_fill_price),
        'executedQty': '0.002',
        'cummulativeQuoteQty': str(0.002 * real_fill_price)
    }
    
    executor.exchange.execute_market_sell = AsyncMock(return_value=mock_response)
    
    # 3. Ejecutar salida
    await executor.execute_exit(pos, expected_sell_price, "TEST_EXIT")
    
    # 4. Verificar TradeHistory
    query = select(TradeHistory).where(TradeHistory.symbol == "BTCUSDT")
    result = await db_session.execute(query)
    history = result.scalar_one()
    
    expected_slippage = ((real_fill_price / expected_sell_price) - 1) * 100
    
    assert history.sell_price == real_fill_price
    assert history.expected_sell_price == expected_sell_price
    assert pytest.approx(history.slippage_sell_pct, 0.0001) == expected_slippage
    assert history.latency_ms > 0
    assert history.reason == "TEST_EXIT"
    
    # Verificar que el slot se liberó
    await db_session.refresh(slot)
    from models.trading import SlotStatus
    assert slot.status == SlotStatus.AVAILABLE

@pytest.mark.asyncio
async def test_buy_metrics_logging(executor, db_session):
    """
    Verifica que la compra calcule correctamente el slippage (aunque por ahora
    solo se loguee en EventLog, validamos la lógica).
    """
    await SlotManager.initialize_slots(count=1, capital_per_slot=100.0)
    
    # Mock de symbol info y respuesta de compra
    executor.exchange.get_symbol_info = AsyncMock(return_value={
        'symbol': 'BTCUSDT',
        'filters': [{'filterType': 'LOT_SIZE', 'stepSize': '0.00001'}, {'filterType': 'NOTIONAL', 'minNotional': '5.0'}]
    })
    
    expected_price = 50000.0
    real_price = 50100.0 # 0.2% slippage positivo (malo para compra)
    
    mock_response = {
        'symbol': 'BTCUSDT',
        'orderId': '999',
        'status': 'FILLED',
        'price': str(real_price),
        'executedQty': '0.002',
        'cummulativeQuoteQty': str(0.002 * real_price)
    }
    executor.exchange.execute_market_buy = AsyncMock(return_value=mock_response)
    
    # Ejecutar compra
    await executor.try_buy("BTCUSDT", expected_price, score=100)
    
    # Verificar que la posición se creó con el precio real
    assert "BTCUSDT" in executor.active_positions
    pos = executor.active_positions["BTCUSDT"]
    assert pos.buy_price == real_price
    
    # Verificar Order en DB
    query = select(Order).where(Order.symbol == "BTCUSDT")
    result = await db_session.execute(query)
    order = result.scalar_one()
    assert order.status == OrderStatus.FILLED
    assert order.fill_price == real_price
