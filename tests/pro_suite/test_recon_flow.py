import pytest
import asyncio
from unittest.mock import AsyncMock
from sqlalchemy import select
from models.trading import Order, OrderStatus, Position, SlotStatus, PositionStatus
from trading.reconciliation import ReconciliationEngine
from trading.slots import SlotManager



@pytest.mark.asyncio
async def test_reconciliation_orphan_buy(db_session, mock_exchange):
    """
    Prueba que el motor de reconciliación detecte una orden de compra 
    que fue ejecutada (FILLED) pero no persistida como Posición en la DB local.
    """
    # 1. Preparar slots
    await SlotManager.initialize_slots(count=2, capital_per_slot=100.0)
    
    # 2. Insertar una orden "huérfana" en la DB (SUBMITTED pero sin posición)
    client_id = "buy_orphan_123"
    orphan_order = Order(
        client_order_id=client_id,
        symbol="ETHUSDT",
        side="BUY",
        status=OrderStatus.SUBMITTED,
        price=2000.0,
        quantity=0.05
    )
    db_session.add(orphan_order)
    await db_session.commit()
    
    # 3. Configurar mock del exchange para que devuelva FILLED
    fill_price = 2005.0
    mock_exchange.get_order_status = AsyncMock(return_value={
        'symbol': 'ETHUSDT',
        'orderId': 'ext_999',
        'clientOrderId': client_id,
        'status': 'FILLED',
        'price': str(fill_price),
        'executedQty': '0.05',
        'cummulativeQuoteQty': str(0.05 * fill_price)
    })
    
    # 4. Ejecutar reconciliación
    engine = ReconciliationEngine(mock_exchange)
    await engine.reconcile_orders()
    
    # Asegurar que los cambios de la reconciliación sean visibles en la sesión principal
    await db_session.flush()
    
    # 5. Verificaciones
    # A. La orden debe estar FILLED en DB
    await db_session.refresh(orphan_order)
    assert orphan_order.status == OrderStatus.FILLED
    assert orphan_order.fill_price == fill_price
    
    # B. Se debe haber creado una Posición
    query_pos = select(Position).where(Position.symbol == "ETHUSDT")
    result_pos = await db_session.execute(query_pos)
    pos = result_pos.scalar_one()
    assert pos.buy_price == fill_price
    assert pos.status == PositionStatus.OPEN
    assert pos.slot_id is not None
    
    # C. El slot debe estar ocupado (IN_USE)
    # SlotManager.lock_slot marca como IN_USE
    from models.trading import Slot
    query_slot = select(Slot).where(Slot.id == pos.slot_id)
    result_slot = await db_session.execute(query_slot)
    slot = result_slot.scalar_one()
    assert slot.status == SlotStatus.IN_USE
