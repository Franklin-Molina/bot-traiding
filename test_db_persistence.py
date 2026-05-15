import asyncio
from infrastructure.database import async_session, init_db
from models.trading import Position, Slot, SlotStatus
from sqlalchemy import select
from loguru import logger

async def test_persistence():
    logger.info("Iniciando prueba de persistencia...")
    await init_db()
    
    async with async_session() as session:
        # 1. Asegurar que haya al menos un slot
        query = select(Slot).limit(1)
        result = await session.execute(query)
        slot = result.scalar_one_or_none()
        
        if not slot:
            logger.info("Creando slot de prueba...")
            slot = Slot(status=SlotStatus.AVAILABLE, assigned_capital=100.0)
            session.add(slot)
            await session.commit()
            await session.refresh(slot)
        
        logger.info(f"Usando Slot ID: {slot.id}, Status: {slot.status}")
        
        # 2. Intentar insertar una posición
        try:
            logger.info("Insertando posición de prueba...")
            new_pos = Position(
                symbol="BTCUSDT",
                buy_price=50000.0,
                quantity=0.001,
                slot_id=slot.id,
                take_profit=51000.0,
                stop_loss=49000.0
            )
            session.add(new_pos)
            await session.commit()
            logger.success("✅ Posición insertada correctamente.")
            
            # 3. Verificar que se guardó
            query = select(Position).where(Position.symbol == "BTCUSDT")
            result = await session.execute(query)
            saved_pos = result.scalar_one_or_none()
            if saved_pos:
                logger.success(f"✅ Verificación exitosa: Encontrada posición {saved_pos.id} para {saved_pos.symbol}")
                
                # Limpiar (opcional, pero mejor dejarlo para ver en la DB si el usuario quiere)
                # await session.delete(saved_pos)
                # await session.commit()
            else:
                logger.error("❌ La posición no se encontró después del commit.")
                
        except Exception as e:
            logger.error(f"❌ Error durante la persistencia: {e}")
            await session.rollback()

if __name__ == "__main__":
    asyncio.run(test_persistence())
