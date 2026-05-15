import asyncio
from infrastructure.database import async_session
from models.trading import Slot, Position
from sqlalchemy import select
from loguru import logger

async def check_status():
    async with async_session() as session:
        # Check slots
        query = select(Slot)
        result = await session.execute(query)
        slots = result.scalars().all()
        logger.info(f"Total slots: {len(slots)}")
        for s in slots:
            logger.info(f"Slot {s.id}: Status={s.status}, Capital={s.assigned_capital}")
        
        # Check positions
        query = select(Position)
        result = await session.execute(query)
        positions = result.scalars().all()
        logger.info(f"Total positions: {len(positions)}")
        for p in positions:
            logger.info(f"Position {p.id}: Symbol={p.symbol}, Slot={p.slot_id}")

if __name__ == "__main__":
    asyncio.run(check_status())
