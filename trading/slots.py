from sqlalchemy import select, update
from infrastructure.database import async_session
from models.trading import Slot, SlotStatus
from loguru import logger

class SlotManager:
    @staticmethod
    async def get_available_slot():
        """
        Busca el primer slot disponible en la base de datos.
        """
        async with async_session() as session:
            query = select(Slot).where(Slot.status == SlotStatus.AVAILABLE).limit(1)
            result = await session.execute(query)
            slot = result.scalar_one_or_none()
            return slot

    @staticmethod
    async def lock_slot(slot_id: int):
        """
        Bloquea un slot para su uso.
        """
        async with async_session() as session:
            stmt = (
                update(Slot)
                .where(Slot.id == slot_id)
                .values(status=SlotStatus.IN_USE)
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Slot {slot_id} bloqueado exitosamente.")

    @staticmethod
    async def release_slot(slot_id: int):
        """
        Libera un slot volviéndolo disponible.
        """
        async with async_session() as session:
            stmt = (
                update(Slot)
                .where(Slot.id == slot_id)
                .values(status=SlotStatus.AVAILABLE)
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Slot {slot_id} liberado exitosamente.")

    @staticmethod
    async def initialize_slots(count: int, capital_per_slot: float):
        """
        Crea los slots iniciales si no existen.
        """
        async with async_session() as session:
            # Verificar si ya hay slots
            query = select(Slot)
            result = await session.execute(query)
            if not result.scalars().all():
                logger.info(f"Inicializando {count} slots con {capital_per_slot} USD cada uno.")
                for _ in range(count):
                    new_slot = Slot(status=SlotStatus.AVAILABLE, assigned_capital=capital_per_slot)
                    session.add(new_slot)
                await session.commit()
