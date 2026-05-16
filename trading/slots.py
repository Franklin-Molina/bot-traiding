from sqlalchemy import select, update, func
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
    async def get_used_slots() -> int:
        """
        Retorna la cantidad de slots actualmente en uso.
        """
        async with async_session() as session:
            query = select(func.count(Slot.id)).where(Slot.status == SlotStatus.IN_USE)
            result = await session.execute(query)
            return result.scalar() or 0

    @staticmethod
    async def get_total_slots() -> int:
        """
        Retorna la cantidad total de slots configurados.
        """
        async with async_session() as session:
            query = select(func.count(Slot.id))
            result = await session.execute(query)
            return result.scalar() or 0

    @staticmethod
    async def initialize_slots(count: int, capital_per_slot: float):
        """
        Crea o ajusta los slots iniciales según la configuración.
        """
        async with async_session() as session:
            query = select(Slot)
            result = await session.execute(query)
            existing_slots = result.scalars().all()
            current_count = len(existing_slots)

            if current_count < count:
                to_add = count - current_count
                logger.info(f"Ajustando slots: {current_count} -> {count}. Creando {to_add} nuevos slots.")
                for _ in range(to_add):
                    new_slot = Slot(status=SlotStatus.AVAILABLE, assigned_capital=capital_per_slot)
                    session.add(new_slot)
                await session.commit()
            elif current_count > count:
                logger.warning(f"Tienes {current_count} slots pero la configuración pide {count}. No se eliminarán slots activos.")
