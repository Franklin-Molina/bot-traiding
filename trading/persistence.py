import asyncio
import time
from typing import Dict, Any, List
from loguru import logger
from sqlalchemy import update
from infrastructure.database import async_session
from models.trading import Position

class PersistenceManager:
    """
    Gestiona actualizaciones de base de datos en batch para evitar latencia en el loop principal.
    Especialmente diseñado para actualizaciones frecuentes de SL y precios máximos.
    """
    def __init__(self, flush_interval: float = 1.0, batch_size: int = 50):
        self.queue = asyncio.Queue(maxsize=1000)
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self.is_running = False
        self._worker_task = None
        self._pending_updates: Dict[int, Dict[str, Any]] = {}

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        self._worker_task = asyncio.create_task(self._worker())
        logger.info("✅ PersistenceManager iniciado.")

    async def stop(self):
        self.is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        # Flush final
        if self._pending_updates:
            await self._flush()

    def enqueue_position_update(self, pos_id: int, updates: Dict[str, Any]):
        """
        Añade una actualización a la cola. 
        Si ya hay una actualización pendiente para ese pos_id, se fusionan (sobrescribiendo con lo más nuevo).
        """
        if pos_id not in self._pending_updates:
            self._pending_updates[pos_id] = {}
        self._pending_updates[pos_id].update(updates)

    async def _worker(self):
        while self.is_running:
            try:
                await asyncio.sleep(self.flush_interval)
                if self._pending_updates:
                    await self._flush()
            except Exception as e:
                logger.error(f"Error en worker de PersistenceManager: {e}")
                await asyncio.sleep(1)

    async def _flush(self):
        """Persiste todas las actualizaciones acumuladas en una sola transacción."""
        if not self._pending_updates:
            return

        updates_to_process = self._pending_updates.copy()
        self._pending_updates.clear()

        start_time = time.perf_counter()
        try:
            async with async_session() as session:
                for pos_id, values in updates_to_process.items():
                    await session.execute(
                        update(Position).where(Position.id == pos_id).values(**values)
                    )
                await session.commit()
            
            duration = (time.perf_counter() - start_time) * 1000
            logger.debug(f"Persistence flush: {len(updates_to_process)} posiciones actualizadas en {duration:.2f}ms")
        except Exception as e:
            logger.error(f"Error durante flush de PersistenceManager: {e}")
            # En caso de error, devolvemos las actualizaciones no procesadas (simplificado)
            for pos_id, values in updates_to_process.items():
                if pos_id not in self._pending_updates:
                    self._pending_updates[pos_id] = values
                else:
                    # Mezclar, priorizando lo que ya volvió a entrar
                    merged = values.copy()
                    merged.update(self._pending_updates[pos_id])
                    self._pending_updates[pos_id] = merged

# Singleton
persistence_manager = PersistenceManager()
