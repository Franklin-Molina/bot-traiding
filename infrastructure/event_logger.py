import asyncio
import json
from datetime import datetime, UTC

from loguru import logger

from infrastructure.database import async_session
from models.trading import EventLog


class EventLogger:
    """
    WAL (Write Ahead Log) asíncrono y desacoplado.

    Objetivos:
    - No bloquear path crítico
    - Persistencia batch eficiente
    - Backpressure controlado
    - Shutdown limpio
    - Timestamps timezone-aware
    """

    def __init__(self, queue_size: int = 5000):

        self.queue = asyncio.Queue(maxsize=queue_size)

        self.is_running = False

        self._worker_task = None

        self.batch_size = 50
        self.max_wait = 1.0

    async def log_event(
        self,
        event_type: str,
        symbol: str,
        data: dict
    ):
        """
        Añade evento a cola WAL.

        Nunca debe bloquear el trading engine.
        """

        try:

            payload = {
                "type": event_type,
                "symbol": symbol,
                "data": json.dumps(data, default=str),
                "timestamp": datetime.now(UTC)
            }

            self.queue.put_nowait(payload)

        except asyncio.QueueFull:

            logger.warning(
                f"WAL queue llena. "
                f"Descartando evento={event_type} "
                f"symbol={symbol}"
            )

        except Exception as e:

            logger.exception(
                f"Error serializando evento WAL: {e}"
            )

    async def start(self):

        if self.is_running:
            logger.warning("EventLogger ya iniciado.")
            return

        self.is_running = True

        self._worker_task = asyncio.create_task(
            self._persistence_worker()
        )

        logger.info("✅ EventLogger Worker iniciado.")

    async def stop(self):
        """
        Shutdown limpio.
        """

        self.is_running = False

        if not self._worker_task:
            return

        try:

            await self.queue.join()

            self._worker_task.cancel()

            try:
                await self._worker_task

            except asyncio.CancelledError:
                pass

        finally:

            logger.info("🛑 EventLogger Worker detenido.")

    async def _persistence_worker(self):

        batch = []

        while self.is_running or not self.queue.empty():

            try:

                try:

                    event = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.max_wait
                    )

                    batch.append(event)

                except asyncio.TimeoutError:
                    pass

                should_flush = (
                    len(batch) >= self.batch_size
                    or (
                        batch
                        and (
                            not self.is_running
                            or self.queue.empty()
                        )
                    )
                )

                if should_flush:

                    success = await self._flush_batch(batch)

                    if success:

                        for _ in batch:
                            self.queue.task_done()

                        logger.debug(
                            f"WAL flush exitoso "
                            f"({len(batch)} eventos)"
                        )

                        batch.clear()

                    else:

                        logger.warning(
                            "Flush WAL falló. "
                            "Reintentando batch..."
                        )

                        await asyncio.sleep(1)

            except asyncio.CancelledError:

                logger.warning(
                    "Worker WAL cancelado."
                )

                break

            except Exception as e:

                logger.exception(
                    f"Error crítico en WAL worker: {e}"
                )

                await asyncio.sleep(1)

        # Flush final de seguridad
        if batch:

            logger.warning(
                f"Flush final WAL "
                f"({len(batch)} eventos restantes)"
            )

            success = await self._flush_batch(batch)

            if success:

                for _ in batch:
                    self.queue.task_done()

    async def _flush_batch(self, batch) -> bool:

        if not batch:
            return True

        try:

            async with async_session() as session:

                logs = [
                    EventLog(
                        event_type=e["type"],
                        symbol=e["symbol"],
                        data=e["data"],
                        timestamp=e["timestamp"]
                    )
                    for e in batch
                ]

                session.add_all(logs)

                await session.commit()

                return True

        except Exception as e:

            logger.exception(
                f"Error flush WAL batch: {e}"
            )

            return False


# Singleton global
event_logger = EventLogger()