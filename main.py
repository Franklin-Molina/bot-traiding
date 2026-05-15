import asyncio
import signal
from contextlib import suppress
from loguru import logger

from bot.main import start_bot
from trading.engine import start_trading_engine
from trading.macro import MacroEngine
from trading.reconciliation import ReconciliationEngine
from core.config import settings
from infrastructure.database import init_db
from infrastructure.event_logger import event_logger
from infrastructure.binance_rest import BinanceRest
from infrastructure.paper_exchange import PaperExchange
from trading.slots import SlotManager

shutdown_event = asyncio.Event()

async def shutdown():
    logger.warning("Apagando sistema...")
    shutdown_event.set()
    await event_logger.stop()

async def main():
    logger.info("🚀 Iniciando Arquitectura Maestro V2.0 PRO")

    # 1. Inicializar infraestructura y WAL
    await init_db()
    await event_logger.start()
    await event_logger.log_event("SYSTEM_START", None, {"mode": "PRO", "simulation": settings.SIMULATION_MODE})

    # 2. Configuración de Slots
    await SlotManager.initialize_slots(
        count=settings.MAX_OPEN_POSITIONS,
        capital_per_slot=settings.USDT_PER_SLOT
    )
    logger.info("✅ Slots inicializados")

    # 3. Colas desacopladas
    market_queue = asyncio.Queue(maxsize=1000)
    strategy_queue = asyncio.Queue(maxsize=50)
    alert_queue = asyncio.Queue(maxsize=100)

    # 4. Exchange Interface para Reconciliación
    exchange = PaperExchange() if settings.SIMULATION_MODE else BinanceRest()

    # 5. Motores
    macro_engine = MacroEngine(strategy_queue)
    recon_engine = ReconciliationEngine(exchange, interval=60) # Cada 1 min

    tasks = [
        asyncio.create_task(start_bot(alert_queue), name="telegram_bot"),
        asyncio.create_task(macro_engine.run_cycle(), name="macro_engine"),
        asyncio.create_task(start_trading_engine(market_queue, strategy_queue, alert_queue), name="trading_engine"),
        asyncio.create_task(recon_engine.start(), name="recon_engine")
    ]

    logger.info("✅ Componentes PRO iniciados")

    try:
        while not shutdown_event.is_set():
            done, pending = await asyncio.wait(
                tasks,
                timeout=1,
                return_when=asyncio.FIRST_EXCEPTION
            )

            for task in done:
                exc = task.exception()
                if exc:
                    logger.critical(f"❌ Task '{task.get_name()}' falló: {exc}")
                    shutdown_event.set()

    except asyncio.CancelledError:
        logger.warning("Main cancelado")
    finally:
        logger.warning("🛑 Cerrando tareas...")
        for t in tasks: t.cancel()
        for t in tasks:
            with suppress(asyncio.CancelledError):
                await t
        await event_logger.stop()
        logger.info("✅ Sistema PRO apagado correctamente")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
