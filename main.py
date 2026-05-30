import asyncio
import signal
from contextlib import suppress
from loguru import logger

from bot.main import start_bot
from trading.engine import start_trading_engine
from trading.macro import MacroEngine
from trading.reconciliation import ReconciliationEngine
from trading.outcome_tracker import OutcomeTracker
from core.config import settings
from infrastructure.database import init_db
from infrastructure.event_logger import event_logger
from infrastructure.binance_rest import BinanceRest
from infrastructure.paper_exchange import PaperExchange
from trading.slots import SlotManager

shutdown_event = asyncio.Event()

async def main():
    logger.info("🚀 Iniciando Arquitectura Maestro PRO")

    alert_queue = asyncio.Queue(maxsize=100)

    # Redefinir shutdown para que tenga acceso a alert_queue
    async def graceful_shutdown():
        logger.warning("Apagando sistema...")
        shutdown_event.set()
        try:
            await alert_queue.put("⚠️ **Bot Inactivo**")
            # Dar un margen para que Telegram envíe el mensaje
            await asyncio.sleep(2)
        except:
            pass
        await event_logger.stop()

    # Actualizar handler de señales si es posible
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(graceful_shutdown()))

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
    market_queue = asyncio.Queue(maxsize=settings.MARKET_QUEUE_MAXSIZE)
    strategy_queue = asyncio.Queue(maxsize=50)

    # 4. Exchange Interface para Reconciliación
    exchange = PaperExchange() if settings.SIMULATION_MODE else BinanceRest()
    if not settings.SIMULATION_MODE:
        await exchange.sync_time()

    # 5. Motores
    macro_engine = MacroEngine(strategy_queue, alert_queue)
    recon_engine = ReconciliationEngine(exchange, interval=60) # Cada 1 min
    outcome_tracker = OutcomeTracker(interval_minutes=15, timeout_minutes=45)

    tasks = [
        asyncio.create_task(start_bot(alert_queue), name="telegram_bot"),
        asyncio.create_task(macro_engine.run_cycle(), name="macro_engine"),
        asyncio.create_task(start_trading_engine(market_queue, strategy_queue, alert_queue), name="trading_engine"),
        asyncio.create_task(recon_engine.start(), name="recon_engine"),
        asyncio.create_task(outcome_tracker.start(), name="outcome_tracker")
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
        # Asegurar que enviamos notificación si no se hizo por señal
        if not shutdown_event.is_set():
            with suppress(Exception):
                alert_queue.put_nowait("⚠️ **Bot Inactivo**")
            
        for t in tasks: t.cancel()
        await outcome_tracker.stop()
        
        # Esperar a que las tareas terminen su cancelación
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        await event_logger.stop()
        logger.info("✅ Sistema PRO apagado correctamente")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        try:
            # Cancelar tareas pendientes en el loop para evitar "Task was destroyed but it is pending"
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                
            loop.run_until_complete(loop.shutdown_asyncgens())
        except:
            pass
        finally:
            loop.close()
