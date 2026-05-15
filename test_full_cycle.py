import asyncio
from trading.executor import TradeExecutor
from trading.slots import SlotManager
from infrastructure.database import init_db
from loguru import logger

async def test_full_cycle():
    logger.info("Iniciando simulación de ciclo completo...")
    await init_db()
    
    # Asegurar slots
    await SlotManager.initialize_slots(10, 100.0)
    
    alert_queue = asyncio.Queue()
    executor = TradeExecutor(alert_queue)
    
    # Símbolo para probar (usaremos uno barato por si acaso, o simplemente veremos el error)
    symbol = "BTCUSDT"
    price = 60000.0 # Precio ficticio, pero try_buy lo usa para calcular qty
    score = 85
    
    logger.info(f"Simulando gatillo para {symbol}")
    await executor.try_buy(symbol, price, score)
    
    # Ver si hay algo en la cola de alertas
    while not alert_queue.empty():
        alert = await alert_queue.get()
        logger.success(f"Alerta recibida: {alert}")

if __name__ == "__main__":
    asyncio.run(test_full_cycle())
