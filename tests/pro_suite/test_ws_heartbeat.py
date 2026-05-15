import asyncio
import time
import pytest
from loguru import logger
from infrastructure.binance_ws import BinanceWS
from core.state import system_state, HealthStatus

@pytest.mark.asyncio
async def test_watchdog_zombie_detection():
    logger.info("Iniciando prueba de Watchdog (detección de conexión zombie)...")
    
    queue = asyncio.Queue()
    ws = BinanceWS(queue)
    ws.is_running = True
    
    # Iniciar watchdog
    watchdog_task = asyncio.create_task(ws._watchdog())
    system_state.task_registry.register(watchdog_task, "Test_Watchdog")
    
    # Simular que recibimos un mensaje ahora
    ws._last_msg_time = time.time()
    
    logger.info("Esperando 5 segundos (salud debería ser HEALTHY)...")
    await asyncio.sleep(5)
    assert system_state.health == HealthStatus.HEALTHY
    
    logger.info("Simulando inactividad (dejando que pase el timeout de 10s)...")
    await asyncio.sleep(7) # 5 + 7 = 12s total
    
    if system_state.health == HealthStatus.RECOVERING:
        logger.success("✅ Watchdog detectó la conexión zombie y cambió estado a RECOVERING.")
    else:
        logger.error(f"❌ Watchdog falló. Estado actual: {system_state.health}")

    ws.is_running = False
    await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(test_watchdog_zombie_detection())
