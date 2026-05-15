import asyncio
import time
import pytest
from loguru import logger
from infrastructure.binance_ws import BinanceWS

@pytest.mark.asyncio
async def test_throughput_benchmark():
    logger.info("Iniciando Benchmark de Throughput y Latencia...")
    
    # Cola con límite para probar backpressure
    market_queue = asyncio.Queue(maxsize=10)
    ws = BinanceWS(market_queue)
    
    # Simular recepción masiva de mensajes
    start_time = time.time()
    for i in range(100):
        fake_event = {
            "e": "24hrMiniTicker",
            "E": int(time.time() * 1000) - 10, # Simular 10ms de latencia de red
            "s": "BTCUSDT",
            "c": "50000.00"
        }
        await ws._process_message(json_serialize(fake_event))
        
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"Procesados {ws.msg_count} mensajes en {duration:.4f}s")
    logger.info(f"Mensajes descartados (Backpressure): {ws.discarded_count}")
    tps_real = ws.msg_count / duration if duration > 0 else 0
    logger.info(f"TPS Real: {tps_real:.2f}")
    
    if ws.discarded_count > 0:
        logger.success("✅ Backpressure funcionando: se descartaron mensajes excedentes.")

def json_serialize(data):
    import json
    return json.dumps(data)

if __name__ == "__main__":
    asyncio.run(test_throughput_benchmark())
