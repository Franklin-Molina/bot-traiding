import asyncio
import json
import time
import pytest
from infrastructure.binance_ws import BinanceWS
from infrastructure.event_logger import event_logger
from loguru import logger

@pytest.mark.asyncio
async def test_market_queue_backpressure():
    """
    Satura la market_queue para verificar que el Load Shedding
    y el descarte de mensajes funcionan correctamente.
    """
    # Cola pequeña para forzar backpressure rápido
    market_queue = asyncio.Queue(maxsize=10)
    ws = BinanceWS(market_queue)
    ws.is_running = True
    
    # Simular 100 mensajes. Los primeros 8 (80% de 10) pasan normal.
    # Del 9 al 10 se consideran Load Shedding si no son críticos.
    # A partir de 10 se activa descarte extremo.
    
    for i in range(100):
        # Mensaje NO crítico
        tick = [{"s": "FAKE", "c": "100.0", "o": "100.0", "v": "10", "E": int(time.time() * 1000)}]
        await ws._process_message(json.dumps(tick))
    
    logger.info(f"Test Backpressure | Procesados: {ws.msg_count} | Descartados: {ws.discarded_count}")
    
    # Deben haberse descartado la gran mayoría
    assert ws.discarded_count > 80
    # La cola debe estar llena o casi llena
    assert market_queue.qsize() <= 10
    
    # Verificar que un mensaje CRÍTICO pase a pesar del Load Shedding (si hay espacio)
    # Primero vaciamos un poco
    for _ in range(5): market_queue.get_nowait()
    
    critical_tick = [{"s": "VOLATILE", "c": "150.0", "o": "100.0", "v": "1000", "E": int(time.time() * 1000)}]
    await ws._process_message(json.dumps(critical_tick))
    
    # Al haber espacio (5 slots libres), el crítico debe entrar
    # Nota: la cola se llenó hasta 9 debido al Load Shedding (>80%), 9-5+1 = 5
    assert market_queue.qsize() == 5

@pytest.mark.asyncio
async def test_event_logger_batching_under_load(db_session):
    """
    Prueba que el EventLogger maneje una carga masiva de eventos
    usando batching sin perder integridad.
    """
    from infrastructure.event_logger import EventLogger
    from models.trading import EventLog
    from sqlalchemy import select, func
    
    # Instancia local para el test para controlar su ciclo de vida
    test_logger = EventLogger(queue_size=1000)
    await test_logger.start()
    
    num_events = 200
    for i in range(num_events):
        await test_logger.log_event("STRESS_TEST", "ALL", {"idx": i})
        
    # Dar tiempo al worker para procesar los batches (batch_size=50)
    # Debería hacer ~4 flushes
    await asyncio.sleep(2)
    await test_logger.stop()
    
    # Verificar en DB
    query = select(func.count()).select_from(EventLog).where(EventLog.event_type == "STRESS_TEST")
    result = await db_session.execute(query)
    count = result.scalar()
    
    logger.info(f"EventLogger Stress | Eventos enviados: {num_events} | En DB: {count}")
    assert count == num_events
