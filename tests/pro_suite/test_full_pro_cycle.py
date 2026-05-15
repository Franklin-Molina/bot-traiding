import asyncio
import pytest
from loguru import logger
from trading.recovery import RecoveryEngine
from trading.indicators import PriceBuffer
from infrastructure.paper_exchange import PaperExchange
from core.state import system_state, HealthStatus

@pytest.mark.asyncio
async def test_recovery_and_warmup():
    logger.info("Iniciando prueba de Recovery Engine y Warmup...")
    
    recovery = RecoveryEngine()
    # Usar PaperExchange inyectado (el recovery engine usa BinanceRest por defecto, 
    # pero aquí podemos mockearlo o confiar en que PaperExchange funciona si lo cambiamos)
    # Para el test, vamos a parchear el binance client del recovery
    recovery.binance = PaperExchange()
    
    buffer = PriceBuffer(maxlen=100)
    symbol = "BTCUSDT"
    
    assert not recovery.is_in_warmup(symbol)
    
    # Simular recuperación
    await recovery.recover_symbol(symbol, buffer)
    
    # Durante la recuperación el estado debería haber pasado por RECOVERING
    # Al final debería ser HEALTHY si no hay más símbolos en warmup
    assert system_state.health == HealthStatus.HEALTHY
    assert len(buffer.prices) == 100
    
    indicators = buffer.get_indicators()
    logger.info(f"Indicadores post-recuperación: {indicators}")
    
    if indicators["rsi_14"] is not None and indicators["atr_14"] is not None:
        logger.success("✅ Recuperación de estado exitosa y Warmup completado.")
    else:
        logger.error("❌ Los indicadores no se calcularon correctamente tras la recuperación.")

if __name__ == "__main__":
    asyncio.run(test_recovery_and_warmup())
