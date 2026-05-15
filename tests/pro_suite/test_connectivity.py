import pytest
import os
from binance.spot import Spot
from core.config import settings
from loguru import logger

@pytest.mark.skipif(not settings.BINANCE_API_KEY or settings.BINANCE_API_KEY == "", 
                    reason="BINANCE_API_KEY no configurada en .env")
def test_binance_connectivity_read_only():
    """
    Verifica la conectividad real con las llaves de Binance.
    SOLO operaciones de lectura (GET) para garantizar que el transporte es operativo.
    """
    logger.info("Verificando conectividad real con Binance (Read-Only)...")
    
    client = Spot(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET
    )
    
    try:
        # 1. Probar conectividad básica (Ping)
        client.ping()
        logger.success("Ping a Binance: OK")
        
        # 2. Obtener estado de la cuenta
        account = client.account()
        assert account['canTrade'] is True
        logger.success(f"Cuenta Binance conectada: Trading habilitado. Comisiones: {account['makerCommission']} maker")
        
        # 3. Obtener info de un símbolo común
        info = client.exchange_info(symbol="BTCUSDT")
        assert 'symbols' in info and len(info['symbols']) > 0
        symbol_data = info['symbols'][0]
        assert symbol_data['symbol'] == "BTCUSDT"
        assert 'filters' in symbol_data
        logger.success("Obtención de Symbol Info (BTCUSDT): OK")
        
    except Exception as e:
        pytest.fail(f"Error de conectividad con Binance: {e}")

def test_config_integrity():
    """Valida que las reglas de trading críticas estén configuradas."""
    assert settings.MAX_ACTIVE_SLOTS > 0
    assert 0 < settings.RISK_PER_TRADE < 1.0
    assert settings.MIN_TRADE_USD >= 5.0
    logger.success("Configuración de reglas de trading validada.")
