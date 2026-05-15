import pytest
from decimal import Decimal
from loguru import logger

@pytest.mark.asyncio
async def test_calculate_quantity_btc(executor):
    """Prueba el cálculo de cantidad para un escenario tipo BTCUSDT."""
    symbol_info = {
        'symbol': 'BTCUSDT',
        'filters': [
            {'filterType': 'LOT_SIZE', 'stepSize': '0.00001', 'minQty': '0.00001'},
            {'filterType': 'NOTIONAL', 'minNotional': '5.0'}
        ]
    }
    
    # Capital 100 USD, Precio 50,000 -> Qty = 0.002
    qty = executor._calculate_quantity(100.0, 50000.0, symbol_info)
    assert qty == Decimal("0.002")
    assert executor._validate_notional(qty, 50000.0, symbol_info) == True

@pytest.mark.asyncio
async def test_calculate_quantity_alt(executor):
    """Prueba el redondeo (stepSize) para un escenario tipo ALTUSDT."""
    symbol_info = {
        'symbol': 'ALTUSDT',
        'filters': [
            {'filterType': 'LOT_SIZE', 'stepSize': '1.0', 'minQty': '1.0'},
            {'filterType': 'NOTIONAL', 'minNotional': '5.0'}
        ]
    }
    # Capital 10.5 USD, Precio 2.3. Qty raw = 4.565... -> Debe ser 4.0
    qty = executor._calculate_quantity(10.5, 2.3, symbol_info)
    assert qty == Decimal("4.0")

@pytest.mark.asyncio
async def test_min_notional_validation(executor):
    """Verifica que se rechacen órdenes por debajo del MIN_NOTIONAL."""
    symbol_info = {
        'symbol': 'LOWNOTIONAL',
        'filters': [
            {'filterType': 'LOT_SIZE', 'stepSize': '0.01', 'minQty': '0.01'},
            {'filterType': 'NOTIONAL', 'minNotional': '10.0'}
        ]
    }
    
    # Capital 5 USD, Precio 1 -> Qty 5. Notional 5 < 10.
    qty = executor._calculate_quantity(5.0, 1.0, symbol_info)
    assert executor._validate_notional(qty, 1.0, symbol_info) == False

@pytest.mark.asyncio
async def test_notional_filter_name_variation(executor):
    """Prueba variaciones en el nombre del filtro Notional (Binance API cambia a veces)."""
    symbol_info = {
        'symbol': 'VARNOTIONAL',
        'filters': [
            {'filterType': 'MIN_NOTIONAL', 'minNotional': '6.0'}
        ]
    }
    qty = Decimal("5.0")
    # 5.0 * 1.0 = 5.0 < 6.0
    assert executor._validate_notional(qty, 1.0, symbol_info) == False
