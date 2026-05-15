import asyncio
from infrastructure.binance_rest import BinanceRest
from core.config import settings
from loguru import logger

async def test_connection():
    logger.info("Iniciando prueba de conexión con Binance...")
    
    # Verificar carga de variables
    key = settings.BINANCE_API_KEY
    secret = settings.BINANCE_API_SECRET
    
    if not key or not secret:
        logger.warning("¡Atención! Las variables de Binance en settings están vacías.")
    else:
        logger.info(f"API Key cargada (longitud: {len(key)})")
        logger.info(f"API Secret cargado (longitud: {len(secret)})")

    # Inicializar el cliente REST
    binance = BinanceRest()
    
    # Intentar obtener la información de la cuenta
    logger.info("Solicitando información de la cuenta...")
    account_info = await binance.get_account()
    
    if account_info:
        logger.success("¡Conexión exitosa!")
        
        # Mostrar permisos
        permissions = account_info.get('permissions', [])
        logger.info(f"Permisos de la API Key: {permissions}")
        
        can_trade = account_info.get('canTrade', False)
        logger.info(f"¿Puede operar (Spot)?: {'SÍ' if can_trade else 'NO'}")
        
        # Mostrar balances con saldo
        logger.info("Balances con saldo disponible:")
        balances = account_info.get('balances', [])
        found_balance = False
        for balance in balances:
            free = float(balance.get('free', 0))
            locked = float(balance.get('locked', 0))
            if free > 0 or locked > 0:
                logger.info(f"  - {balance['asset']}: Disponible: {free} | Bloqueado: {locked}")
                found_balance = True
        
        if not found_balance:
            logger.info("No se encontraron activos con saldo.")
            
    else:
        logger.error("No se pudo obtener la información de la cuenta. Revisa tus API Keys en el archivo .env")

if __name__ == "__main__":
    asyncio.run(test_connection())
