import asyncio
from binance.spot import Spot
from loguru import logger
from core.config import settings
from infrastructure.exchange_interface import ExchangeInterface

class BinanceRest(ExchangeInterface):
    def __init__(self, api_key: str = None, api_secret: str = None):
        self.client = Spot(
            api_key=api_key or settings.BINANCE_API_KEY,
            api_secret=api_secret or settings.BINANCE_API_SECRET,
            base_url="https://api.binance.com"
        )

    async def get_balance(self, asset: str = "USDT"):
        loop = asyncio.get_event_loop()
        try:
            account = await loop.run_in_executor(None, self.client.account)
            for balance in account['balances']:
                if balance['asset'] == asset:
                    return float(balance['free'])
            return 0.0
        except Exception as e:
            logger.error(f"Error obteniendo balance: {e}")
            return 0.0

    async def get_account(self):
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self.client.account)
        except Exception as e:
            logger.error(f"Error obteniendo cuenta: {e}")
            return None

    async def get_ticker(self, symbol: str):
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self.client.ticker_price(symbol=symbol))
        except Exception as e:
            logger.error(f"Error obteniendo ticker para {symbol}: {e}")
            return None

    async def get_24hr_tickers(self):
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self.client.ticker_24hr)
        except Exception as e:
            logger.error(f"Error obteniendo tickers: {e}")
            return []

    async def get_symbol_info(self, symbol: str):
        loop = asyncio.get_event_loop()
        try:
            exchange_info = await loop.run_in_executor(None, lambda: self.client.exchange_info(symbol=symbol))
            if exchange_info and 'symbols' in exchange_info:
                return exchange_info['symbols'][0]
            return None
        except Exception as e:
            logger.error(f"Error obteniendo info de símbolo {symbol}: {e}")
            return None

    async def get_klines(self, symbol: str, interval: str, limit: int = 100):
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self.client.klines(symbol=symbol, interval=interval, limit=limit))
        except Exception as e:
            logger.error(f"Error obteniendo klines para {symbol}: {e}")
            return []

    async def execute_market_buy(self, symbol: str, quantity: float, client_order_id: str = None):
        """Ejecuta compra a mercado con idempotencia (client_order_id)."""
        loop = asyncio.get_event_loop()
        try:
            params = {
                'symbol': symbol,
                'side': 'BUY',
                'type': 'MARKET',
                'quantity': quantity
            }
            if client_order_id:
                params['newClientOrderId'] = client_order_id
                
            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Compra ejecutada (ID: {client_order_id}): {order}")
            return order
        except Exception as e:
            logger.error(f"Error en compra ({symbol}): {e}")
            return None

    async def execute_market_sell(self, symbol: str, quantity: float, client_order_id: str = None):
        """Ejecuta venta a mercado con idempotencia."""
        loop = asyncio.get_event_loop()
        try:
            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': quantity
            }
            if client_order_id:
                params['newClientOrderId'] = client_order_id
                
            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Venta ejecutada (ID: {client_order_id}): {order}")
            return order
        except Exception as e:
            logger.error(f"Error en venta ({symbol}): {e}")
            return None

    async def get_order_status(self, symbol: str, client_order_id: str = None, exchange_order_id: str = None):
        """Consulta el estado de una orden por cualquiera de sus IDs."""
        loop = asyncio.get_event_loop()
        try:
            params = {'symbol': symbol}
            if client_order_id:
                params['origClientOrderId'] = client_order_id
            elif exchange_order_id:
                params['orderId'] = exchange_order_id
            else:
                return None
                
            return await loop.run_in_executor(None, lambda: self.client.get_order(**params))
        except Exception as e:
            logger.error(f"Error consultando orden {client_order_id or exchange_order_id}: {e}")
            return None
