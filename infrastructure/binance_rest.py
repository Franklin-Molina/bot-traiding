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

    async def execute_limit_ioc_sell(self, symbol: str, price: float, quantity: float = None, client_order_id: str = None):
        """Venta LIMIT IOC (Immediate or Cancel) para evitar barrer el libro."""
        loop = asyncio.get_event_loop()
        try:
            base_asset = symbol.replace("USDT", "")
            balance = await self.get_balance(base_asset)
            if balance <= 0: return {"status": "INSUFFICIENT_BALANCE", "qty": 0}

            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info: return None

            step_size = None
            min_qty = None
            for f in symbol_info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    min_qty = float(f['minQty'])

            def adjust_qty(qty, step):
                import math
                return math.floor(qty / step) * step

            qty_to_sell = balance if quantity is None else min(quantity, balance)
            qty_to_sell = adjust_qty(qty_to_sell, step_size)

            if qty_to_sell < min_qty:
                return {"status": "INSUFFICIENT_BALANCE", "qty": qty_to_sell}

            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'LIMIT',
                'timeInForce': 'IOC', # Immediate or Cancel
                'quantity': float(f"{qty_to_sell:.8f}"),
                'price': float(f"{price:.8f}")
            }

            if client_order_id:
                params['newClientOrderId'] = client_order_id

            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Venta LIMIT IOC ejecutada ({symbol}): {qty_to_sell} @ {price}")
            return order
        except Exception as e:
            logger.error(f"Error en venta LIMIT IOC ({symbol}): {e}")
            return None

    async def execute_market_sell(self, symbol: str, quantity: float = None, client_order_id: str = None):
        """Venta a mercado segura (usa balance real y ajusta stepSize)."""
        loop = asyncio.get_event_loop()

        try:
            # 1. Obtener asset (ej: OSMO de OSMOUSDT)
            base_asset = symbol.replace("USDT", "")

            # 2. Obtener balance REAL
            balance = await self.get_balance(base_asset)

            if balance <= 0:
                logger.warning(f"No hay balance para vender {base_asset}")
                return {"status": "INSUFFICIENT_BALANCE", "qty": 0}

            # 3. Obtener info del símbolo (para stepSize)
            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                logger.error(f"No se pudo obtener info del símbolo {symbol}")
                return None

            step_size = None
            min_qty = None

            for f in symbol_info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    min_qty = float(f['minQty'])

            if step_size is None or min_qty is None:
                logger.error(f"No se encontró LOT_SIZE para {symbol}")
                return None

            # 4. Ajustar cantidad
            def adjust_qty(qty, step):
                import math
                return math.floor(qty / step) * step

            qty_to_sell = balance if quantity is None else min(quantity, balance)
            qty_to_sell = adjust_qty(qty_to_sell, step_size)

            if qty_to_sell < min_qty:
                logger.warning(f"Cantidad menor al mínimo permitido: {qty_to_sell} < {min_qty}")
                return {"status": "INSUFFICIENT_BALANCE", "qty": qty_to_sell}

            # 5. Crear orden
            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': float(f"{qty_to_sell:.8f}")
            }

            if client_order_id:
                params['newClientOrderId'] = client_order_id

            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))

            logger.success(f"Venta ejecutada REAL ({symbol}): {qty_to_sell}")
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
            # Si el error es -2013 (Order does not exist), lo manejamos más silenciosamente
            # ya que es esperado durante la reconciliación de órdenes que fallaron antes de enviarse.
            if "Order does not exist" in str(e) or "-2013" in str(e):
                logger.debug(f"Orden no encontrada en Binance: {client_order_id or exchange_order_id}")
            else:
                logger.error(f"Error consultando orden {client_order_id or exchange_order_id}: {e}")
            return None
