import asyncio
import time
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
        self._symbol_info_cache = {}
        self._time_offset = 0
        
        # Rate Limiting (Token Bucket)
        # Binance permite 1200 weight por minuto por IP.
        self._rate_limit_lock = asyncio.Lock()
        self._tokens = 1200 
        self._last_refill = time.time()
        self._max_tokens = 1200

    async def _acquire_token(self, weight: int = 1):
        """Implementación de Token Bucket para respetar Rate Limits de Binance."""
        async with self._rate_limit_lock:
            now = time.time()
            passed = now - self._last_refill
            # Refill progresivo
            self._tokens = min(self._max_tokens, self._tokens + passed * (self._max_tokens / 60))
            self._last_refill = now
            
            if self._tokens < weight:
                wait_time = (weight - self._tokens) / (self._max_tokens / 60)
                logger.warning(f"Rate Limit Binance: Esperando {wait_time:.2f}s (Weight: {weight})")
                await asyncio.sleep(wait_time)
                self._tokens = weight 
                self._last_refill = time.time()
            
            self._tokens -= weight

    async def sync_time(self):
        """Sincroniza el offset de tiempo con el servidor de Binance."""
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            server_time = await loop.run_in_executor(None, self.client.time)
            local_time = int(time.time() * 1000)
            self._time_offset = server_time['serverTime'] - local_time
            logger.info(f"Sincronización de tiempo completa. Offset: {self._time_offset}ms")
        except Exception as e:
            logger.error(f"Error sincronizando tiempo: {e}")

    async def get_balance(self, asset: str = "USDT"):
        await self._acquire_token(weight=10) # /account es pesado
        loop = asyncio.get_event_loop()
        try:
            account = await loop.run_in_executor(None, lambda: self.client.account(recvWindow=10000))
            for balance in account['balances']:
                if balance['asset'] == asset:
                    return float(balance['free'])
            return 0.0
        except Exception as e:
            logger.error(f"Error obteniendo balance: {e}")
            return 0.0

    async def get_account(self):
        await self._acquire_token(weight=10)
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self.client.account(recvWindow=10000))
        except Exception as e:
            logger.error(f"Error obteniendo cuenta: {e}")
            return None

    async def get_ticker(self, symbol: str):
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self.client.ticker_price(symbol=symbol))
        except Exception as e:
            logger.error(f"Error obteniendo ticker para {symbol}: {e}")
            return None

    async def get_24hr_tickers(self):
        # /ticker/24hr sin símbolo es muy pesado (weight 40 o más dependiendo del endpoint)
        await self._acquire_token(weight=40) 
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self.client.ticker_24hr)
        except Exception as e:
            logger.error(f"Error obteniendo tickers: {e}")
            return []

    async def get_symbol_info(self, symbol: str):
        if symbol in self._symbol_info_cache:
            return self._symbol_info_cache[symbol]
            
        await self._acquire_token(weight=10)
        loop = asyncio.get_event_loop()
        try:
            exchange_info = await loop.run_in_executor(None, lambda: self.client.exchange_info(symbol=symbol))
            if exchange_info and 'symbols' in exchange_info:
                info = exchange_info['symbols'][0]
                self._symbol_info_cache[symbol] = info
                return info
            return None
        except Exception as e:
            logger.error(f"Error obteniendo info de símbolo {symbol}: {e}")
            return None

    async def get_klines(self, symbol: str, interval: str, limit: int = 100):
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self.client.klines(symbol=symbol, interval=interval, limit=limit))
        except Exception as e:
            logger.error(f"Error obteniendo klines para {symbol}: {e}")
            return []

    async def execute_sniper_buy(self, symbol: str, amount_usd: float, current_ask: float, slippage_tolerance: float = 0.001, client_order_id: str = None):
        """Ejecuta una orden de compra LIMIT IOC."""
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        import math
        try:
            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info: return None

            tick_size = 0.0001
            step_size = 0.01
            for f in symbol_info['filters']:
                if f['filterType'] == 'PRICE_FILTER':
                    tick_size = float(f['tickSize'])
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])

            max_price = current_ask * (1 + slippage_tolerance)
            max_price = math.floor(max_price / tick_size) * tick_size
            
            quantity = amount_usd / current_ask
            quantity = math.floor(quantity / step_size) * step_size

            params = {
                "symbol": symbol,
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "IOC",
                "quantity": float(f"{quantity:.8f}"),
                "price": float(f"{max_price:.8f}"),
                "recvWindow": 10000,
                "timestamp": int(time.time() * 1000 + self._time_offset)
            }
            if client_order_id:
                params['newClientOrderId'] = client_order_id

            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Sniper BUY ejecutada ({symbol}): {quantity} @ {max_price}")
            return order
        except Exception as e:
            logger.error(f"Error en sniper buy ({symbol}): {e}")
            return None

    async def execute_market_buy(self, symbol: str, quantity: float, client_order_id: str = None):
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            params = {
                'symbol': symbol,
                'side': 'BUY',
                'type': 'MARKET',
                'quantity': quantity,
                'recvWindow': 10000,
                'timestamp': int(time.time() * 1000 + self._time_offset)
            }
            if client_order_id:
                params['newClientOrderId'] = client_order_id
                
            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Compra MARKET (ID: {client_order_id})")
            return order
        except Exception as e:
            logger.error(f"Error en compra MARKET ({symbol}): {e}")
            return None

    async def execute_limit_ioc_sell(self, symbol: str, price: float, quantity: float = None, client_order_id: str = None, slippage_tolerance: float = 0.002):
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            base_asset = symbol.replace("USDT", "")
            balance = await self.get_balance(base_asset)
            if balance <= 0: return {"status": "INSUFFICIENT_BALANCE", "qty": 0}

            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info: return None

            step_size = 1.0
            min_qty = 0.0
            tick_size = 0.0001
            for f in symbol_info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    min_qty = float(f['minQty'])
                if f['filterType'] == 'PRICE_FILTER':
                    tick_size = float(f['tickSize'])

            qty_to_sell = balance if quantity is None else min(quantity, balance)
            import math
            qty_to_sell = math.floor(qty_to_sell / step_size) * step_size

            if qty_to_sell < min_qty:
                return {"status": "INSUFFICIENT_BALANCE", "qty": qty_to_sell}

            min_price = price * (1 - slippage_tolerance)
            min_price = math.floor(min_price / tick_size) * tick_size

            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'LIMIT',
                'timeInForce': 'IOC',
                'quantity': float(f"{qty_to_sell:.8f}"),
                'price': float(f"{min_price:.8f}"),
                'recvWindow': 10000,
                'timestamp': int(time.time() * 1000 + self._time_offset)
            }
            if client_order_id:
                params['newClientOrderId'] = client_order_id

            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Venta LIMIT IOC ({symbol})")
            return order
        except Exception as e:
            logger.error(f"Error en venta LIMIT IOC ({symbol}): {e}")
            return None

    async def execute_market_sell(self, symbol: str, quantity: float = None, client_order_id: str = None):
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            base_asset = symbol.replace("USDT", "")
            balance = await self.get_balance(base_asset)
            if balance <= 0: return {"status": "INSUFFICIENT_BALANCE", "qty": 0}

            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info: return None

            step_size = 1.0
            min_qty = 0.0
            for f in symbol_info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    min_qty = float(f['minQty'])

            qty_to_sell = balance if quantity is None else min(quantity, balance)
            import math
            qty_to_sell = math.floor(qty_to_sell / step_size) * step_size

            if qty_to_sell < min_qty:
                return {"status": "INSUFFICIENT_BALANCE", "qty": qty_to_sell}

            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': float(f"{qty_to_sell:.8f}"),
                'recvWindow': 10000,
                'timestamp': int(time.time() * 1000 + self._time_offset)
            }
            if client_order_id:
                params['newClientOrderId'] = client_order_id

            order = await loop.run_in_executor(None, lambda: self.client.new_order(**params))
            logger.success(f"Venta MARKET REAL ({symbol})")
            return order
        except Exception as e:
            logger.error(f"Error en venta MARKET ({symbol}): {e}")
            return None

    async def get_order_status(self, symbol: str, client_order_id: str = None, exchange_order_id: str = None):
        await self._acquire_token(weight=1)
        loop = asyncio.get_event_loop()
        try:
            params = {
                'symbol': symbol,
                'recvWindow': 10000,
                'timestamp': int(time.time() * 1000 + self._time_offset)
            }
            if client_order_id:
                params['origClientOrderId'] = client_order_id
            elif exchange_order_id:
                params['orderId'] = exchange_order_id
            else:
                return None
                
            return await loop.run_in_executor(None, lambda: self.client.get_order(**params))
        except Exception as e:
            if "Order does not exist" in str(e) or "-2013" in str(e):
                logger.debug(f"Orden no encontrada: {client_order_id or exchange_order_id}")
            else:
                logger.error(f"Error consultando orden: {e}")
            return None
