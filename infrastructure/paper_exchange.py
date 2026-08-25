import asyncio
import time
import uuid
from loguru import logger
from infrastructure.exchange_interface import ExchangeInterface

class PaperExchange(ExchangeInterface):
    """
    Simulador de exchange (Paper Trading) para pruebas sin riesgo.
    """
    def __init__(self):
        self.last_order_id = 1000
        self.orders = {} # Almacén de órdenes simuladas
        self.balances = {"USDT": 1000.0}  # BUG-6 FIX: Tracking de balances simulados

    async def get_balance(self, asset: str = "USDT") -> float:
        """Retorna el balance simulado de un asset."""
        return self.balances.get(asset, 0.0)

    async def get_symbol_info(self, symbol: str):
        # Mock de info básica
        return {
            'symbol': symbol,
            'filters': [
                {'filterType': 'LOT_SIZE', 'stepSize': '0.00001'},
                {'filterType': 'PRICE_FILTER', 'tickSize': '0.01'},
                {'filterType': 'NOTIONAL', 'minNotional': '5.0'}
            ]
        }

    async def execute_sniper_buy(self, symbol: str, amount_usd: float, current_ask: float, slippage_tolerance: float = 0.001, client_order_id: str = None):
        self.last_order_id += 1
        oid = str(self.last_order_id)
        cid = client_order_id or str(uuid.uuid4())
        qty = amount_usd / current_ask
        
        order = {
            'symbol': symbol,
            'orderId': oid,
            'clientOrderId': cid,
            'status': 'FILLED',
            'side': 'BUY',
            'type': 'LIMIT',
            'timeInForce': 'IOC',
            'price': str(current_ask),
            'executedQty': str(qty),
            'cummulativeQuoteQty': str(amount_usd),
            'transactTime': int(time.time() * 1000)
        }
        self.orders[cid] = order
        logger.info(f"[PAPER] Sniper Buy ejecutada: {symbol} Qty: {qty} @ {current_ask}")
        return order

    async def execute_market_buy(self, symbol: str, quantity: float, client_order_id: str = None):
        self.last_order_id += 1
        oid = str(self.last_order_id)
        cid = client_order_id or str(uuid.uuid4())
        
        order = {
            'symbol': symbol,
            'orderId': oid,
            'clientOrderId': cid,
            'status': 'FILLED',
            'side': 'BUY',
            'price': '50000.0', # Mock
            'executedQty': str(quantity),
            'cummulativeQuoteQty': str(quantity * 50000.0),
            'transactTime': int(time.time() * 1000)
        }
        self.orders[cid] = order
        logger.info(f"[PAPER] Compra ejecutada: {symbol} Qty: {quantity}")
        return order

    async def execute_limit_ioc_sell(self, symbol: str, price: float, quantity: float = None, client_order_id: str = None):
        self.last_order_id += 1
        oid = str(self.last_order_id)
        cid = client_order_id or str(uuid.uuid4())
        qty = quantity if quantity else 1.0 # Default para mock
        
        order = {
            'symbol': symbol,
            'orderId': oid,
            'clientOrderId': cid,
            'status': 'FILLED',
            'side': 'SELL',
            'type': 'LIMIT',
            'timeInForce': 'IOC',
            'price': str(price),
            'executedQty': str(qty),
            'cummulativeQuoteQty': str(qty * price),
            'transactTime': int(time.time() * 1000)
        }
        self.orders[cid] = order
        logger.info(f"[PAPER] Venta LIMIT IOC ejecutada: {symbol} Qty: {qty} @ {price}")
        return order

    async def execute_market_sell(self, symbol: str, quantity: float, client_order_id: str = None):
        self.last_order_id += 1
        oid = str(self.last_order_id)
        cid = client_order_id or str(uuid.uuid4())
        
        order = {
            'symbol': symbol,
            'orderId': oid,
            'clientOrderId': cid,
            'status': 'FILLED',
            'side': 'SELL',
            'price': '50010.0', # Mock
            'executedQty': str(quantity),
            'cummulativeQuoteQty': str(quantity * 50010.0),
            'transactTime': int(time.time() * 1000)
        }
        self.orders[cid] = order
        logger.info(f"[PAPER] Venta ejecutada: {symbol} Qty: {quantity}")
        return order

    async def get_order_status(self, symbol: str, client_order_id: str = None, exchange_order_id: str = None):
        if client_order_id in self.orders:
            return self.orders[client_order_id]
        return None

    async def get_ticker(self, symbol: str):
        return {'symbol': symbol, 'price': '50000.00'}

    async def get_klines(self, symbol: str, interval: str, limit: int = 100):
        # Generar klines falsos para warmup
        now = int(time.time() * 1000)
        interval_ms = 60000 # 1m
        klines = []
        for i in range(limit):
            ts = now - (limit - i) * interval_ms
            klines.append([ts, "50000", "50100", "49900", "50050", "100", ts + 59999, "5000", 10, "50", "50", "0"])
        return klines
