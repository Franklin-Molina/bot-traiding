from abc import ABC, abstractmethod

class ExchangeInterface(ABC):
    """
    Interfaz abstracta para desacoplar la estrategia de la ejecución.
    """
    @abstractmethod
    async def get_symbol_info(self, symbol: str):
        pass

    @abstractmethod
    async def execute_market_buy(self, symbol: str, quantity: float, client_order_id: str = None):
        pass

    @abstractmethod
    async def execute_sniper_buy(self, symbol: str, amount_usd: float, current_ask: float, slippage_tolerance: float = 0.001, client_order_id: str = None):
        pass

    @abstractmethod
    async def execute_market_sell(self, symbol: str, quantity: float, client_order_id: str = None):
        pass
    
    @abstractmethod
    async def get_order_status(self, symbol: str, client_order_id: str = None, exchange_order_id: str = None):
        pass

    @abstractmethod
    async def get_ticker(self, symbol: str):
        pass

    @abstractmethod
    async def get_klines(self, symbol: str, interval: str, limit: int = 100):
        pass
