import math
from collections import deque
from loguru import logger

class TA:
    """
    Indicadores técnicos optimizados y de baja latencia.
    """
    @staticmethod
    def calculate_ema(prices: list, period: int):
        if len(prices) < period:
            return None
        
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    @staticmethod
    def calculate_rsi(prices: list, period: int = 14):
        if len(prices) < period + 1:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            gains.append(max(diff, 0))
            losses.append(abs(min(diff, 0)))
            
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        if avg_loss == 0:
            return 100.0
            
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_atr(highs: list, lows: list, closes: list, period: int = 14):
        """
        Cálculo de Average True Range (ATR).
        """
        if len(closes) < period + 1:
            return None
        
        tr_list = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            tr_list.append(tr)
            
        atr = sum(tr_list[:period]) / period
        for tr in tr_list[period:]:
            atr = (atr * (period - 1) + tr) / period
        return atr

    @staticmethod
    def detect_volatility_regime(prices: list, atr: float):
        """
        Detecta el régimen de volatilidad actual.
        Retorna: 'LOW', 'NORMAL', 'EXPANSION', 'PANIC'
        """
        if not prices or not atr or len(prices) < 20:
            return "NORMAL"
        
        current_price = prices[-1]
        mean_price = sum(prices[-20:]) / 20
        std_dev = (sum((x - mean_price)**2 for x in prices[-20:]) / 20)**0.5
        
        # Bollinger Bandwidth as proxy for compression/expansion
        bandwidth = (std_dev * 4) / mean_price if mean_price > 0 else 0
        
        # ATR relative to price
        atr_pct = (atr / current_price) * 100 if current_price > 0 else 0

        if atr_pct > 3.0: # Umbral arbitrario para pánico
            return "PANIC"
        elif bandwidth < 0.01: # Compresión
            return "LOW"
        elif bandwidth > 0.05: # Expansión
            return "EXPANSION"
        else:
            return "NORMAL"

class PriceBuffer:
    """
    Buffer dinámico con soporte para Warmup y métricas de volatilidad.
    """
    def __init__(self, maxlen: int = 100):
        self.prices = deque(maxlen=maxlen)
        self.highs = deque(maxlen=maxlen)
        self.lows = deque(maxlen=maxlen)
        self.last_atr = None
        self._tick_count = 0

    def add(self, price: float, high: float = None, low: float = None):
        self.prices.append(price)
        self.highs.append(high or price)
        self.lows.append(low or price)
        self._tick_count += 1

    def get_indicators(self, update_atr: bool = False):
        indicators = {
            "ema_20": TA.calculate_ema(list(self.prices), 20),
            "rsi_14": TA.calculate_rsi(list(self.prices), 14)
        }
        
        # Optimización: Solo recalcular ATR si se solicita (ej. microbatch o candle close)
        if update_atr or self.last_atr is None:
            self.last_atr = TA.calculate_atr(list(self.highs), list(self.lows), list(self.prices), 14)
            
        indicators["atr_14"] = self.last_atr
        return indicators
