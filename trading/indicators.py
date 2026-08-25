import math
import time
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
        """
        Cálculo de RSI con suavizado de Wilder.
        """
        if len(prices) < period + 1:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            gains.append(max(diff, 0))
            losses.append(abs(min(diff, 0)))
            
        # Primer promedio (Simple)
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        # Promedios subsiguientes (Wilder)
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            
        if avg_loss == 0:
            return 100.0
            
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_atr(highs: list, lows: list, closes: list, period: int = 14):
        """
        Cálculo de Average True Range (ATR) con suavizado de Wilder.
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
        for i in range(period, len(tr_list)):
            atr = (atr * (period - 1) + tr_list[i]) / period
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
        # Usar los últimos 20 precios para estadísticas locales
        subset = list(prices)[-20:]
        mean_price = sum(subset) / 20
        std_dev = (sum((x - mean_price)**2 for x in subset) / 20)**0.5
        
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
    Buffer dinámico con soporte para indicadores incrementales y ventanas temporales.
    """
    def __init__(self, maxlen: int = 100):
        self.prices = deque(maxlen=maxlen)
        self.timestamps = deque(maxlen=maxlen)
        self.highs = deque(maxlen=maxlen)
        self.lows = deque(maxlen=maxlen)
        self.volumes = deque(maxlen=maxlen)
        
        # Para Z-Score de anomalías (1 hora = 240 ventanas de 15s)
        self.momentums_15s = deque(maxlen=240)
        self.last_momentum_calc_ts = 0.0
        
        # Estados para indicadores incrementales
        self._ema_20 = None
        self._avg_gain_14 = None
        self._avg_loss_14 = None
        self._atr_14 = None
        self._last_price = None
        self._tick_count = 0

    def clear(self):
        self.prices.clear()
        self.timestamps.clear()
        self.highs.clear()
        self.lows.clear()
        self.volumes.clear()
        self.momentums_15s.clear()
        self._ema_20 = None
        self._avg_gain_14 = None
        self._avg_loss_14 = None
        self._atr_14 = None
        self._last_price = None
        self._tick_count = 0

    def add(self, price: float, high: float = None, low: float = None, timestamp: float = None, volume: float = 0.0):
        h = high or price
        l = low or price
        ts = timestamp or time.time()
        
        # 1. Actualización de EMA 20 (Incremental)
        if self._ema_20 is None:
            if len(self.prices) == 19: # Estamos a punto de tener 20
                temp_prices = list(self.prices) + [price]
                self._ema_20 = sum(temp_prices) / 20
        else:
            alpha = 2 / (20 + 1)
            self._ema_20 = (price - self._ema_20) * alpha + self._ema_20
            
        # 2. Actualización de RSI 14 (Incremental con Wilder)
        if self._last_price is not None:
            gain = max(price - self._last_price, 0)
            loss = abs(min(price - self._last_price, 0))
            
            if self._avg_gain_14 is None:
                # ARQ-4 FIX: Intentar inicializar si tenemos suficientes datos
                if len(self.prices) >= 14:
                    self._initialize_rsi_atr()
            else:
                self._avg_gain_14 = (self._avg_gain_14 * 13 + gain) / 14
                self._avg_loss_14 = (self._avg_loss_14 * 13 + loss) / 14
        
        # 3. Actualización de ATR 14 (Incremental)
        if self._last_price is not None:
            tr = max(h - l, abs(h - self._last_price), abs(l - self._last_price))
            if self._atr_14 is None:
                if len(self.prices) >= 14:
                    self._initialize_rsi_atr()
            else:
                self._atr_14 = (self._atr_14 * 13 + tr) / 14

        # Añadir a deques
        self.prices.append(price)
        self.timestamps.append(ts)
        self.highs.append(h)
        self.lows.append(l)
        self.volumes.append(volume)
        self._last_price = price
        self._tick_count += 1
        
        # Recalcular bases si aún no están inicializadas (Warmup)
        if self._ema_20 is None and len(self.prices) >= 20:
             self._ema_20 = TA.calculate_ema(list(self.prices), 20)
        
        # ARQ-4 FIX: Inicializar siempre que tengamos datos suficientes (no solo en tick_count==15)
        if self._avg_gain_14 is None and len(self.prices) >= 15:
            self._initialize_rsi_atr()

    def _initialize_rsi_atr(self):
        """Inicialización de promedios para RSI y ATR tras fase de warmup."""
        prices = list(self.prices)
        highs = list(self.highs)
        lows = list(self.lows)
        
        if len(prices) < 15: return
        
        # RSI Base
        gains = []
        losses = []
        for i in range(1, 15):
            diff = prices[i] - prices[i-1]
            gains.append(max(diff, 0))
            losses.append(abs(min(diff, 0)))
        self._avg_gain_14 = sum(gains) / 14
        self._avg_loss_14 = sum(losses) / 14
        
        # ATR Base
        tr_list = []
        for i in range(1, 15):
            tr = max(highs[i] - lows[i], abs(highs[i] - prices[i-1]), abs(lows[i] - prices[i-1]))
            tr_list.append(tr)
        self._atr_14 = sum(tr_list) / 14

    def get_price_ago(self, seconds: float):
        """
        Retorna el precio aproximado de hace N segundos basado en timestamps.
        Si no hay suficientes datos para cubrir la ventana completa, retorna None.
        """
        if not self.prices or not self.timestamps or len(self.prices) < 2:
            return None
        
        now = self.timestamps[-1]
        target_ts = now - seconds
        
        # Si el primer dato es más reciente que el target, no tenemos ventana suficiente
        if self.timestamps[0] > target_ts:
            return None

        # Búsqueda reversa
        for i in range(len(self.timestamps) - 1, -1, -1):
            if self.timestamps[i] <= target_ts:
                return self.prices[i]
        
        return None

    def get_local_range(self, seconds: float):
        """
        Calcula la expansión de precio (High-Low) en una ventana de tiempo.
        Retorna (range_pct, current_price)
        """
        if not self.prices or len(self.prices) < 2:
            return 0.0, 0.0
            
        now = self.timestamps[-1]
        target_ts = now - seconds
        
        subset = []
        for i in range(len(self.timestamps) - 1, -1, -1):
            if self.timestamps[i] >= target_ts:
                subset.append(self.prices[i])
            else:
                break
        
        if not subset:
            return 0.0, self.prices[-1]
            
        local_max = max(subset)
        local_min = min(subset)
        current = self.prices[-1]
        
        range_pct = (local_max - local_min) / local_min if local_min > 0 else 0
        return range_pct, current

    def get_relative_volume(self, seconds: float, baseline_sec: float = 3600.0) -> float:
        """
        Calcula el volumen relativo en la ventana de tiempo comparado con el promedio del buffer histórico.
        """
        if not self.volumes or len(self.volumes) < 10:
            return 1.0
            
        now = self.timestamps[-1]
        target_ts = now - seconds
        
        window_vol = 0.0
        window_ticks = 0
        for i in range(len(self.timestamps) - 1, -1, -1):
            if self.timestamps[i] >= target_ts:
                window_vol += self.volumes[i]
                window_ticks += 1
            else:
                break
                
        if window_ticks == 0:
            return 1.0
            
        total_vol = sum(self.volumes)
        total_ticks = len(self.volumes)
        
        if total_ticks == 0 or total_vol == 0:
            return 1.0
            
        avg_vol_per_tick = total_vol / total_ticks
        window_avg_vol_per_tick = window_vol / window_ticks
        
        return window_avg_vol_per_tick / avg_vol_per_tick

    def get_momentum_zscore(self, current_momentum: float, current_ts: float) -> float:
        """
        Calcula el Z-Score del momentum actual comparado con el histórico reciente (última hora).
        Retorna None si no hay suficientes datos para una desviación estándar válida.
        """
        # Añadir al histórico solo cada 15 segundos para evitar solapamiento excesivo
        if current_ts - self.last_momentum_calc_ts >= 15.0:
            self.momentums_15s.append(current_momentum)
            self.last_momentum_calc_ts = current_ts

        # Necesitamos al menos 10 muestras (2.5 minutos) para que el Z-Score tenga algo de sentido
        if len(self.momentums_15s) < 10:
            return None
            
        mean_mom = sum(self.momentums_15s) / len(self.momentums_15s)
        variance = sum((x - mean_mom) ** 2 for x in self.momentums_15s) / len(self.momentums_15s)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            return 0.0
            
        z_score = (current_momentum - mean_mom) / std_dev
        return z_score

    def get_indicators(self, update_atr: bool = False):
        """
        Retorna indicadores precalculados incrementalmente.
        FEAT-5: Añadidos bollinger_pos, tick_rate, atr_relative.
        """
        if self._ema_20 is None or self._avg_gain_14 is None:
            base = {
                "ema_20": TA.calculate_ema(list(self.prices), 20),
                "rsi_14": TA.calculate_rsi(list(self.prices), 14),
                "atr_14": TA.calculate_atr(list(self.highs), list(self.lows), list(self.prices), 14)
            }
            base.update(self._compute_advanced_features())
            return base
        
        rsi = 50.0
        if self._avg_loss_14 > 0:
            rs = self._avg_gain_14 / self._avg_loss_14
            rsi = 100 - (100 / (1 + rs))
        
        result = {
            "ema_20": self._ema_20,
            "rsi_14": rsi,
            "atr_14": self._atr_14
        }
        result.update(self._compute_advanced_features())
        return result

    def _compute_advanced_features(self) -> dict:
        """FEAT-5: Features avanzadas para XGBoost."""
        features = {
            "bollinger_pos": 0.5,
            "tick_rate": 0.0,
            "atr_relative": 0.0,
            "hour_sin": 0.0,
            "hour_cos": 1.0
        }
        
        # Bollinger Band Position (0=lower, 0.5=middle, 1=upper)
        if len(self.prices) >= 20 and self._ema_20:
            import math
            prices_list = list(self.prices)
            last_20 = prices_list[-20:]
            std = (sum((p - self._ema_20) ** 2 for p in last_20) / 20) ** 0.5
            if std > 0:
                upper = self._ema_20 + (2 * std)
                lower = self._ema_20 - (2 * std)
                band_width = upper - lower
                if band_width > 0:
                    features["bollinger_pos"] = max(0.0, min(1.0, (prices_list[-1] - lower) / band_width))
        
        # Tick Rate (ticks per second en los últimos 60s)
        if len(self.timestamps) >= 2:
            now = self.timestamps[-1]
            cutoff = now - 60.0
            recent_ticks = sum(1 for t in self.timestamps if t >= cutoff)
            features["tick_rate"] = recent_ticks / 60.0
        
        # ATR Relative
        if self._atr_14 and len(self.prices) > 0 and self.prices[-1] > 0:
            features["atr_relative"] = self._atr_14 / self.prices[-1]
        
        # Hour of day (cyclical encoding UTC)
        if self.timestamps:
            import math
            from datetime import datetime, UTC
            dt = datetime.fromtimestamp(self.timestamps[-1], tz=UTC)
            hour_frac = dt.hour + dt.minute / 60.0
            features["hour_sin"] = math.sin(2 * math.pi * hour_frac / 24)
            features["hour_cos"] = math.cos(2 * math.pi * hour_frac / 24)
        
        return features
