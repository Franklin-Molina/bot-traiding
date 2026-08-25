import asyncio
import time
from loguru import logger
from infrastructure.binance_rest import get_binance_rest
from core.state import system_state, HealthStatus

class RecoveryEngine:
    """
    Motor encargado de garantizar la continuidad de los datos y el estado de Warmup.
    """
    def __init__(self):
        self.binance = get_binance_rest()  # ARQ-1: Singleton compartido
        self._warmup_symbols = set()
        self.recovery_started = set()

    def is_in_warmup(self, symbol: str) -> bool:
        return symbol in self._warmup_symbols

    async def recover_symbol(self, symbol: str, buffer, interval: str = "1m", limit: int = 100):
        """
        Reconstruye el buffer de un símbolo validando continuidad temporal.
        """
        logger.info(f"🔄 Iniciando recuperación de estado para {symbol}...")
        self._warmup_symbols.add(symbol)
        system_state.set_health(HealthStatus.RECOVERING)
        
        try:
            # 1. Descargar datos históricos vía REST
            klines = await self.binance.get_klines(symbol, interval, limit=limit)
            
            if not klines:
                logger.error(f"No se pudieron obtener klines para {symbol}. Permaneciendo en Warmup.")
                return

            # 2. Validar continuidad temporal (timestamps secuenciales)
            # k[0] es open time
            last_ts = klines[0][0]
            interval_ms = self._interval_to_ms(interval)
            
            for i in range(1, len(klines)):
                current_ts = klines[i][0]
                if current_ts - last_ts > interval_ms:
                    logger.warning(f"Gap detectado en recuperación de {symbol} en {current_ts}")
                last_ts = current_ts

            # 3. Rellenar buffer
            buffer.clear()
            for k in klines:
                # k[4] = Close, k[5] = Volume, k[6] = Close Time (ms)
                buffer.add(
                    price=float(k[4]), 
                    timestamp=k[6] / 1000.0, 
                    volume=float(k[5])
                )
            
            logger.success(f"✅ Estado de {symbol} reconstruido. Saliendo de Warmup.")
                
        except Exception as e:
            logger.exception(f"Error crítico en recuperación de {symbol}: {e}")
            system_state.set_health(HealthStatus.DEGRADED)
        finally:
            self.recovery_started.discard(symbol)
            self._warmup_symbols.discard(symbol)
            if not self._warmup_symbols and system_state.health != HealthStatus.DEGRADED:
                system_state.set_health(HealthStatus.HEALTHY)

    def _interval_to_ms(self, interval: str) -> int:
        units = {'m': 60, 'h': 3600, 'd': 86400}
        value = int(interval[:-1])
        unit = interval[-1]
        return value * units[unit] * 1000
