import asyncio
import random
from loguru import logger
from core.config import settings

class AICircuitBreaker:
    def __init__(self):
        self.api_keys = settings.OPENROUTER_API_KEYS.split(",")
        self.blocked_keys = {} # key: expiration_time
        self.current_index = 0

    def get_active_key(self) -> str:
        """
        Obtiene una clave que no esté bloqueada.
        """
        now = asyncio.get_event_loop().time()
        
        # Limpiar llaves rehabilitadas
        self.blocked_keys = {k: exp for k, exp in self.blocked_keys.items() if exp > now}
        
        available_keys = [k for k in self.api_keys if k not in self.blocked_keys]
        
        if not available_keys:
            logger.critical("No hay API keys de IA disponibles (todas bloqueadas).")
            raise Exception("No AI API keys available")
        
        return random.choice(available_keys)

    def block_key(self, key: str, duration: int = 60):
        """
        Bloquea una clave por un tiempo determinado (cooldown).
        """
        logger.warning(f"Bloqueando API Key {key[:8]}... por {duration}s.")
        self.blocked_keys[key] = asyncio.get_event_loop().time() + duration

class AIOrchestrator:
    def __init__(self):
        self.circuit_breaker = AICircuitBreaker()

    async def analyze_asset(self, symbol: str, context: dict) -> int:
        """
        Analiza un activo usando modelos de IA y devuelve un score (0-100).
        """
        if settings.SIMULATION_MODE:
            await asyncio.sleep(0.5)
            return random.randint(60, 90)

        key = self.circuit_breaker.get_active_key()
        
        try:
            logger.info(f"Analizando {symbol} con IA...")
            # Aquí iría el fetch real a OpenRouter
            await asyncio.sleep(1) # Simulación
            
            score = random.randint(50, 95) # Simulación
            return score
            
        except Exception as e:
            logger.error(f"Error consultando IA: {e}")
            self.circuit_breaker.block_key(key)
            return 0
