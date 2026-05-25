import asyncio
import random
import aiohttp
import re
from loguru import logger
from core.config import settings

class AICircuitBreaker:
    def __init__(self):
        raw_keys = settings.OPENROUTER_API_KEYS.split(",")
        self.api_keys = []
        for k in raw_keys:
            k = k.strip()
            if not k: continue
            if k != "mock_key" and not k.startswith("sk-or-v1-"):
                raise ValueError(f"API key inválida de OpenRouter: debe empezar con 'sk-or-v1-'. Encontrada: {k[:10]}...")
            self.api_keys.append(k)
            
        if not self.api_keys:
            raise ValueError("No se encontraron API keys configuradas.")
            
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
            logger.warning("No hay API keys de IA disponibles (todas en cooldown).")
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
        self.models = [m.strip() for m in settings.MODELOS.split(",")]

    async def test_connection(self) -> bool:
        """
        Realiza un healthcheck a la API de IA para asegurar que funciona (Ping ligero).
        """
        if settings.SIMULATION_MODE or settings.OPENROUTER_API_KEYS == "mock_key":
            return True
            
        try:
            key = self.circuit_breaker.get_active_key()
            logger.info("Realizando Healthcheck de IA...")
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json"
                }
                payload = {
                    "model": self.models[0],
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 1
                }
                async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=5) as resp:
                    if resp.status == 200:
                        logger.success("✅ IA Healthcheck OK")
                        return True
                    else:
                        logger.error(f"❌ IA Healthcheck falló. HTTP {resp.status}")
                        return False
        except Exception as e:
            logger.error(f"❌ IA Healthcheck falló con excepción: {e}")
            return False

    async def analyze_asset(self, symbol: str, context: dict) -> int:
        """
        Analiza un activo usando modelos de IA y devuelve un score (0-100).
        """
        if settings.SIMULATION_MODE:
            await asyncio.sleep(0.5)
            return random.randint(60, 90)

        try:
            key = self.circuit_breaker.get_active_key()
            model = random.choice(self.models)
            
            prompt = (
                f"Eres un experto analista de criptomonedas de alta frecuencia.\n"
                f"Evalúa este activo para una operación de 'Momentum' a corto plazo:\n"
                f"Símbolo: {symbol}\n"
                f"Cambio 24h: {context.get('change', 0)}%\n"
                f"Volumen 24h: {context.get('volume', 0)} USDT\n\n"
                f"Responde ÚNICAMENTE con un número entero del 0 al 100 que represente la "
                f"calificación de la oportunidad de compra (100 = compra agresiva inmediata, 0 = descartar).\n"
                f"No des explicaciones, solo el número."
            )
            
            logger.info(f"Analizando {symbol} con IA ({model})...")
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://localhost", # Requisito opcional de OpenRouter
                    "X-Title": "BotTrading" # Requisito opcional de OpenRouter
                }
                
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "You are a quantitative trading AI."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 10
                }
                
                async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        content = data['choices'][0]['message']['content'].strip()
                        
                        match = re.search(r'\d+', content)
                        if match:
                            score = int(match.group())
                            return min(max(score, 0), 100)
                        else:
                            logger.warning(f"Respuesta IA inválida para {symbol}: {content}")
                            return 0
                    elif resp.status == 429: # Rate limit
                        logger.warning(f"Rate limit de OpenRouter con key {key[:8]}...")
                        self.circuit_breaker.block_key(key, duration=60)
                        return 0
                    else:
                        error_text = await resp.text()
                        logger.error(f"Error OpenRouter ({resp.status}): {error_text}")
                        self.circuit_breaker.block_key(key, duration=120)
                        return 0
                        
        except asyncio.TimeoutError:
            logger.error(f"Timeout consultando IA para {symbol}")
            return 0
        except Exception as e:
            if "No AI API keys available" in str(e):
                return -1 # Señal explícita de agotamiento de keys
            logger.error(f"Error consultando IA: {e}")
            if 'key' in locals() and key:
                self.circuit_breaker.block_key(key)
            return 0
