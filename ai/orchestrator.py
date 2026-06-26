import asyncio
import random
import aiohttp
import re
import json
import time
from loguru import logger
from core.config import settings
from core.state import system_state

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
        logger.warning(f"Bloqueando API Key {key[:15]}... por {duration}s.")
        self.blocked_keys[key] = asyncio.get_event_loop().time() + duration

class AIOrchestrator:
    def __init__(self):
        self.circuit_breaker = AICircuitBreaker()
        self.models = [m.strip() for m in settings.MODELOS.split(",")]
        self.ai_cache = {}  # {symbol: {"score": int, "raw": dict, "exp": float}}

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
    async def analyze_asset(self, symbol: str, context: dict) -> tuple[int, dict]:
        """
        Analiza un activo usando modelos de IA y devuelve (ai_score, raw_json_dict).
        """
        now = time.time()
        
        # Validar caché y Señales de Invalidación (Flash Crash/Anomalías)
        if symbol in system_state.invalidated_symbols:
            logger.warning(f"💥 Purga de Caché IA ejecutada para {symbol} por orden del Motor Táctico.")
            self.ai_cache.pop(symbol, None)
            system_state.invalidated_symbols.discard(symbol)
            
        if symbol in self.ai_cache:
            cache_entry = self.ai_cache[symbol]
            if cache_entry["exp"] > now:
                logger.info(f"Usando análisis de IA en caché para {symbol}")
                return cache_entry["score"], cache_entry["raw"]
            else:
                del self.ai_cache[symbol]
                
        if settings.SIMULATION_MODE:
            await asyncio.sleep(0.5)
            mock_score = random.randint(60, 90)
            return mock_score, {"mock": True}

        try:
            key = self.circuit_breaker.get_active_key()
            model = random.choice(self.models)
            
            prompt = (
                f"Eres un experto cuantitativo de alta frecuencia.\n"
                f"Evalúa este activo:\nSímbolo: {symbol}\nCambio 24h: {context.get('change', 0)}%\nVolumen: {context.get('volume', 0)} USDT\n"
                f"Responde ÚNICAMENTE con un JSON válido usando esta estructura exacta y valores de 0.0 a 1.0:\n"
                f"{{\"momentum\": 0.0, \"risk\": 0.0, \"manipulation\": 0.0, \"news_strength\": 0.0, \"confidence\": 0.0}}\n"
                f"No incluyas markdown, código ni explicaciones."
            )
            
            logger.info(f"Analizando {symbol} con IA ({model})...")
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://localhost",
                    "X-Title": "BotTrading"
                }
                
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "You are a JSON-only quantitative AI."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2,
                    "max_tokens": 150
                }
                
                async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        content = data['choices'][0]['message']['content'].strip()
                        
                        try:
                            # Limpiar si la IA envió markdown block
                            if content.startswith("```json"):
                                content = content.split("```json")[1].split("```")[0].strip()
                            elif content.startswith("```"):
                                content = content.split("```")[1].split("```")[0].strip()
                                
                            result = json.loads(content)
                            
                            m = float(result.get("momentum", 0.0))
                            r = float(result.get("risk", 0.0))
                            man = float(result.get("manipulation", 0.0))
                            ns = float(result.get("news_strength", 0.0))
                            c = float(result.get("confidence", 0.0))
                            
                            ai_score_raw = (m * 0.35) + (ns * 0.25) + (c * 0.30) - (r * 0.05) - (man * 0.05)
                            ai_score = int(max(0.0, min(1.0, ai_score_raw)) * 100)
                            
                            # Guardar en caché
                            self.ai_cache[symbol] = {
                                "score": ai_score,
                                "raw": result,
                                "exp": now + (settings.AI_CACHE_TTL_MINUTES * 60)
                            }
                            
                            return ai_score, result
                        except json.JSONDecodeError:
                            logger.warning(f"Respuesta IA inválida para {symbol} (no es JSON): {content}")
                            return 0, {}
                    elif resp.status == 429: # Rate limit
                        logger.warning(f"Rate limit de OpenRouter con key {key[:8]}...")
                        self.circuit_breaker.block_key(key, duration=60)
                        return 50, {}
                    else:
                        error_text = await resp.text()
                        logger.error(f"Error OpenRouter ({resp.status}): {error_text}")
                        self.circuit_breaker.block_key(key, duration=120)
                        return 50, {}
                        
        except asyncio.TimeoutError:
            logger.error(f"Timeout consultando IA para {symbol}")
            return 50, {}
        except Exception as e:
            if "No AI API keys available" in str(e):
                return -1, {} # Señal explícita de agotamiento de keys
            logger.error(f"Error consultando IA: {e}")
            if 'key' in locals() and key:
                self.circuit_breaker.block_key(key)
            return 50, {}
