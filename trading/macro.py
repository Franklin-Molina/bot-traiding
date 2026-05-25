import asyncio
from loguru import logger
from core.config import settings
from core.state import system_state
from infrastructure.binance_rest import BinanceRest
from ai.orchestrator import AIOrchestrator

class MacroEngine:
    def __init__(self, candidates_queue: asyncio.Queue, alert_queue: asyncio.Queue):
        self.candidates_queue = candidates_queue
        self.alert_queue = alert_queue
        self.binance = BinanceRest()
        self.ai = AIOrchestrator()
    async def run_cycle(self):
        """
        Ejecuta el ciclo macro: escaneo -> filtrado -> IA.
        """
        while True:
            if not system_state.is_running:
                logger.info("Motor Macro en espera (Sistema OFF o PÁNICO)...")
                await asyncio.sleep(60)
                continue

            # --- HEALTHCHECK IA (Evaluación periódica por ciclo) ---
            is_ai_healthy = await self.ai.test_connection()
            if not is_ai_healthy:
                if system_state.ai_enabled: # Solo alertar si acabamos de perderla
                    logger.critical("⚠️ FALLO CRÍTICO IA - Activando Fallback Técnico")
                    system_state.ai_enabled = False
                    try:
                        await self.alert_queue.put("⚠️ **Alerta del Sistema**\nFallo o Rate Limit con OpenRouter IA.\n\n🔄 **Cambiando a modo SÓLO TÉCNICO (Fallback).**")
                    except Exception as e:
                        pass
            else:
                if not system_state.ai_enabled:
                    logger.success("✅ IA Recuperada - Desactivando Fallback Técnico")
                system_state.ai_enabled = True

            logger.info(f"Iniciando ciclo Macro de escaneo... (IA Activada: {system_state.ai_enabled})")
            try:
                # 1. Obtener todos los tickers de 24h
                tickers = await self.binance.get_24hr_tickers()
                
                # 2. Filtrar candidatos iniciales
                candidates = []
                for t in tickers:
                    symbol = t['symbol']
                    # Filtros: Solo USDT, movimiento >= MIN_PERCENT, volumen > 2M (Subimos exigencia)
                    # Excluimos símbolos con nombres sospechosos (tokens apalancados, etc)
                    if symbol.endswith("USDT") and not any(x in symbol for x in ["UP", "DOWN", "BEAR", "BULL"]):
                        change = float(t['priceChangePercent'])
                        volume = float(t['quoteVolume'])
                        
                        # Filtro Estricto de Liquidez para APIs gratuitas: Mínimo 10M USDT y 2% de movimiento
                        if change >= 2.0 and volume > 10_000_000:
                            candidates.append({
                                "symbol": symbol,
                                "change": change,
                                "volume": volume
                            })
                
                logger.info(f"Filtro técnico inicial: {len(candidates)} activos encontrados.")

                # 3. Consultar IA para los mejores candidatos (Top 5 por combinación de volumen y cambio)
                # Priorizamos activos que tienen volumen Y movimiento
                candidates = sorted(candidates, key=lambda x: x['change'] * x['volume'], reverse=True)[:5]

                for c in candidates:
                    if system_state.ai_enabled:
                        score = await self.ai.analyze_asset(c['symbol'], context=c)
                        
                        if score == -1:
                            logger.warning(f"⚠️ Rate Limit alcanzado durante ciclo. Cambiando {c['symbol']} y el resto a Fallback Técnico.")
                            system_state.ai_enabled = False
                            score = 85
                            await self.candidates_queue.put({"symbol": c['symbol'], "score": score})
                            await asyncio.sleep(1.0)
                            continue

                        if score > 70:
                            logger.success(f"¡Joyita encontrada! {c['symbol']} con score IA: {score}")
                            await self.candidates_queue.put({"symbol": c['symbol'], "score": score})
                        
                        # 🚨 LA SOLUCIÓN ANTI-BANEO:
                        # Esperar 5 segundos entre cada llamada para respetar el tier gratuito
                        await asyncio.sleep(5.0)
                    else:
                        # FALLBACK PURAMENTE TÉCNICO
                        # Si no hay IA, usamos un score técnico simulado basado en el momentum
                        # Para que el motor táctico lo reciba y lo opere si cumple spreads y ATR
                        score = 85 # Score base para que pase al táctico (debe ser > 70)
                        logger.warning(f"Fallback Técnico: Enviando {c['symbol']} a motor táctico sin validación IA.")
                        await self.candidates_queue.put({"symbol": c['symbol'], "score": score})
                        
                        await asyncio.sleep(1.0) # Evitar procesar todo de golpe
                
            except Exception as e:
                logger.error(f"Error en ciclo Macro: {e}")
            
            # Intervalo dinámico según configuración global
            await asyncio.sleep(settings.MACRO_SCAN_INTERVAL_MINUTES * 60)
