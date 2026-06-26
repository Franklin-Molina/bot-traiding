import asyncio
import time
import math
from loguru import logger
from core.config import settings
from core.state import system_state
from infrastructure.binance_rest import BinanceRest
from ai.orchestrator import AIOrchestrator
import uuid
from infrastructure.database import async_session
from models.trading import MLTrainingData

class MacroEngine:
    def __init__(self, candidates_queue: asyncio.Queue, alert_queue: asyncio.Queue):
        self.candidates_queue = candidates_queue
        self.alert_queue = alert_queue
        self.binance = BinanceRest()
        self.ai = AIOrchestrator()
        self.cooldowns = {} # symbol: {"exp": timestamp, "reason": str}

    def _clean_cooldowns(self):
        now = time.time()
        self.cooldowns = {k: v for k, v in self.cooldowns.items() if v["exp"] > now}

    async def _record_shadow_trade(self, symbol: str, price: float, tech_score: float, market_regime: str, ai_raw: dict, reject_reason: str):
        """Guarda un trade rechazado en la base de datos para Outcome Tracking (Triple Barrier)."""
        try:
            async with async_session() as session:
                shadow = MLTrainingData(
                    trade_id=f"shadow_{uuid.uuid4().hex[:8]}",
                    trade_type="SHADOW",
                    status="PENDING",
                    reject_reason=reject_reason,
                    entry_price=price,
                    symbol=symbol,
                    market_regime=market_regime,
                    tech_score=tech_score,
                    ai_risk=ai_raw.get("risk", 0.0),
                    ai_manipulation=ai_raw.get("manipulation", 0.0),
                    ai_news=ai_raw.get("news_strength", 0.0),
                    ai_momentum=ai_raw.get("momentum", 0.0),
                    ai_confidence=ai_raw.get("confidence", 0.0)
                )
                session.add(shadow)
                await session.commit()
        except Exception as e:
            logger.error(f"Error guardando SHADOW_TRADE para {symbol}: {e}")

    async def run_cycle(self):
        """
        Ejecuta el ciclo macro: escaneo -> filtrado -> IA.
        """
        while True:
            if not system_state.is_running:
                logger.info("Motor Macro en espera (Sistema OFF o PÁNICO)...")
                await asyncio.sleep(60)
                continue

            self._clean_cooldowns()
            now = time.time()

            # --- HEALTHCHECK IA ---
            is_ai_healthy = await self.ai.test_connection()
            if not is_ai_healthy:
                if system_state.ai_enabled:
                    logger.critical("⚠️ FALLO CRÍTICO IA - Activando Fallback Técnico")
                    system_state.ai_enabled = False
                    try:
                        await self.alert_queue.put("⚠️ **Alerta del Sistema**\nFallo o Rate Limit con OpenRouter IA.\n🔄 **Cambiando a Fallback Técnico.**")
                    except Exception:
                        pass
            else:
                if not system_state.ai_enabled:
                    logger.success("✅ IA Recuperada - Desactivando Fallback Técnico")
                system_state.ai_enabled = True

            try:
                # 1. EVALUAR MARKET REGIME (BTC)
                tickers = await self.binance.get_24hr_tickers()
                btc_ticker = next((t for t in tickers if t['symbol'] == 'BTCUSDT'), None)
                
                market_score = 50
                market_regime = "WARM"
                scan_interval_override = None
                
                if btc_ticker:
                    btc_change = float(btc_ticker['priceChangePercent'])
                    # Cálculo empírico simple de score de mercado
                    market_score = int(min(max(50 + (btc_change * 10), 0), 100))
                    
                    if market_score < 30 or abs(btc_change) < 0.5:
                        market_regime = "DEAD"
                        scan_interval_override = 15 # mins
                    elif market_score > 70:
                        market_regime = "HOT"
                
                logger.info(f"Iniciando ciclo Macro... Regime: {market_regime} | MarketScore: {market_score} | IA: {system_state.ai_enabled}")

                # 2. OBTENER SPREADS ACTUALES (Pre-Filtro)
                book_tickers = await self.binance.get_book_tickers()
                spreads = {}
                for bt in book_tickers:
                    bid = float(bt['bidPrice'])
                    ask = float(bt['askPrice'])
                    if bid > 0:
                        spreads[bt['symbol']] = ((ask - bid) / bid)
                
                # 3. FILTRAR CANDIDATOS
                candidates = []
                for t in tickers:
                    symbol = t['symbol']
                    
                    if symbol in self.cooldowns:
                        continue
                        
                    if symbol.endswith("USDT") and not any(x in symbol for x in ["UP", "DOWN", "BEAR", "BULL", "USDC", "FDUSD"]):
                        change = float(t['priceChangePercent'])
                        volume = float(t['quoteVolume'])
                        
                        if change >= 1.0 and volume > 10_000_000:
                            if symbol not in spreads:
                                logger.debug(f"Descartado {symbol}: No tiene liquidez o fue delistada (No Orderbook)")
                                self.cooldowns[symbol] = {"exp": now + (settings.COOLDOWN_SPREAD_MINUTES * 60), "reason": "No Orderbook"}
                                continue
                                
                            spread = spreads[symbol]
                            if spread > settings.MAX_SPREAD_PERCENT:
                                logger.debug(f"Descartado {symbol} por Spread Alto: {spread*100:.2f}% (Máx {settings.MAX_SPREAD_PERCENT*100:.2f}%)")
                                self.cooldowns[symbol] = {"exp": now + (settings.COOLDOWN_SPREAD_MINUTES * 60), "reason": "Spread Alto"}
                                continue
                                
                            # Calcular Technical Score
                            # Normalizar volumen (10M a 100M+) -> 0 a 50 puntos
                            vol_score = min(max((math.log10(max(volume, 1)) - 7) / 1.5 * 50, 0), 50)
                            # Normalizar change (1% a 20%+) -> 0 a 50 puntos
                            chg_score = min(max((change - 1.0) / 19.0 * 50, 0), 50)
                            
                            tech_score = int(vol_score + chg_score)
                            
                            if tech_score >= settings.MIN_TECHNICAL_SCORE_AI:
                                candidates.append({
                                    "symbol": symbol,
                                    "change": change,
                                    "volume": volume,
                                    "tech_score": tech_score
                                })
                
                logger.info(f"Candidatos iniciales tras Spread y TechScore >= {settings.MIN_TECHNICAL_SCORE_AI}: {len(candidates)}")

                # 4. ORDENAR Y LIMITAR (Modo Degradado)
                candidates = sorted(candidates, key=lambda x: x['tech_score'], reverse=True)
                
                top_n = 3
                # Remover penalidad pre-IA para que evalúe a los candidatos con al menos MIN_TECHNICAL_SCORE_AI
                min_tech_req = settings.MIN_TECHNICAL_SCORE_AI
                
                candidates_to_eval = [c for c in candidates if c['tech_score'] >= min_tech_req][:top_n]

                if candidates and not candidates_to_eval:
                    max_score_found = candidates[0]['tech_score']
                    logger.info(f"⚠️ Candidatos descartados por Régimen {market_regime}. Necesitaban {min_tech_req} pts, el mejor tuvo {max_score_found} pts.")

                for c in candidates_to_eval:
                    symbol = c['symbol']
                    tech_score = c['tech_score']
                    
                    ai_score = 0
                    raw_ai_json = {}
                    
                    if system_state.ai_enabled:
                        ai_score, raw_ai_json = await self.ai.analyze_asset(symbol, context=c)
                        
                        if ai_score == -1: # Exhaustion
                            logger.warning(f"⚠️ API Keys agotadas. Fallback Técnico activado.")
                            system_state.ai_enabled = False
                            ai_score = 50
                        elif ai_score < 50:
                            logger.info(f"❌ IA rechaza {symbol} (Score: {ai_score}). Cooldown {settings.COOLDOWN_AI_REJECT_MINUTES}m.")
                            self.cooldowns[symbol] = {"exp": now + (settings.COOLDOWN_AI_REJECT_MINUTES * 60), "reason": "IA Reject"}
                            await self._record_shadow_trade(symbol, c.get('price', 0), tech_score, market_regime, raw_ai_json, "IA Reject")
                            await asyncio.sleep(5.0)
                            continue
                        
                        await asyncio.sleep(5.0) # Anti-ban
                    else:
                        ai_score = 50 # Neutro en fallback
                        
                    # 5. SCORE FINAL (3 CAPAS)
                    final_score = int((market_score * 0.2) + (tech_score * 0.5) + (ai_score * 0.3))
                    
                    # LOGGING ML (Telemetría)
                    logger.debug(f"📊 ML-TELEMETRY | {symbol} | Market: {market_score} | Tech: {tech_score} | AI: {ai_score} | Final: {final_score} | AI_Raw: {raw_ai_json}")
                    
                    umbral_aprobacion = settings.MIN_TECHNICAL_SCORE_AI
                    if market_regime == "DEAD":
                        umbral_aprobacion -= 5
                        
                    if final_score >= umbral_aprobacion or (not system_state.ai_enabled and tech_score >= settings.MIN_TECHNICAL_SCORE_AI + 5):
                        logger.success(f"🚀 ¡Aprobado! {symbol} | Score Final: {final_score}")
                        await self.candidates_queue.put({
                            "symbol": symbol, 
                            "score": final_score,
                            "tech_score": tech_score,
                            "market_regime": market_regime,
                            "ai_raw": raw_ai_json
                        })
                    else:
                        logger.info(f"🗑️ Descartado post-fusión {symbol} | Score Final: {final_score}")
                        await self._record_shadow_trade(symbol, c.get('price', 0), tech_score, market_regime, raw_ai_json, "Low Final Score")

            except Exception as e:
                logger.error(f"Error en ciclo Macro: {e}")
            
            wait_time = (scan_interval_override or settings.MACRO_SCAN_INTERVAL_MINUTES) * 60
            await asyncio.sleep(wait_time)
