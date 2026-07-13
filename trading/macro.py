import asyncio
import time
import math
from loguru import logger
from core.config import settings
from core.state import system_state
from infrastructure.binance_rest import BinanceRest
import uuid
from infrastructure.database import async_session
from models.trading import MLTrainingData

class MacroEngine:
    def __init__(self, candidates_queue: asyncio.Queue, alert_queue: asyncio.Queue):
        self.candidates_queue = candidates_queue
        self.alert_queue = alert_queue
        self.binance = BinanceRest()
        self.cooldowns = {} # symbol: {"exp": timestamp, "reason": str}

    def _clean_cooldowns(self):
        now = time.time()
        self.cooldowns = {k: v for k, v in self.cooldowns.items() if v["exp"] > now}

    async def _record_shadow_trade(self, symbol: str, price: float, tech_score: float, market_regime: str, reject_reason: str):
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
                    tech_score=tech_score
                )
                session.add(shadow)
                await session.commit()
        except Exception as e:
            logger.error(f"Error guardando SHADOW_TRADE para {symbol}: {e}")

    async def run_cycle(self):
        """
        Ejecuta el ciclo macro: escaneo -> filtrado técnico (Sin IA).
        """
        while True:
            if not system_state.is_running:
                logger.info("Motor Macro en espera (Sistema OFF o PÁNICO)...")
                await asyncio.sleep(60)
                continue

            self._clean_cooldowns()
            now = time.time()

            try:
                # 1. EVALUAR MARKET REGIME (BTC)
                tickers = await self.binance.get_24hr_tickers()
                btc_ticker = next((t for t in tickers if t['symbol'] == 'BTCUSDT'), None)
                
                market_score = 50
                market_regime = "WARM"
                scan_interval_override = None
                
                if btc_ticker:
                    btc_change = float(btc_ticker['priceChangePercent'])
                    market_score = int(min(max(50 + (btc_change * 15), 0), 100)) # Más sensibilidad al BTC
                    
                    if market_score < 30 or btc_change < -0.5:
                        market_regime = "DEAD"
                        scan_interval_override = 3 # mins
                    elif market_score > 75:
                        market_regime = "HOT"
                
                logger.info(f"Iniciando ciclo Macro... Regime: {market_regime} | MarketScore: {market_score}")

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
                                self.cooldowns[symbol] = {"exp": now + (settings.COOLDOWN_SPREAD_MINUTES * 60), "reason": "No Orderbook"}
                                continue
                                
                            spread = spreads[symbol]
                            
                            # Spread Dinámico según el Régimen
                            tolerancia_spread = settings.MAX_SPREAD_PERCENT # HOT default
                            if market_regime == "DEAD":
                                tolerancia_spread = settings.MAX_SPREAD_PERCENT * 0.6  # 60% of max
                            elif market_regime == "WARM":
                                tolerancia_spread = settings.MAX_SPREAD_PERCENT * 0.8  # 80% of max
                                
                            if spread > tolerancia_spread:
                                logger.debug(f"Descartado {symbol} por Spread Alto: {spread*100:.3f}% (Máx {tolerancia_spread*100:.3f}%)")
                                self.cooldowns[symbol] = {"exp": now + (settings.COOLDOWN_SPREAD_MINUTES * 60), "reason": "Spread Alto"}
                                continue
                                
                            # Calcular Technical Score No Lineal
                            # Volumen base logarítmico (da menos peso a incrementos absurdos)
                            vol_score = min(max((math.log10(max(volume, 1)) - 7) / 1.5 * 40, 0), 40)
                            # Rate of Change exponencial (premia cambios explosivos)
                            chg_score = min(max(((change - 1.0) ** 1.2) / (19.0 ** 1.2) * 60, 0), 60)
                            
                            tech_score = int(vol_score + chg_score)
                            
                            if tech_score >= settings.MIN_TECHNICAL_SCORE_AI:
                                candidates.append({
                                    "symbol": symbol,
                                    "change": change,
                                    "volume": volume,
                                    "tech_score": tech_score,
                                    "price": float(t['lastPrice'])
                                })
                
                logger.info(f"Candidatos iniciales tras Spread y TechScore >= {settings.MIN_TECHNICAL_SCORE_AI}: {len(candidates)}")

                # 4. ORDENAR Y LIMITAR
                candidates = sorted(candidates, key=lambda x: x['tech_score'], reverse=True)
                top_n = 5
                min_tech_req = settings.MIN_TECHNICAL_SCORE_AI
                candidates_to_eval = [c for c in candidates if c['tech_score'] >= min_tech_req][:top_n]

                if candidates and not candidates_to_eval:
                    logger.info(f"⚠️ Candidatos descartados por Score. Necesitaban {min_tech_req} pts, el mejor tuvo {candidates[0]['tech_score']} pts.")

                for c in candidates_to_eval:
                    symbol = c['symbol']
                    tech_score = c['tech_score']
                    
                    # 5. SCORE FINAL (Recalibrado: 35% Mercado, 65% Tech)
                    final_score = int((market_score * 0.35) + (tech_score * 0.65))
                    
                    logger.debug(f"📊 ML-TELEMETRY | {symbol} | Market: {market_score} | Tech: {tech_score} | Final: {final_score}")
                    
                    umbral_aprobacion = settings.MIN_TECHNICAL_SCORE_AI
                    if market_regime == "DEAD":
                        umbral_aprobacion += 5 # Más estricto si el mercado está muerto
                        
                    if final_score >= umbral_aprobacion:
                        logger.success(f"🚀 ¡Aprobado! {symbol} | Score Final: {final_score} | Spread: {spreads[symbol]*100:.3f}%")
                        await self.candidates_queue.put({
                            "symbol": symbol, 
                            "score": final_score,
                            "tech_score": tech_score,
                            "market_regime": market_regime,
                            "ai_raw": {} # Backward compatibility if needed
                        })
                    else:
                        logger.info(f"🗑️ Descartado post-fusión {symbol} | Score Final: {final_score}")
                        await self._record_shadow_trade(symbol, c.get('price', 0), tech_score, market_regime, "Low Final Score")

            except Exception as e:
                logger.error(f"Error en ciclo Macro: {e}")
            
            wait_time = (scan_interval_override or settings.MACRO_SCAN_INTERVAL_MINUTES) * 60
            await asyncio.sleep(wait_time)
