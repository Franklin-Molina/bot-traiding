import asyncio
import time
from loguru import logger
from core.config import settings
from core.state import system_state, HealthStatus
from infrastructure.binance_ws import BinanceWS
from trading.executor import TradeExecutor
from trading.recovery import RecoveryEngine
from trading.indicators import PriceBuffer, TA
from trading.persistence import persistence_manager
from ai.ml_inference import HybridInferenceEngine

async def start_trading_engine(market_queue: asyncio.Queue, strategy_queue: asyncio.Queue, alert_queue: asyncio.Queue):
    """
    Orquestador del motor de trading (Motor 2 - Táctico).
    """
    logger.info("Iniciando Motor de Trading Táctico...")
    
    ws_client = BinanceWS(market_queue)
    inference_engine = HybridInferenceEngine()
    
    # Iniciar Persistencia Batch
    await persistence_manager.start()
    
    # Trabajadores desacoplados
    tasks = [
        asyncio.create_task(ws_client.connect()),
        asyncio.create_task(strategy_processor(market_queue, strategy_queue, alert_queue, ws_client, inference_engine))
    ]
    
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error en el motor de trading: {e}")
    finally:
        ws_client.stop()
        await persistence_manager.stop()
        for t in tasks:
            t.cancel()

async def strategy_processor(market_queue: asyncio.Queue, strategy_queue: asyncio.Queue, alert_queue: asyncio.Queue, ws_client: BinanceWS, inference_engine: HybridInferenceEngine):
    """
    Procesador de estrategias en tiempo real (Motor 2).
    Mantiene la lista de candidatos activos y monitorea sus ticks.
    """
    logger.info("Procesador de estrategias activo.")
    from infrastructure.binance_rest import BinanceRest
    executor = TradeExecutor(alert_queue)
    recovery = RecoveryEngine()
    binance_rest = BinanceRest()
    
    await executor.start()
    await executor.load_active_positions()
    
    async def check_macro_trend(sym: str, current_p: float) -> bool:
        try:
            klines = await binance_rest.get_klines(sym, '15m', limit=20)
            if not klines or len(klines) < 10:
                return True
            closes = [float(k[4]) for k in klines]
            closes[-1] = current_p # Update last unclosed candle
            ema9 = TA.calculate_ema(closes, 9)
            if ema9 is None:
                return True
            return current_p > ema9
        except Exception as e:
            logger.error(f"Error en macro trend check para {sym}: {e}")
            return True
    
    active_candidates = {} # symbol: {"data": dict, "created_at": float}
    price_buffers = {} # symbol: PriceBuffer
    book_tickers = {} # symbol: {'bid': float, 'ask': float, 'last_update': float}
    last_analysis = {} # symbol: timestamp
    last_logged_score = {} # symbol: score
    
    cleanup_interval = 60
    last_cleanup = time.time()
    
    while True:
        if not system_state.is_running:
            if system_state.panic_mode:
                logger.warning("¡MODO PÁNICO DETECTADO! Cerrando todo...")
                await executor.close_all_positions()
                while system_state.panic_mode:
                    await asyncio.sleep(1)
            
            await asyncio.sleep(1)
            continue

        # 1. Revisar si hay nuevos candidatos del Motor Macro
        try:
            new_candidates_batch = []
            while not strategy_queue.empty():
                new_candidates_batch.append(strategy_queue.get_nowait())
                strategy_queue.task_done()
                
            if new_candidates_batch:
                MAX_TRACKED_CANDIDATES = 3
                streams_to_subscribe = []
                streams_to_unsubscribe = []
                
                for new_candidate in new_candidates_batch:
                    symbol = new_candidate["symbol"]
                    
                    if symbol in active_candidates:
                        active_candidates[symbol]['data']['score'] = new_candidate['score']
                        continue

                    if len(active_candidates) >= MAX_TRACKED_CANDIDATES:
                        lowest_score_sym = min(active_candidates, key=lambda k: active_candidates[k]['data']['score'])
                        if new_candidate['score'] > active_candidates[lowest_score_sym]['data']['score']:
                            logger.info(f"Reemplazando {lowest_score_sym} por {symbol} (Mejor Score: {new_candidate['score']})")
                            del active_candidates[lowest_score_sym]
                            streams_to_unsubscribe.extend([
                                f"{lowest_score_sym.lower()}@trade",
                                f"{lowest_score_sym.lower()}@bookTicker"
                            ])
                            
                            active_candidates[symbol] = {
                                "data": new_candidate,
                                "created_at": time.time()
                            }
                            await executor.prepare_symbol(symbol)
                            streams_to_subscribe.extend([
                                f"{symbol.lower()}@trade",
                                f"{symbol.lower()}@bookTicker"
                            ])
                    else:
                        active_candidates[symbol] = {
                            "data": new_candidate,
                            "created_at": time.time()
                        }
                        logger.info(f"Recibido candidato táctico: {symbol} (Score: {new_candidate['score']})")
                        await executor.prepare_symbol(symbol)
                        streams_to_subscribe.extend([
                            f"{symbol.lower()}@trade",
                            f"{symbol.lower()}@bookTicker"
                        ])
                
                if streams_to_subscribe:
                    await ws_client.subscribe(streams_to_subscribe)
                    
        except Exception as e:
            logger.error(f"Error procesando cola de estrategias: {e}")

        # 1.1 Limpieza
        now = time.time()
        if now - last_cleanup > cleanup_interval:
            last_cleanup = now
            expired_candidates = [s for s, c in active_candidates.items() if now - c['created_at'] > 300]
            for s in expired_candidates:
                del active_candidates[s]
            
            expired_tickers = [s for s, t in book_tickers.items() if now - t.get('last_update', 0) > 60]
            for s in expired_tickers:
                if s not in active_candidates and s not in executor.active_positions:
                    del book_tickers[s]
            
            current_symbols = set(active_candidates.keys()) | set(executor.active_positions.keys()) | executor.pending_orders
            expired_buffers = [s for s in price_buffers if s not in current_symbols]
            for s in expired_buffers:
                del price_buffers[s]

        # 2. Procesar datos de mercado (Snapshot Architecture)
        try:
            # --- Batch Processing & Backpressure ---
            q_size = market_queue.qsize()
            max_q = market_queue.maxsize or 1000
            
            messages = []
            if q_size > 0:
                # Si hay saturación (>80%), drenamos agresivamente
                drain_limit = 50 if q_size < max_q * 0.8 else 200
                for _ in range(min(q_size, drain_limit)):
                    try:
                        messages.append(market_queue.get_nowait())
                    except asyncio.QueueEmpty: break
            else:
                # Si no hay datos, esperamos uno
                try:
                    messages.append(await asyncio.wait_for(market_queue.get(), timeout=0.5))
                except asyncio.TimeoutError: continue

            # --- Normalización y Actualización de Caché ---
            batch_ticks = []
            for msg in messages:
                if isinstance(msg, list): # miniTicker@arr (Macro)
                    batch_ticks.extend(msg)
                elif isinstance(msg, dict):
                    event_type = msg.get('e')
                    symbol = msg.get('s', '').upper()
                    if not symbol: continue
                    
                    if event_type == 'trade':
                        price = float(msg['p'])
                        if symbol in price_buffers:
                            price_buffers[symbol].add(price, timestamp=msg['E']/1000)
                        batch_ticks.append(msg)
                    elif 'b' in msg and 'a' in msg: # bookTicker
                        book_tickers[symbol] = {
                            'bid': float(msg['b']), 'ask': float(msg['a']), 'last_update': time.time()
                        }
                        # Generar pseudo-tick para mantener vivo el motor táctico
                        batch_ticks.append({'s': symbol, 'p': msg['a'], 'e': 'bookTicker', 'E': int(time.time()*1000)})
                    else:
                        batch_ticks.append(msg)
                
                market_queue.task_done()

            processed_in_cycle = set()
            for tick in batch_ticks:
                    try:
                        symbol = tick.get('s', '').upper()
                        if not symbol: continue
                        
                        is_relevant = (symbol in active_candidates or symbol in executor.active_positions or symbol in executor.pending_orders)
                        if not is_relevant: continue

                        if not symbol.endswith("USDT") or any(bad in symbol for bad in ["TRY", "EUR", "IDR", "GBP", "DAI", "RUB"]):
                            continue
                            
                        # Throttling por símbolo en este ciclo de batch
                        if symbol in processed_in_cycle: continue
                        processed_in_cycle.add(symbol)

                        current_price = float(tick.get('p', tick.get('c', 0)))
                        if current_price == 0: continue
                        
                        # Spread
                        spread_pct = 0.0
                        if symbol in book_tickers:
                            spread_pct = (book_tickers[symbol]['ask'] / book_tickers[symbol]['bid']) - 1

                        if symbol not in price_buffers:
                            if symbol not in recovery.recovery_started:
                                recovery.recovery_started.add(symbol)
                                price_buffers[symbol] = PriceBuffer(maxlen=300)
                                task = asyncio.create_task(recovery.recover_symbol(symbol, price_buffers[symbol]))
                                system_state.task_registry.register(task, f"Recovery_{symbol}")

                        if symbol in price_buffers and tick.get('e') != 'trade':
                             price_buffers[symbol].add(current_price)

                        # A. Monitorear salidas
                        current_atr = None
                        if symbol in price_buffers:
                            indicators = price_buffers[symbol].get_indicators()
                            current_atr = indicators.get('atr_14')
                        
                        executor.monitor_and_exit(symbol, current_price, atr=current_atr)

                        if recovery.is_in_warmup(symbol): continue

                        # B. Evaluar entrada
                        if symbol in active_candidates and symbol not in executor.active_positions:
                            now_ts = time.time()
                            if now_ts - last_analysis.get(symbol, 0) < settings.STRATEGY_EVAL_INTERVAL: 
                                continue
                            last_analysis[symbol] = now_ts

                            if system_state.is_paused: continue

                            candidate = active_candidates[symbol]['data']
                            indicators = price_buffers[symbol].get_indicators()
                            if indicators['atr_14'] is None: continue

                            atr_rel = indicators['atr_14'] / current_price
                            entry_score = 0
                            momentum_ok = False
                            
                            # 1. Micro-Momentum Real (Ventana Temporal 15s)
                            price_1s_ago = price_buffers[symbol].get_price_ago(15.0)
                            momentum = 0.0
                            if price_1s_ago:
                                momentum = (current_price - price_1s_ago) / price_1s_ago
                                local_range, _ = price_buffers[symbol].get_local_range(15.0)
                                
                                # Calcular Z-Score
                                z_score = price_buffers[symbol].get_momentum_zscore(momentum, current_ts=now_ts)
                                
                                # --- CORTACIRCUITOS DE ANOMALÍAS (Flash Crash/Pump) ---
                                # Disparamos si Z-Score > 3.0 (3 sigmas) o si falla catastróficamente con el umbral fijo
                                is_anomaly = (z_score is not None and abs(z_score) > 3.0) or abs(momentum) > 0.006 or local_range > 0.010
                                
                                if is_anomaly:
                                    logger.warning(f"💥 ANOMALÍA DETECTADA {symbol} | Z-Score: {z_score} | Mom={momentum:.4%} | Range={local_range:.4%} | Disparando purga IA.")
                                    system_state.invalidate_symbol_cache(symbol)
                                
                                # Filtro Táctico ENDURECIDO: > 0.08% de movimiento fuerte y seco en 15s
                                if momentum > 0.0008:
                                    entry_score += 30
                                    
                                    # 2. Expansión de Rango Local (Volatility Breakout ajustado)
                                    if local_range > 0.0006: # 0.06% min expansion (endurecido)
                                        entry_score += 20
                                        momentum_ok = True
                                        logger.success(f"🔥 MOMENTUM REAL {symbol} | Mom={momentum:.4%} | Range={local_range:.4%}")

                            # 3. Spread (Estricto)
                            spread_ok = False
                            if symbol in book_tickers:
                                if spread_pct <= settings.MAX_SPREAD_PERCENT:
                                    entry_score += 30
                                    spread_ok = True
                                    current_price = book_tickers[symbol]['ask']
                                else:
                                    logger.warning(f"❌ SPREAD FAIL {symbol} | {spread_pct:.4%} > {settings.MAX_SPREAD_PERCENT:.4%}")

                            # 4. ATR (Volatilidad Mínima)
                            if atr_rel >= settings.MIN_ATR_RELATIVE:
                                entry_score += 30

                            # 5. Régimen de Volatilidad
                            prices_list = list(price_buffers[symbol].prices)
                            regime = TA.detect_volatility_regime(prices_list, indicators['atr_14'])
                            if regime == "PANIC": entry_score -= 50
                            elif regime == "EXPANSION": entry_score += 10

                            # 6. Gestión de Candidatos Muertos (Pruning)
                            if entry_score < 20 and not momentum_ok:
                                if symbol in active_candidates:
                                    logger.warning(f"🗑️ Expulsando {symbol} (Score insuficiente: {entry_score})")
                                    active_candidates.pop(symbol, None)
                                    last_logged_score.pop(symbol, None)
                                    continue

                            # Logging Inteligente (Solo si cambia score significativamente)
                            if entry_score != last_logged_score.get(symbol):
                                logger.info(f"TACTICAL | {symbol} | Score={entry_score} | Mom={momentum:.5f} | RangeOK={momentum_ok} | SpreadOK={spread_ok}")
                                last_logged_score[symbol] = entry_score

                            if entry_score >= 80 and momentum_ok and spread_ok:
                                # Validación Macro (Filtro 15m)
                                macro_ok = await check_macro_trend(symbol, current_price)
                                if not macro_ok:
                                    logger.warning(f"🚫 COMPRA RECHAZADA por Tendencia Macro (15m bajista): {symbol}")
                                    # Aplicar delay para no spamear la API
                                    last_analysis[symbol] = now_ts + 60 
                                    continue

                                logger.success(f"🚀 GATILLO TÁCTICO: {symbol} Score: {entry_score} | Price: {current_price}")
                                
                                ml_features = {
                                    "market_regime": candidate.get("market_regime", "NORMAL"),
                                    "tech_score": candidate.get("tech_score", 50),
                                    "spread": spread_pct,
                                    "momentum_15s": momentum,
                                    "local_range_15s": local_range,
                                    "ai_raw": candidate.get("ai_raw", {})
                                }
                                
                                # INFERENCIA HÍBRIDA (XGBoost)
                                is_approved, prob_exito = inference_engine.predict_trade(ml_features)
                                
                                if not is_approved:
                                    logger.warning(f"🚫 RECHAZO XGBOOST: {symbol} | Prob Éxito: {prob_exito:.1%} < 40%")
                                    
                                    # Registrar Shadow Trade por Rechazo de ML
                                    from sqlalchemy import update
                                    from infrastructure.database import async_session
                                    from models.trading import MLTrainingData
                                    import uuid
                                    
                                    async with async_session() as session:
                                        shadow = MLTrainingData(
                                            trade_id=f"shadow_{uuid.uuid4().hex[:8]}",
                                            trade_type="SHADOW",
                                            status="PENDING",
                                            reject_reason="Rechazo XGBoost",
                                            entry_price=current_price,
                                            symbol=symbol,
                                            market_regime=ml_features["market_regime"],
                                            tech_score=ml_features["tech_score"],
                                            spread=spread_pct,
                                            momentum_15s=momentum,
                                            local_range_15s=local_range,
                                            ai_risk=ml_features["ai_raw"].get("risk", 0.0),
                                            ai_manipulation=ml_features["ai_raw"].get("manipulation", 0.0),
                                            ai_news=ml_features["ai_raw"].get("news_strength", 0.0),
                                            ai_momentum=ml_features["ai_raw"].get("momentum", 0.0),
                                            ai_confidence=ml_features["ai_raw"].get("confidence", 0.0)
                                        )
                                        session.add(shadow)
                                        await session.commit()
                                        
                                    active_candidates.pop(symbol, None)
                                    continue
                                    
                                logger.success(f"✅ APROBADO XGBOOST: {symbol} | Prob Éxito: {prob_exito:.1%}")
                                
                                await executor.try_buy(symbol, current_price, candidate['score'], atr=indicators['atr_14'], timestamp=tick.get('E'), momentum=momentum, ml_features=ml_features)
                                active_candidates.pop(symbol, None)

                    except Exception as e:
                        logger.exception(f"Error procesando tick para {tick.get('s', 'unknown')}: {e}")
        except asyncio.TimeoutError: continue
        except Exception as e:
            logger.error(f"Error crítico en loop de mercado: {e}")
