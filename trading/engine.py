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
    from infrastructure.binance_rest import get_binance_rest
    executor = TradeExecutor(alert_queue)
    recovery = RecoveryEngine()
    binance_rest = get_binance_rest()  # ARQ-1: Singleton compartido
    
    await executor.start()
    await executor.load_active_positions()
    
    async def check_macro_trend(sym: str, current_p: float) -> bool:
        """EST-3: Validación multi-timeframe (15m + 1H)."""
        try:
            # --- Timeframe 15m ---
            klines_15m = await binance_rest.get_klines(sym, '15m', limit=96)
            tf15_bearish = False
            if klines_15m and len(klines_15m) >= 50:
                closes = [float(k[4]) for k in klines_15m]
                closes[-1] = current_p
                ema9 = TA.calculate_ema(closes, 9)
                ema21 = TA.calculate_ema(closes, 21)
                ema50 = TA.calculate_ema(closes, 50)
                
                if ema9 and ema21 and ema50:
                    cumulative_vp = sum(((float(k[2]) + float(k[3]) + float(k[4])) / 3) * float(k[5]) for k in klines_15m)
                    cumulative_v = sum(float(k[5]) for k in klines_15m)
                    vwap = cumulative_vp / cumulative_v if cumulative_v > 0 else 0
                    
                    if current_p < ema9 and current_p < ema21 and current_p < ema50:
                        tf15_bearish = True
                    if current_p < vwap * 0.998:
                        tf15_bearish = True

            # --- Timeframe 1H ---
            klines_1h = await binance_rest.get_klines(sym, '1h', limit=50)
            tf1h_bearish = False
            if klines_1h and len(klines_1h) >= 20:
                closes_1h = [float(k[4]) for k in klines_1h]
                closes_1h[-1] = current_p
                ema20_1h = TA.calculate_ema(closes_1h, 20)
                ema50_1h = TA.calculate_ema(closes_1h, 50) if len(closes_1h) >= 50 else None
                
                if ema20_1h and current_p < ema20_1h * 0.995:
                    tf1h_bearish = True
                if ema50_1h and current_p < ema50_1h:
                    tf1h_bearish = True

            # Rechazar solo si AMBOS timeframes son bajistas
            if tf15_bearish and tf1h_bearish:
                logger.warning(f"🚫 Multi-TF Bearish: {sym} bajista en 15m Y 1H")
                return False
            # Advertir si uno es bajista
            if tf15_bearish or tf1h_bearish:
                logger.info(f"⚠️ {sym} parcialmente bajista: 15m={'↓' if tf15_bearish else '↑'} | 1H={'↓' if tf1h_bearish else '↑'}")
            return True
        except Exception as e:
            logger.error(f"Error en macro trend check para {sym}: {e}")
            return True
    
    active_candidates = {} # symbol: {"data": dict, "created_at": float}
    price_buffers = {} # symbol: PriceBuffer
    book_tickers = {} # symbol: {'bid': float, 'ask': float, 'last_update': float}
    last_analysis = {} # symbol: timestamp
    last_logged_score = {} # symbol: score
    pending_entries = {} # symbol: {"trigger_price": p, "base_price": p, "ts": ts, "score": s, "ml": dict}
    
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
            expired_candidates = [s for s, c in active_candidates.items() if now - c['created_at'] > 1800] # 30 mins paciencia
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
                        
                        now_ts = time.time()
                        
                        # --- VALIDACIÓN DE SEGUNDA ETAPA (Confirmador Adaptativo) ---
                        if symbol in pending_entries:
                            entry_data = pending_entries[symbol]
                            
                            # BUG-1 FIX: atr_rel ahora viene del pending_entry
                            atr_relative = entry_data.get("atr_rel", 0.002)
                            if atr_relative > 0.005:
                                confirm_seconds = 5
                            elif atr_relative > 0.003:
                                confirm_seconds = 8
                            else:
                                confirm_seconds = 12

                            trigger_price = entry_data["trigger_price"]
                            base_price = entry_data["base_price"]
                            
                            # Ejecución inmediata si sigue subiendo mucho (+0.05%)
                            if current_price >= trigger_price * 1.0005:
                                confirm_seconds = 0
                                logger.info(f"⚡ {symbol} Momentum acelerado, forzando ejecución inmediata.")
                                
                            elapsed = now_ts - entry_data["ts"]
                            if elapsed >= confirm_seconds:
                                total_move = trigger_price - base_price
                                max_retrace_price = trigger_price - (total_move * 0.5)
                                
                                if current_price >= max_retrace_price:
                                    logger.success(f"✅ CONFIRMACIÓN EXITOSA: {symbol} no retrocedió >50%")
                                    ml_features = entry_data["ml"]
                                    
                                    # INFERENCIA HÍBRIDA (XGBoost)
                                    is_approved, prob_exito = inference_engine.predict_trade(ml_features)
                                    
                                    if not is_approved:
                                        logger.warning(f"🚫 RECHAZO XGBOOST: {symbol} | Prob Éxito: {prob_exito:.1%} < 60%")
                                        active_candidates.pop(symbol, None)
                                    else:
                                        logger.success(f"✅ APROBADO XGBOOST: {symbol} | Prob Éxito: {prob_exito:.1%}")
                                        # BUG-2 FIX: Recalcular indicators dentro de confirmación
                                        confirm_atr = None
                                        if symbol in price_buffers:
                                            confirm_indicators = price_buffers[symbol].get_indicators()
                                            confirm_atr = confirm_indicators.get('atr_14')
                                        await executor.try_buy(symbol, current_price, entry_data["score"], atr=confirm_atr, timestamp=tick.get('E'), momentum=ml_features.get("momentum_15s", 0), ml_features=ml_features)
                                        active_candidates.pop(symbol, None)
                                else:
                                    logger.warning(f"🚫 COMPRA ABORTADA: {symbol} retrocedió >50% en los 10s de espera.")
                                
                                del pending_entries[symbol]
                                last_analysis[symbol] = now_ts + 30 # Cooldown tras intento

                        # B. Evaluar entrada
                        if symbol in active_candidates and symbol not in executor.active_positions and symbol not in pending_entries:
                            now_ts = time.time()
                            if now_ts - last_analysis.get(symbol, 0) < settings.STRATEGY_EVAL_INTERVAL: 
                                continue
                            last_analysis[symbol] = now_ts

                            if system_state.is_paused: continue

                            candidate = active_candidates[symbol]['data']
                            indicators = price_buffers[symbol].get_indicators()
                            if indicators['atr_14'] is None: continue

                            # EST-1: Filtro RSI — rechazar si está en sobrecompra extrema
                            rsi_14 = indicators.get('rsi_14')
                            if rsi_14 is not None and rsi_14 > settings.MAX_RSI_ENTRY:
                                logger.warning(f"🚫 RSI OVERBOUGHT: {symbol} RSI={rsi_14:.1f} > {settings.MAX_RSI_ENTRY}. Cooldown 60s.")
                                last_analysis[symbol] = now_ts + 60
                                continue
                            atr_rel = indicators['atr_14'] / current_price
                            entry_score = 0
                            momentum_ok = False
                            
                            # 1. Micro-Momentum Real (Ventana Temporal 60s)
                            price_60s_ago = price_buffers[symbol].get_price_ago(60.0)
                            momentum = 0.0
                            local_range = 0.0  # BUG-3 FIX: Inicializar antes del bloque condicional
                            if price_60s_ago:
                                momentum = (current_price - price_60s_ago) / price_60s_ago
                                local_range, _ = price_buffers[symbol].get_local_range(60.0)
                                
                                # Volumen relativo (últimos 60s vs promedio histórico 1H)
                                rel_volume = price_buffers[symbol].get_relative_volume(60.0, baseline_sec=3600.0)
                                
                                # Calcular Z-Score
                                z_score = price_buffers[symbol].get_momentum_zscore(momentum, current_ts=now_ts)
                                
                                # --- CORTACIRCUITOS DE ANOMALÍAS (Flash Crash/Pump) ---
                                if z_score is not None and abs(z_score) > 3.5:
                                    logger.warning(f"💥 ANOMALÍA GRAVE DETECTADA {symbol} | Z-Score: {z_score} > 3.5 | Descartando.")
                                    system_state.invalidate_symbol_cache(symbol)
                                    active_candidates.pop(symbol, None)
                                    continue
                                elif z_score is not None and abs(z_score) > 2.0:
                                    logger.warning(f"⚠️ RUIDO DETECTADO {symbol} | Z-Score: {z_score} > 2.0 | Cooldown 60s.")
                                    last_analysis[symbol] = now_ts + 60
                                    continue
                                    
                                is_anomaly = abs(momentum) > 0.008 or local_range > 0.015
                                
                                if is_anomaly:
                                    logger.warning(f"💥 MOVIMIENTO EXTREMO DETECTADO {symbol} | Mom={momentum:.4%} | Range={local_range:.4%} | Cooldown.")
                                    last_analysis[symbol] = now_ts + 120
                                    continue
                                
                                # Filtro Táctico: > 0.03% de movimiento fuerte en 60s + Volumen Relativo
                                if momentum > 0.0003 and rel_volume > 1.2:
                                    entry_score += 30
                                    
                                    # 2. Expansión de Rango Local (Volatility Breakout ajustado)
                                    if local_range > 0.0003: # 0.03% min expansion
                                        entry_score += 20
                                        momentum_ok = True
                                        logger.success(f"🔥 MOMENTUM REAL {symbol} | Mom={momentum:.4%} | Range={local_range:.4%} | RelVol={rel_volume:.2f}")

                            # 3. Spread Adaptativo
                            spread_ok = False
                            if symbol in book_tickers:
                                market_regime = candidate.get("market_regime", "WARM")
                                
                                if market_regime == "HOT":
                                    max_spread = settings.MAX_SPREAD_PERCENT
                                elif market_regime == "WARM":
                                    max_spread = settings.MAX_SPREAD_PERCENT * 0.8
                                else:
                                    max_spread = settings.MAX_SPREAD_PERCENT * 0.6
                                    
                                # Permitir un poco de holgura extra basada en ATR si hay muchísima volatilidad
                                if atr_rel > 0.01:
                                    max_spread = max(max_spread, 0.0020)
                                    
                                if spread_pct <= max_spread:
                                    entry_score += 30
                                    spread_ok = True
                                    current_price = book_tickers[symbol]['ask']
                                else:
                                    logger.debug(f"SPREAD FAIL {symbol} | {spread_pct:.4%} > {max_spread:.4%}")

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

                            # ARQ-3: Logging Inteligente (Solo si cambia score significativamente)
                            if entry_score != last_logged_score.get(symbol):
                                logger.debug(f"TACTICAL | {symbol} | Score={entry_score} | Mom={momentum:.5f} | RangeOK={momentum_ok} | SpreadOK={spread_ok}")
                                last_logged_score[symbol] = entry_score

                            if entry_score >= 80 and momentum_ok and spread_ok:
                                # Validación Macro (Filtro 15m)
                                macro_ok = await check_macro_trend(symbol, current_price)
                                if not macro_ok:
                                    logger.warning(f"🚫 COMPRA RECHAZADA por Tendencia Macro (15m bajista): {symbol}")
                                    # Aplicar delay para no spamear la API
                                    last_analysis[symbol] = now_ts + 60 
                                    continue

                                logger.success(f"🚀 GATILLO TÁCTICO PENDIENTE DE CONFIRMACIÓN: {symbol} Score: {entry_score} | Price: {current_price}")
                                
                                ml_features = {
                                    "market_regime": candidate.get("market_regime", "NORMAL"),
                                    "tech_score": candidate.get("tech_score", 50),
                                    "spread": spread_pct,
                                    "momentum_15s": momentum,
                                    "local_range_15s": local_range,
                                    # FEAT-5: Features avanzadas del PriceBuffer
                                    "rsi_14": indicators.get("rsi_14", 50.0),
                                    "bollinger_pos": indicators.get("bollinger_pos", 0.5),
                                    "tick_rate": indicators.get("tick_rate", 0.0),
                                    "atr_relative": indicators.get("atr_relative", 0.0),
                                    "hour_sin": indicators.get("hour_sin", 0.0),
                                    "hour_cos": indicators.get("hour_cos", 1.0),
                                }
                                
                                # Enviar a cola de confirmación
                                pending_entries[symbol] = {
                                    "trigger_price": current_price,
                                    "base_price": price_60s_ago,
                                    "ts": now_ts,
                                    "score": entry_score,
                                    "atr_rel": atr_rel,  # BUG-1 FIX: Guardar para confirmación adaptativa
                                    "ml": ml_features
                                }

                    except Exception as e:
                        logger.exception(f"Error procesando tick para {tick.get('s', 'unknown')}: {e}")
        except asyncio.TimeoutError: continue
        except Exception as e:
            logger.error(f"Error crítico en loop de mercado: {e}")
