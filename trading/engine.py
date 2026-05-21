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

async def start_trading_engine(market_queue: asyncio.Queue, strategy_queue: asyncio.Queue, alert_queue: asyncio.Queue):
    """
    Orquestador del motor de trading (Motor 2 - Táctico).
    """
    logger.info("Iniciando Motor de Trading Táctico...")
    
    ws_client = BinanceWS(market_queue)
    
    # Iniciar Persistencia Batch
    await persistence_manager.start()
    
    # Trabajadores desacoplados
    tasks = [
        asyncio.create_task(ws_client.connect()),
        asyncio.create_task(strategy_processor(market_queue, strategy_queue, alert_queue, ws_client))
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

async def strategy_processor(market_queue: asyncio.Queue, strategy_queue: asyncio.Queue, alert_queue: asyncio.Queue, ws_client: BinanceWS):
    """
    Procesador de estrategias en tiempo real (Motor 2).
    Mantiene la lista de candidatos activos y monitorea sus ticks.
    """
    logger.info("Procesador de estrategias activo.")
    executor = TradeExecutor(alert_queue)
    recovery = RecoveryEngine()
    
    await executor.start()
    await executor.load_active_positions()
    
    active_candidates = {} # symbol: {"data": dict, "created_at": float}
    price_buffers = {} # symbol: PriceBuffer
    book_tickers = {} # symbol: {'bid': float, 'ask': float, 'last_update': float}
    last_analysis = {} # symbol: timestamp
    
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
            while not strategy_queue.empty():
                new_candidate = strategy_queue.get_nowait()
                symbol = new_candidate["symbol"]
                active_candidates[symbol] = {
                    "data": new_candidate,
                    "created_at": time.time()
                }
                logger.info(f"Recibido candidato táctico: {symbol} (Score: {new_candidate['score']})")
                await ws_client.subscribe([f"{symbol.lower()}@bookTicker"])
                strategy_queue.task_done()
        except asyncio.QueueEmpty:
            pass

        # 1.1 Limpieza de candidatos y tickers obsoletos (TTL)
        now = time.time()
        if now - last_cleanup > cleanup_interval:
            last_cleanup = now
            # Limpiar candidatos > 5 minutos
            expired_candidates = [s for s, c in active_candidates.items() if now - c['created_at'] > 300]
            for s in expired_candidates:
                del active_candidates[s]
                if s not in executor.active_positions and s not in executor.pending_orders:
                    # Opcionalmente desuscribir si no es posición activa
                    pass
            
            # Limpiar book_tickers > 1 minuto
            expired_tickers = [s for s, t in book_tickers.items() if now - t.get('last_update', 0) > 60]
            for s in expired_tickers:
                if s not in active_candidates and s not in executor.active_positions:
                    del book_tickers[s]
            
            # Limpiar buffers de símbolos que ya no estamos siguiendo
            current_symbols = set(active_candidates.keys()) | set(executor.active_positions.keys()) | executor.pending_orders
            expired_buffers = [s for s in price_buffers if s not in current_symbols]
            for s in expired_buffers:
                del price_buffers[s]

        # 2. Procesar datos de mercado
        try:
            # 🚀 Backpressure: Si la cola está muy llena, vaciamos para procesar solo lo último
            q_size = market_queue.qsize()
            if q_size > 500:
                logger.warning(f"Cola de mercado saturada ({q_size}). Purgando para reducir lag...")
                while market_queue.qsize() > 50:
                    try:
                        market_queue.get_nowait()
                        market_queue.task_done()
                    except asyncio.QueueEmpty:
                        break

            data = await asyncio.wait_for(market_queue.get(), timeout=0.1)
            
            try:
                batch = []
                if isinstance(data, list): # miniTicker@arr
                    batch = data
                elif isinstance(data, dict): # bookTicker individual
                    if data.get('u') or 'b' in data: # Es un bookTicker
                        symbol = data['s']
                        book_tickers[symbol] = {
                            'bid': float(data['b']),
                            'ask': float(data['a']),
                            'last_update': time.time()
                        }
                        mid_price = (float(data['b']) + float(data['a'])) / 2
                        if symbol in price_buffers:
                            price_buffers[symbol].add(mid_price, high=float(data['a']), low=float(data['b']))
                        continue

                for tick in batch:
                    try:
                        symbol = tick['s']
                        
                        # 🛡️ Filtro de Relevancia (EVITAR PROCESAMIENTO INNECESARIO)
                        is_relevant = (
                            symbol in active_candidates or 
                            symbol in executor.active_positions or 
                            symbol in executor.pending_orders
                        )
                        
                        if not is_relevant:
                            continue

                        if not symbol.endswith("USDT") or any(bad in symbol for bad in ["TRY", "EUR", "IDR", "GBP", "DAI", "RUB"]):
                            continue

                        current_price = float(tick['c'])
                        
                        if symbol not in price_buffers:
                            if symbol not in recovery.recovery_started:
                                recovery.recovery_started.add(symbol)
                                price_buffers[symbol] = PriceBuffer(maxlen=100)
                                task = asyncio.create_task(recovery.recover_symbol(symbol, price_buffers[symbol]))
                                system_state.task_registry.register(task, f"Recovery_{symbol}")

                        if symbol in price_buffers:
                            price_buffers[symbol].add(current_price)

                        # A. Monitorear salidas
                        current_atr = None
                        if symbol in price_buffers:
                            indicators = price_buffers[symbol].get_indicators(update_atr=False)
                            current_atr = indicators.get('atr_14')
                        
                        await executor.monitor_and_exit(symbol, current_price, atr=current_atr)

                        if recovery.is_in_warmup(symbol):
                            continue

                        # B. Evaluar entrada
                        if symbol in active_candidates and symbol not in executor.active_positions:
                            if symbol not in price_buffers:
                                # Esto no debería ocurrir con el fix de recovery_started, pero somos defensivos
                                logger.warning(f"Buffer faltante para candidato {symbol}. Reintentando recuperación...")
                                recovery.recovery_started.discard(symbol)
                                continue

                            now_ts = time.time()
                            if now_ts - last_analysis.get(symbol, 0) < 5:
                                continue
                            last_analysis[symbol] = now_ts

                            if system_state.is_paused:
                                logger.debug(f"Ignorando entrada {symbol} (Sistema PAUSADO)")
                            else:
                                candidate = active_candidates[symbol]['data']
                                indicators = price_buffers[symbol].get_indicators(update_atr=True)
                                
                                if indicators['atr_14'] is None:
                                    continue

                                atr_rel = indicators['atr_14'] / current_price
                                entry_score = 0
                                
                                prices_list = list(price_buffers[symbol].prices)
                                momentum_ok = False
                                momentum = 0
                                
                                # Usamos un Lookback dinámico. Queremos comparar el precio actual
                                # con el precio de hace ~20-50 ticks para ver tendencia real, no ruido.
                                lookback_period = min(len(prices_list) - 1, 50) 
                                
                                if lookback_period >= 10: # Requerimos al menos 10 ticks de historial
                                    # Momentum = % de cambio desde el precio histórico
                                    historical_price = prices_list[-(lookback_period + 1)]
                                    momentum = (prices_list[-1] - historical_price) / historical_price
                                    
                                    # Reducimos la exigencia de 0.1% a 0.05% (es scalping)
                                    if momentum > 0.0005: 
                                        entry_score += 20
                                        
                                        # Confirmación de Micro-Tendencia: El precio actual
                                        # debe ser mayor a una Media Móvil Simple de los últimos 5 ticks
                                        short_sma = sum(prices_list[-5:]) / 5
                                        if prices_list[-1] > short_sma:
                                            entry_score += 20
                                            momentum_ok = True

                                spread_ok = False
                                if symbol in book_tickers:
                                    bt = book_tickers[symbol]
                                    spread_pct = (bt['ask'] / bt['bid']) - 1
                                    if spread_pct <= settings.MAX_SPREAD_PERCENT:
                                        entry_score += 30
                                        spread_ok = True
                                        current_price = bt['ask']

                                if atr_rel >= settings.MIN_ATR_RELATIVE:
                                    entry_score += 30

                                regime = TA.detect_volatility_regime(prices_list, indicators['atr_14'])
                                if regime == "PANIC":
                                    entry_score -= 50
                                elif regime == "EXPANSION":
                                    entry_score += 10

                                if entry_score >= 70 and momentum_ok and spread_ok:
                                    anticipation_factor = 0.0005 if momentum > 0.002 else 0.0
                                    adjusted_price = current_price * (1 - anticipation_factor)
                                    
                                    logger.success(f"🚀 GATILLO TÁCTICO: {symbol} Score: {entry_score} | Price: {current_price}")
                                    
                                    await executor.try_buy(
                                        symbol, 
                                        current_price, 
                                        candidate['score'], 
                                        atr=indicators['atr_14'],
                                        timestamp=tick.get('E'),
                                        momentum=momentum
                                    )
                                    active_candidates.pop(symbol, None)
                                else:
                                    if price_buffers[symbol]._tick_count % 50 == 0:
                                        logger.debug(f"Analizando {symbol}: Score {entry_score}")
                    except Exception as e:
                        logger.exception(f"Error procesando tick para {tick.get('s', 'unknown')}: {e}")
            finally:
                market_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Error crítico en loop de mercado: {e}")
