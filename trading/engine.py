import asyncio
import time
from loguru import logger
from core.config import settings
from core.state import system_state, HealthStatus
from infrastructure.binance_ws import BinanceWS
from trading.executor import TradeExecutor
from trading.recovery import RecoveryEngine
from trading.indicators import PriceBuffer, TA

async def start_trading_engine(market_queue: asyncio.Queue, strategy_queue: asyncio.Queue, alert_queue: asyncio.Queue):
    """
    Orquestador del motor de trading (Motor 2 - Táctico).
    """
    logger.info("Iniciando Motor de Trading Táctico...")
    
    ws_client = BinanceWS(market_queue)
    
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
    
    await executor.load_active_positions()
    
    active_candidates = {} # symbol: macro_data
    price_buffers = {} # symbol: PriceBuffer
    book_tickers = {} # symbol: {'bid': float, 'ask': float}
    last_analysis = {} # symbol: timestamp
    momentum_confirmations = {} # symbol: count
    
    while True:
        if not system_state.is_running:
            if system_state.panic_mode:
                logger.warning("¡MODO PÁNICO DETECTADO! Cerrando todo...")
                await executor.close_all_positions()
                # Una vez cerrado todo, podemos quedarnos en pausa o salir
                while system_state.panic_mode:
                    await asyncio.sleep(1)
            
            await asyncio.sleep(1)
            continue

        # 1. Revisar si hay nuevos candidatos del Motor Macro
        try:
            while not strategy_queue.empty():
                new_candidate = strategy_queue.get_nowait()
                symbol = new_candidate["symbol"]
                active_candidates[symbol] = new_candidate
                logger.info(f"Recibido candidato táctico: {symbol} (Score: {new_candidate['score']})")
                # Suscribirse dinámicamente al bookTicker para precisión
                await ws_client.subscribe([f"{symbol.lower()}@bookTicker"])
                strategy_queue.task_done()
        except asyncio.QueueEmpty:
            pass

        # 2. Procesar datos de mercado (ticks masivos de !miniTicker@arr y !bookTicker)
        try:
            data = await asyncio.wait_for(market_queue.get(), timeout=0.1)
            
            # Identificar tipo de mensaje
            batch = []
            if isinstance(data, list): # miniTicker@arr
                batch = data
            elif isinstance(data, dict): # bookTicker individual
                if data.get('u') or 'b' in data: # Es un bookTicker
                    symbol = data['s']
                    book_tickers[symbol] = {
                        'bid': float(data['b']),
                        'ask': float(data['a'])
                    }
                    # También lo usamos como tick de precio para el buffer (usamos el mid price o ask)
                    mid_price = (float(data['b']) + float(data['a'])) / 2
                    if symbol in price_buffers:
                        price_buffers[symbol].add(mid_price, high=float(data['a']), low=float(data['b']))
                    continue

            for tick in batch:
                symbol = tick['s']
                
                # 1. Filtro rápido de mercados (Solo USDT y excluir "basura")
                if not symbol.endswith("USDT") or any(bad in symbol for bad in ["TRY", "EUR", "IDR", "GBP", "DAI", "RUB"]):
                    continue

                current_price = float(tick['c'])
                
                # 2. Inicializar buffer y recuperación SOLO si es relevante (Posición activa, Orden pendiente o Candidato)
                is_relevant = (
                    symbol in active_candidates or 
                    symbol in executor.active_positions or 
                    symbol in executor.pending_orders
                )

                if is_relevant:
                    if symbol not in price_buffers:
                        # 🛡️ Evitar Race Conditions en Recovery
                        if not hasattr(recovery, "recovery_started"):
                            recovery.recovery_started = set()
                        
                        if symbol not in recovery.recovery_started:
                            recovery.recovery_started.add(symbol)
                            price_buffers[symbol] = PriceBuffer(maxlen=100)
                            # Recuperación asíncrona para no bloquear el loop
                            task = asyncio.create_task(recovery.recover_symbol(symbol, price_buffers[symbol]))
                            system_state.task_registry.register(task, f"Recovery_{symbol}")

                    if symbol in price_buffers:
                        price_buffers[symbol].add(current_price)
                elif symbol in price_buffers:
                    # 🗑️ Limpieza: Si ya no es relevante, liberamos recursos
                    logger.info(f"Limpiando recursos para {symbol} (Ya no es relevante)")
                    del price_buffers[symbol]
                    if symbol in book_tickers: del book_tickers[symbol]
                    if symbol in last_analysis: del last_analysis[symbol]
                    # Desuscribirse del bookTicker
                    await ws_client.unsubscribe([f"{symbol.lower()}@bookTicker"])

                # A. Monitorear salidas de posiciones abiertas
                # Pasamos el ATR actual para el Trailing Stop dinámico
                current_atr = None
                if symbol in price_buffers:
                    indicators = price_buffers[symbol].get_indicators(update_atr=False) # No forzamos recalculo en cada tick
                    current_atr = indicators.get('atr_14')
                
                await executor.monitor_and_exit(symbol, current_price, atr=current_atr)

                # Si el símbolo está en Warmup, no evaluamos entradas
                if recovery.is_in_warmup(symbol):
                    continue

                # B. Evaluar entrada para candidatos filtrados por Macro
                if symbol in active_candidates and symbol not in executor.active_positions:
                    now = time.time()
                    # 1. DEBOUNCE TÁCTICO: Máximo un análisis cada 5 segundos
                    if now - last_analysis.get(symbol, 0) < 5:
                        continue
                    last_analysis[symbol] = now

                    if system_state.is_paused:
                        logger.debug(f"Ignorando entrada {symbol} (Sistema PAUSADO)")
                    else:
                        candidate = active_candidates[symbol]
                        indicators = price_buffers[symbol].get_indicators(update_atr=True)
                        
                        if indicators['atr_14'] is None:
                            continue

                        # 2. ATR RELATIVO (Uso de porcentaje en lugar de valor absoluto)
                        atr_rel = indicators['atr_14'] / current_price
                        
                        # 3. SCORE TÁCTICO
                        entry_score = 0
                        
                        # A. Momentum Sostenido (3-5 ticks)
                        prices_list = list(price_buffers[symbol].prices)
                        momentum_ok = False
                        if len(prices_list) >= 5:
                            momentum = (prices_list[-1] - prices_list[-5]) / prices_list[-5]
                            if momentum > 0.001: # Al menos 0.1% de momentum
                                entry_score += 20
                                # Confirmación multi-tick: 4 ticks consecutivos al alza
                                if (len(prices_list) >= 4 and 
                                    prices_list[-1] > prices_list[-2] > 
                                    prices_list[-3] > prices_list[-4]):
                                    entry_score += 20
                                    momentum_ok = True

                        # B. Spread real del libro
                        spread_ok = False
                        if symbol in book_tickers:
                            bt = book_tickers[symbol]
                            spread_pct = (bt['ask'] / bt['bid']) - 1
                            if spread_pct <= settings.MAX_SPREAD_PERCENT:
                                entry_score += 30
                                spread_ok = True
                                current_price = bt['ask'] # Compramos al ASK

                        # C. Volatilidad Saludable (ATR Relativo > MIN_ATR_RELATIVE)
                        if atr_rel >= settings.MIN_ATR_RELATIVE:
                            entry_score += 30

                        # D. Régimen de Volatilidad
                        regime = TA.detect_volatility_regime(prices_list, indicators['atr_14'])
                        if regime == "PANIC":
                            entry_score -= 50
                        elif regime == "EXPANSION":
                            entry_score += 10

                        # 4. DISPARO FINAL SI SCORE >= 70
                        if entry_score >= 70 and momentum_ok and spread_ok:
                            logger.success(f"🚀 GATILLO TÁCTICO: {symbol} Score: {entry_score} | ATR Rel: {atr_rel:.2%} | Price: {current_price}")
                            
                            await executor.try_buy(
                                symbol, 
                                current_price, 
                                candidate['score'], 
                                atr=indicators['atr_14'],
                                timestamp=tick.get('E')
                            )
                            # Una vez intentada la compra, lo quitamos de candidatos y desuscribimos si no es posición
                            del active_candidates[symbol]
                            if symbol not in executor.active_positions:
                                await ws_client.unsubscribe([f"{symbol.lower()}@bookTicker"])
                        else:
                            if price_buffers[symbol]._tick_count % 50 == 0:
                                logger.debug(f"Analizando {symbol}: Score {entry_score} (Mom: {momentum_ok}, Spr: {spread_ok})")

            market_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Error procesando tick: {e}")
