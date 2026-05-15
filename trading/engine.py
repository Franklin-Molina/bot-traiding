import asyncio
from loguru import logger
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
        asyncio.create_task(strategy_processor(market_queue, strategy_queue, alert_queue))
    ]
    
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error en el motor de trading: {e}")
    finally:
        ws_client.stop()
        for t in tasks:
            t.cancel()

async def strategy_processor(market_queue: asyncio.Queue, strategy_queue: asyncio.Queue, alert_queue: asyncio.Queue):
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
                strategy_queue.task_done()
        except asyncio.QueueEmpty:
            pass

        # 2. Procesar datos de mercado (ticks masivos de !miniTicker@arr)
        try:
            # miniTicker@arr envía una lista de tickers
            market_data_batch = await asyncio.wait_for(market_queue.get(), timeout=0.1)
            
            for tick in market_data_batch:
                symbol = tick['s']
                current_price = float(tick['c'])
                
                # Inicializar buffer y recuperación si es nuevo
                if symbol not in price_buffers:
                    price_buffers[symbol] = PriceBuffer(maxlen=100)
                    # Recuperación asíncrona para no bloquear el loop
                    task = asyncio.create_task(recovery.recover_symbol(symbol, price_buffers[symbol]))
                    system_state.task_registry.register(task, f"Recovery_{symbol}")

                price_buffers[symbol].add(current_price)

                # A. Monitorear salidas de posiciones abiertas
                await executor.monitor_and_exit(symbol, current_price)

                # Si el símbolo está en Warmup, no evaluamos entradas
                if recovery.is_in_warmup(symbol):
                    continue

                # B. Evaluar entrada para candidatos filtrados por Macro
                if symbol in active_candidates and symbol not in executor.active_positions:
                    if system_state.is_paused:
                        logger.debug(f"Ignorando entrada {symbol} (Sistema PAUSADO)")
                    else:
                        candidate = active_candidates[symbol]
                        indicators = price_buffers[symbol].get_indicators(update_atr=True)
                        regime = TA.detect_volatility_regime(list(price_buffers[symbol].prices), indicators['atr_14'])
                        
                        logger.info(f"Gatillo táctico para {symbol} a {current_price} (ATR: {indicators['atr_14']} | Regime: {regime})")
                        
                        # Riesgo Adaptativo: En regímenes de PANIC, ignoramos entradas nuevas
                        if regime == "PANIC":
                            logger.warning(f"Entrada ignorada para {symbol}: Régimen de PANIC detectado.")
                            continue

                        await executor.try_buy(
                            symbol, 
                            current_price, 
                            candidate['score'], 
                            atr=indicators['atr_14'],
                            timestamp=tick.get('E')
                        )
                        
                        # Una vez intentada la compra, lo quitamos de candidatos para no repetir
                        del active_candidates[symbol]

            market_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Error procesando tick: {e}")
