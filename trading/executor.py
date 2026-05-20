import asyncio
import math
import time
import uuid
from decimal import Decimal, ROUND_FLOOR
from loguru import logger
from datetime import datetime

from trading.slots import SlotManager
from trading.indicators import PriceBuffer
from infrastructure.exchange_interface import ExchangeInterface
from infrastructure.binance_rest import BinanceRest
from infrastructure.paper_exchange import PaperExchange
from infrastructure.database import async_session
from infrastructure.event_logger import event_logger
from models.trading import Position, TradeHistory, Order, OrderStatus
from core.config import settings
from core.state import system_state, HealthStatus

class TradeExecutor:
    def __init__(self, alert_queue: asyncio.Queue, exchange: ExchangeInterface = None):
        self.alert_queue = alert_queue
        if exchange:
            self.exchange = exchange
        else:
            self.exchange = PaperExchange() if settings.SIMULATION_MODE else BinanceRest()
            
        self.active_positions = {} # symbol: Position
        self.pending_orders = set() # symbol
        self.cooldowns = {} # symbol: end_timestamp
        self.failure_counter = 0 
        self.max_failures = 3

    async def load_active_positions(self):
        """Carga posiciones abiertas de DB y valida balance REAL en Binance (Reconciliación Total)."""
        async with async_session() as session:
            from sqlalchemy import select, delete
            query = select(Position).where(Position.status == "OPEN")
            result = await session.execute(query)
            db_positions = result.scalars().all()
            
            # 1. Limpiar posiciones de DB que no tienen balance en Binance
            for pos in db_positions:
                base_asset = pos.symbol.replace("USDT", "")
                try:
                    balance = await self.exchange.get_balance(base_asset)
                    # Si el balance es despreciable (ej. < 0.1 USDT en valor), lo consideramos 0
                    # Para simplificar, usamos > 0 si el asset existe
                    if balance <= 0:
                        logger.warning(f"👻 Reconciliación: Posición fantasma detectada: {pos.symbol}. Limpiando.")
                        await SlotManager.release_slot(pos.slot_id)
                        await session.execute(delete(Position).where(Position.id == pos.id))
                        continue
                    
                    if not pos.highest_price:
                        pos.highest_price = pos.buy_price
                    self.active_positions[pos.symbol] = pos
                except Exception as e:
                    logger.error(f"Error validando balance para {pos.symbol} al inicio: {e}")
                    # En caso de error de red, mantenemos la posición por seguridad
                    self.active_positions[pos.symbol] = pos
            
            await session.commit()
            if self.active_positions:
                logger.info(f"🔄 Persistence Replay: {len(self.active_positions)} posiciones reales sincronizadas.")

    def _calculate_quantity(self, capital: float, price: float, symbol_info: dict) -> Decimal:
        d_capital = Decimal(str(capital))
        d_price = Decimal(str(price))
        step_size = Decimal("0.000001")
        for f in symbol_info.get('filters', []):
            if f['filterType'] == 'LOT_SIZE':
                step_size = Decimal(f['stepSize'])
                break
        raw_qty = d_capital / d_price
        qty = (raw_qty / step_size).quantize(Decimal("1"), rounding=ROUND_FLOOR) * step_size
        return qty.normalize()

    def _validate_notional(self, quantity: Decimal, price: float, symbol_info: dict) -> bool:
        notional = quantity * Decimal(str(price))
        min_notional = Decimal("5.0")
        for f in symbol_info.get('filters', []):
            if f['filterType'] in ['NOTIONAL', 'MIN_NOTIONAL']:
                min_notional = Decimal(f.get('minNotional', f.get('notional', "5.0")))
                break
        return notional >= min_notional

    def _generate_client_id(self, prefix: str = "bot") -> str:
        return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:6]}"

    async def try_buy(self, symbol: str, price: float, score: int, atr: float = None, timestamp: int = None, momentum: float = 0):
        """Intenta abrir posición con Order State Machine y Idempotencia."""
        start_perf = time.perf_counter()
        
        # 🔴 BLOQUEO DE SEGURIDAD: No operar sin ATR o con ATR inválido
        if atr is None or price <= 0:
            logger.warning(f"Compra abortada para {symbol}: ATR o precio inválido")
            return

        # 1. ATR RELATIVO (En lugar de absoluto)
        atr_rel = atr / price
        if atr_rel < settings.MIN_ATR_RELATIVE:
            logger.warning(f"Compra abortada para {symbol}: Volatilidad insuficiente ({atr_rel:.2%})")
            return

        # 🕒 VERIFICACIÓN DE COOLDOWN
        now = time.time()
        if symbol in self.cooldowns:
            if now < self.cooldowns[symbol]:
                # Logger silencioso para cooldown
                return
            else:
                del self.cooldowns[symbol]

        if system_state.health not in [HealthStatus.HEALTHY, HealthStatus.RECOVERING]:
            return

        if symbol in self.pending_orders or symbol in self.active_positions:
            return

        # Validación Portfolio-Level Risk (Capacidad de slots)
        used_slots = await SlotManager.get_used_slots()
        total_slots = await SlotManager.get_total_slots()
        logger.info(f"Slots activos: {used_slots} / {total_slots}")
        logger.info(f"Active positions: {list(self.active_positions.keys())}")
        
        slot = await SlotManager.get_available_slot()
        if not slot:
            logger.warning(f"Portfolio Risk: Sin slots disponibles para {symbol}")
            return

        logger.info(f"Intentando BUY {symbol} | Active: {len(self.active_positions)}")

        self.pending_orders.add(symbol)
        client_order_id = self._generate_client_id("buy")
        
        try:
            symbol_info = await asyncio.wait_for(self.exchange.get_symbol_info(symbol), timeout=5)
            if not symbol_info: return

            quantity = self._calculate_quantity(slot.assigned_capital, price, symbol_info)
            if quantity <= 0 or not self._validate_notional(quantity, price, symbol_info):
                return

            # 1. Registrar Orden en DB (Estado PENDING/SUBMITTED) antes de enviar
            async with async_session() as session:
                db_order = Order(
                    client_order_id=client_order_id,
                    symbol=symbol,
                    side="BUY",
                    status=OrderStatus.SUBMITTED,
                    price=price,
                    quantity=float(quantity)
                )
                session.add(db_order)
                await session.commit()
                await event_logger.log_event("ORDER_SUBMITTED", symbol, {"cid": client_order_id, "price": price})

            # Risk parameters: Usar ATR para SL/TP dinámico
            # Según recomendación: SL = entry - (atr * 2.5), TP = entry + (atr * 4)
            sl_dist = atr * 2.5 
            
            # Asegurar un SL mínimo del 0.5% y máximo del 5% para evitar locuras
            min_sl_dist = price * 0.005
            max_sl_dist = price * 0.05
            sl_dist = max(min(sl_dist, max_sl_dist), min_sl_dist)

            stop_loss = price - sl_dist
            take_profit = price + (atr * 4.0) # RR mejorado basado en ATR

            await SlotManager.lock_slot(slot.id)

            # 2. Ejecutar en Exchange con Sniper Buy (Protección contra Slippage y Latencia)
            # slippage_tolerance = 0.001 (0.1%)
            order_resp = await asyncio.wait_for(
                self.exchange.execute_sniper_buy(
                    symbol, 
                    slot.assigned_capital, 
                    price, 
                    slippage_tolerance=0.001, 
                    client_order_id=client_order_id
                ),
                timeout=10
            )

            if order_resp:
                # 3. Procesar respuesta y Slippage
                fill_price = float(order_resp.get('price', price))
                if 'cummulativeQuoteQty' in order_resp and float(order_resp['executedQty']) > 0:
                    fill_price = float(order_resp['cummulativeQuoteQty']) / float(order_resp['executedQty'])
                
                slippage = ((fill_price / price) - 1) * 100
                latency = (time.perf_counter() - start_perf) * 1000

                # 🛡️ PROTECCIÓN DE SLIPPAGE (Máximo MAX_SLIPPAGE_PERCENT)
                if slippage > (settings.MAX_SLIPPAGE_PERCENT * 100):
                    logger.warning(f"⚠️ ALTO SLIPPAGE DETECTADO: {symbol} @ {slippage:.2f}%")
                    # Podríamos decidir salir inmediatamente, pero por ahora solo alertamos
                    # y dejamos que el sistema gestione la posición

                async with async_session() as session:
                    # Actualizar Orden
                    from sqlalchemy import update
                    await session.execute(update(Order).where(Order.client_order_id == client_order_id).values(
                        status=OrderStatus.FILLED,
                        fill_price=fill_price,
                        executed_quantity=float(order_resp.get('executedQty', quantity)),
                        exchange_order_id=str(order_resp.get('orderId'))
                    ))
                    
                    # Crear Posición
                    new_pos = Position(
                        symbol=symbol,
                        buy_price=fill_price, # Usamos el precio real
                        quantity=float(order_resp.get('executedQty', quantity)),
                        slot_id=slot.id,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        highest_price=fill_price,
                        last_sl_update_price=fill_price,
                        status="OPEN"
                    )
                    session.add(new_pos)
                    await session.commit()
                    await session.refresh(new_pos)
                    self.active_positions[symbol] = new_pos
                
                logger.success(f"COMPRA SNIPER FILLED: {symbol} @ {fill_price:.4f} (Slippage: {slippage:.2f}%)")
                await event_logger.log_event("POSITION_OPENED", symbol, {"price": fill_price, "slippage": slippage, "latency": latency, "momentum": momentum})
                self._send_alert(f"🎯 SNIPER BUY: {symbol} a {fill_price:.4f} (Slip: {slippage:.2f}%)")
                self.failure_counter = 0
            else:
                await self._handle_order_failure(client_order_id, symbol, slot.id)

        except Exception as e:
            logger.error(f"Error crítico en try_buy {symbol}: {e}")
            await self._handle_order_failure(client_order_id, symbol, slot.id)
        finally:
            self.pending_orders.discard(symbol)

    async def _handle_order_failure(self, client_order_id: str, symbol: str, slot_id: int):
        self.failure_counter += 1
        await SlotManager.release_slot(slot_id)
        async with async_session() as session:
            from sqlalchemy import update
            await session.execute(update(Order).where(Order.client_order_id == client_order_id).values(status=OrderStatus.REJECTED))
            await session.commit()
        if self.failure_counter >= self.max_failures:
            system_state.set_health(HealthStatus.DEGRADED)

    async def monitor_and_exit(self, symbol: str, current_price: float, atr: float = None):
        if symbol not in self.active_positions: return
        pos = self.active_positions[symbol]
        
        # 🕒 TIEMPO MÍNIMO DE PERMANENCIA (30 segundos)
        # Solo aplicable a salidas automáticas (SL/TP), no a pánico
        from datetime import datetime, UTC
        seconds_in_trade = (datetime.now(UTC) - pos.opened_at).total_seconds()
        
        # Asegurar inicialización de highest_price
        if pos.highest_price is None:
            pos.highest_price = pos.buy_price

        # 1. Trailing Stop con ATR Dinámico
        if current_price > pos.highest_price:
            pos.highest_price = current_price
            
            # Si tenemos ATR, usamos ATR * 1.5, si no, usamos el porcentaje por defecto
            if atr:
                new_sl = current_price - (atr * 1.5)
            else:
                new_sl = current_price * (1 - settings.TRAILING_STOP_PERCENT)
            
            # Umbral de Hysteresis: Evitar spam de DB si el cambio es ínfimo
            change_pct = (current_price / (pos.last_sl_update_price or pos.buy_price) - 1) * 100
            if new_sl > pos.stop_loss and change_pct > 0.1: # 0.1% threshold
                pos.stop_loss = new_sl
                pos.last_sl_update_price = current_price
                await self._update_position_sl(pos.id, new_sl, current_price, update_sl_price=True)
            else:
                # Persistir highest_price incluso si el SL no se mueve (Hysteresis)
                await self._update_position_sl(pos.id, pos.stop_loss, current_price, update_sl_price=False)

        # 2. Verificación de Salida
        exit_reason = None
        if current_price <= pos.stop_loss:
            exit_reason = "TRAILING_STOP" if current_price > pos.buy_price else "STOP_LOSS"
        elif current_price >= pos.take_profit:
            exit_reason = "TAKE_PROFIT"

        if exit_reason:
            # ⛔ Bloquear salida si no han pasado 30 segundos
            if seconds_in_trade < 30:
                # Logueamos pero no salimos (a menos que sea una caída catastrófica > 10% del SL)
                if current_price > pos.stop_loss * 0.9: 
                    logger.debug(f"Ignorando salida prematura para {symbol} ({seconds_in_trade:.1f}s en trade)")
                    return
                else:
                    exit_reason += "_CRITICAL"

            logger.info(f"🚀 SELL TRIGGER ({exit_reason}) -> Symbol: {symbol} | Price: {current_price} | SL: {pos.stop_loss:.6f} | TP: {pos.take_profit:.6f} | Time: {seconds_in_trade:.1f}s")
            await self.execute_exit(pos, current_price, exit_reason)

    async def execute_exit(self, pos: Position, expected_price: float, reason: str):
        symbol = pos.symbol

        # 🔴 evitar múltiples ejecuciones
        if getattr(pos, "closing", False):
            return
        pos.closing = True

        # 🔴 VALIDACIÓN DE BALANCE REAL ANTES DE VENDER
        base_asset = symbol.replace("USDT", "")
        try:
            real_balance = await self.exchange.get_balance(base_asset)
            if real_balance <= 0:
                logger.warning(f"👻 Posición fantasma detectada antes de vender: {symbol}. Limpiando internamente.")
                self.active_positions.pop(symbol, None)
                await SlotManager.release_slot(pos.slot_id)
                # También limpiar de la DB por si acaso
                async with async_session() as session:
                    from sqlalchemy import delete
                    await session.execute(delete(Position).where(Position.id == pos.id))
                    await session.commit()
                return
        except Exception as e:
            logger.error(f"Error verificando balance real para {symbol} antes de vender: {e}")
            # Continuamos con el intento de venta por si el error fue de red

        # 🔴 sacar de posiciones activas inmediatamente
        self.active_positions.pop(symbol, None)

        client_order_id = self._generate_client_id("sell")
        start_perf = time.perf_counter()

        try:
            async with async_session() as session:
                db_order = Order(
                    client_order_id=client_order_id,
                    symbol=symbol,
                    side="SELL",
                    status=OrderStatus.SUBMITTED,
                    price=expected_price,
                    quantity=pos.quantity
                )
                session.add(db_order)
                await session.commit()

            # 🔴 vender TODO usando balance real con LIMIT IOC
            try:
                # Intentamos vender al precio esperado para evitar barrer el libro
                order_resp = await asyncio.wait_for(
                    self.exchange.execute_limit_ioc_sell(symbol, expected_price, None, client_order_id=client_order_id),
                    timeout=10
                )

                # Si falla o no se llena totalmente (IOC), podríamos reintentar con MARKET como fallback de emergencia
                if not order_resp:
                    logger.warning(f"LIMIT IOC falló para {symbol}. Reintentando con MARKET por seguridad.")
                    order_resp = await asyncio.wait_for(
                        self.exchange.execute_market_sell(symbol, None, client_order_id=client_order_id),
                        timeout=10
                    )

                if order_resp and order_resp.get("status") == "INSUFFICIENT_BALANCE":
                    logger.warning(f"👻 Limpieza de POSICIÓN FANTASMA detectada para {symbol}. Cerrando en DB sin venta.")
                    await self.close_position_complete(pos, expected_price, "GHOST_CLEANUP", 0, 0, expected_price)
                    return

                if order_resp:
                    fill_price = float(order_resp.get('price', expected_price))
                    if 'cummulativeQuoteQty' in order_resp and float(order_resp['executedQty']) > 0:
                        fill_price = float(order_resp['cummulativeQuoteQty']) / float(order_resp['executedQty'])
                    
                    slippage = ((fill_price / expected_price) - 1) * 100 if expected_price > 0 else 0
                    latency = (time.perf_counter() - start_perf) * 1000
                    
                    await self.close_position_complete(pos, fill_price, reason, slippage, latency, expected_price)
                    
                    async with async_session() as session:
                        from sqlalchemy import update
                        await session.execute(update(Order).where(Order.client_order_id == client_order_id).values(
                            status=OrderStatus.FILLED,
                            fill_price=fill_price,
                            executed_quantity=float(order_resp.get('executedQty', pos.quantity)),
                            exchange_order_id=str(order_resp.get('orderId'))
                        ))
                        await session.commit()
                else:
                    logger.error(f"Fallo en ejecución de venta para {symbol} (posible balance insuficiente o error de filtros)")
                    async with async_session() as session:
                        from sqlalchemy import update
                        await session.execute(update(Order).where(Order.client_order_id == client_order_id).values(
                            status=OrderStatus.REJECTED
                        ))
                        await session.commit()
                    # Si falló la venta, debemos restaurar el estado para que el bot lo reintente o marque error
                    pos.closing = False 
                    self.active_positions[symbol] = pos

            except Exception as e:
                logger.error(f"Error en execute_exit para {symbol}: {e}")
                # En caso de excepción, intentamos marcar la orden como fallida
                try:
                    async with async_session() as session:
                        from sqlalchemy import update
                        await session.execute(update(Order).where(Order.client_order_id == client_order_id).values(
                            status=OrderStatus.REJECTED
                        ))
                        await session.commit()
                except:
                    pass
                pos.closing = False
                self.active_positions[symbol] = pos
        except Exception as outer_e:
            logger.error(f"Error crítico en estructura de execute_exit para {symbol}: {outer_e}")
            pos.closing = False
            self.active_positions[symbol] = pos

    async def close_position_complete(self, pos: Position, sell_price: float, reason: str, slippage: float, latency: float, expected_sell: float):
        pnl_pct = (sell_price / pos.buy_price - 1) * 100
        
        # 🛡️ Circuit Breaker: Actualizar PnL Diario
        system_state.daily_pnl += pnl_pct
        if system_state.daily_pnl <= system_state.max_daily_loss_pct:
            logger.critical(f"🛑 CIRCUIT BREAKER ACTIVADO: Pérdida diaria de {system_state.daily_pnl:.2f}% excede límite.")
            system_state.set_paused(True)
            self._send_alert(f"🛑 CIRCUIT BREAKER: Sistema pausado por pérdida diaria de {system_state.daily_pnl:.2f}%")

        # Aplicar Cooldown de 15 minutos (900s) al cerrar posición
        self.cooldowns[pos.symbol] = time.time() + 900
        logger.info(f"Cooldown de 15min activado para {pos.symbol}")

        async with async_session() as session:
            history = TradeHistory(
                symbol=pos.symbol,
                buy_price=pos.buy_price,
                sell_price=sell_price,
                quantity=pos.quantity,
                pnl=(sell_price - pos.buy_price) * pos.quantity,
                pnl_percent=pnl_pct,
                reason=reason,
                expected_sell_price=expected_sell,
                slippage_sell_pct=slippage,
                latency_ms=latency
            )
            session.add(history)
            await session.delete(await session.merge(pos))
            await session.commit()
            
        await SlotManager.release_slot(pos.slot_id)
        self.active_positions.pop(pos.symbol, None)
        await event_logger.log_event("POSITION_CLOSED", pos.symbol, {"pnl": pnl_pct, "reason": reason})
        self._send_alert(f"🚨 VENTA ({reason}): {pos.symbol} PnL: {pnl_pct:.2f}% | Slip: {slippage:.2f}%")

    async def _update_position_sl(self, pos_id: int, new_sl: float, highest: float, update_sl_price: bool = False):
        try:
            async with async_session() as session:
                from sqlalchemy import update
                values = {
                    "stop_loss": new_sl,
                    "highest_price": highest
                }
                if update_sl_price:
                    values["last_sl_update_price"] = highest
                
                await session.execute(update(Position).where(Position.id == pos_id).values(**values))
                await session.commit()
        except Exception as e:
            logger.error(f"Error persistiendo trailing stop: {e}")

    def _send_alert(self, message: str):
        try:
            self.alert_queue.put_nowait(message)
        except asyncio.QueueFull:
            pass

    async def close_all_positions(self):
        symbols = list(self.active_positions.keys())
        for symbol in symbols:
            pos = self.active_positions[symbol]
            await self.execute_exit(pos, pos.highest_price, "PANIC_EXIT")
