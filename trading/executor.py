import asyncio
import math
import time
import uuid
from decimal import Decimal, ROUND_FLOOR
from loguru import logger
from datetime import datetime

from trading.slots import SlotManager
from trading.indicators import PriceBuffer
from trading.persistence import persistence_manager
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
        self.exit_queue = asyncio.Queue(maxsize=100)
        self._exit_worker_task = None
        self.symbol_cache = {}

    async def start(self):
        """Inicia trabajadores asíncronos del ejecutor."""
        self._exit_worker_task = asyncio.create_task(self._exit_worker())
        logger.info("✅ TradeExecutor Exit Worker iniciado.")

    async def stop(self):
        """Detiene trabajadores asíncronos."""
        if self._exit_worker_task:
            self._exit_worker_task.cancel()
            try:
                await self._exit_worker_task
            except asyncio.CancelledError:
                pass

    async def _exit_worker(self):
        """Worker que procesa las salidas de forma asíncrona y desacoplada."""
        while True:
            try:
                pos, expected_price, reason = await self.exit_queue.get()
                await self.execute_exit(pos, expected_price, reason)
                self.exit_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en ExitWorker: {e}")
                await asyncio.sleep(1)

    async def prepare_symbol(self, symbol: str):
        """Pre-carga la información del símbolo para ejecuciones zero-network."""
        if symbol not in self.symbol_cache:
            try:
                info = await self.exchange.get_symbol_info(symbol)
                if info:
                    self.symbol_cache[symbol] = info
                    logger.info(f"✅ Symbol Info cacheado para {symbol}")
            except Exception as e:
                logger.error(f"Error cacheando info para {symbol}: {e}")

    async def load_active_positions(self):
        """Carga posiciones abiertas y órdenes pendientes de DB y valida balance REAL."""
        async with async_session() as session:
            from sqlalchemy import select, delete
            
            # 1. Cargar Órdenes Pendientes para Idempotencia
            order_query = select(Order).where(Order.status.in_([OrderStatus.SUBMITTED, OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]))
            order_result = await session.execute(order_query)
            pending = order_result.scalars().all()
            for o in pending:
                self.pending_orders.add(o.symbol)
            if pending:
                logger.info(f"📋 {len(pending)} órdenes pendientes recuperadas de DB.")

            # 2. Cargar Posiciones
            query = select(Position).where(Position.status == "OPEN")
            result = await session.execute(query)
            db_positions = result.scalars().all()
            
            # 3. Limpiar posiciones de DB que no tienen balance en Binance
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

    # ============================
    # NUEVA LÓGICA MOMENTUM RIDING
    # SIN TAKE PROFIT FIJO
    # SOLO TRAILING DINÁMICO
    # ============================

    async def try_buy(
        self,
        symbol: str,
        price: float,
        score: int,
        atr: float = None,
        timestamp: int = None,
        momentum: float = 0
    ):
        start_perf = time.perf_counter()

        # 🔴 Validación ATR
        if atr is None or price <= 0:
            logger.warning(f"Compra abortada para {symbol}: ATR o precio inválido")
            return

        atr_rel = atr / price

        if atr_rel < settings.MIN_ATR_RELATIVE:
            logger.warning(
                f"Compra abortada para {symbol}: "
                f"Volatilidad insuficiente ({atr_rel:.2%})"
            )
            return

        # Cooldown
        now = time.time()

        if symbol in self.cooldowns:
            if now < self.cooldowns[symbol]:
                return
            else:
                del self.cooldowns[symbol]

        if system_state.health not in [
            HealthStatus.HEALTHY,
            HealthStatus.RECOVERING
        ]:
            return

        if symbol in self.pending_orders:
            return

        if symbol in self.active_positions:
            return

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

            symbol_info = self.symbol_cache.get(symbol)

            if not symbol_info:
                logger.warning(f"Symbol info no cacheado para {symbol}, abortando try_buy.")
                return

            quantity = self._calculate_quantity(
                slot.assigned_capital,
                price,
                symbol_info
            )

            if quantity <= 0:
                return

            if not self._validate_notional(quantity, price, symbol_info):
                return

            # Registrar orden
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

                await event_logger.log_event(
                    "ORDER_SUBMITTED",
                    symbol,
                    {
                        "cid": client_order_id,
                        "price": price
                    }
                )

            # ============================
            # STOP LOSS DINÁMICO ATR
            # ============================

            sl_dist = atr * 2.5

            min_sl_dist = price * 0.005
            max_sl_dist = price * 0.05

            sl_dist = max(
                min(sl_dist, max_sl_dist),
                min_sl_dist
            )

            stop_loss = price - sl_dist

            await SlotManager.lock_slot(slot.id)

            # ============================
            # SNIPER BUY
            # ============================

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

                fill_price = float(order_resp.get('price', price))

                if (
                    'cummulativeQuoteQty' in order_resp and
                    float(order_resp['executedQty']) > 0
                ):
                    fill_price = (
                        float(order_resp['cummulativeQuoteQty']) /
                        float(order_resp['executedQty'])
                    )

                slippage = ((fill_price / price) - 1) * 100

                latency = (
                    (time.perf_counter() - start_perf) * 1000
                )

                if slippage > (
                    settings.MAX_SLIPPAGE_PERCENT * 100
                ):
                    logger.warning(
                        f"⚠️ ALTO SLIPPAGE DETECTADO: "
                        f"{symbol} @ {slippage:.2f}%"
                    )

                # ============================
                # CREAR POSICIÓN
                # SIN TAKE PROFIT
                # ============================

                async with async_session() as session:

                    from sqlalchemy import update

                    await session.execute(
                        update(Order)
                        .where(Order.client_order_id == client_order_id)
                        .values(
                            status=OrderStatus.FILLED,
                            fill_price=fill_price,
                            executed_quantity=float(
                                order_resp.get(
                                    'executedQty',
                                    quantity
                                )
                            ),
                            exchange_order_id=str(
                                order_resp.get('orderId')
                            )
                        )
                    )

                    new_pos = Position(
                        symbol=symbol,
                        buy_price=fill_price,
                        quantity=float(
                            order_resp.get(
                                'executedQty',
                                quantity
                            )
                        ),
                        slot_id=slot.id,

                        # ============================
                        # SOLO STOP LOSS
                        # ============================

                        stop_loss=stop_loss,

                        # TP eliminado
                        take_profit=None,

                        highest_price=fill_price,
                        last_sl_update_price=fill_price,

                        status="OPEN"
                    )

                    session.add(new_pos)

                    await session.commit()

                    await session.refresh(new_pos)

                    self.active_positions[symbol] = new_pos

                logger.success(
                    f"COMPRA MOMENTUM FILLED: "
                    f"{symbol} @ {fill_price:.6f} "
                    f"(Slip: {slippage:.2f}%)"
                )

                await event_logger.log_event(
                    "POSITION_OPENED",
                    symbol,
                    {
                        "price": fill_price,
                        "slippage": slippage,
                        "latency": latency,
                        "momentum": momentum
                    }
                )

                self._send_alert(
                    f"🎯 MOMENTUM BUY: "
                    f"{symbol} @ {fill_price:.6f}"
                )

                self.failure_counter = 0

            else:
                await self._handle_order_failure(
                    client_order_id,
                    symbol,
                    slot.id
                )

        except Exception as e:

            logger.error(
                f"Error crítico en try_buy {symbol}: {e}"
            )

            await self._handle_order_failure(
                client_order_id,
                symbol,
                slot.id
            )

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

    def monitor_and_exit(self, symbol: str, current_price: float, atr: float = None ):

        if symbol not in self.active_positions:
            return

        pos = self.active_positions[symbol]

        from datetime import datetime, UTC

        seconds_in_trade = (
            datetime.now(UTC) - pos.opened_at
        ).total_seconds()

        # ============================
        # INIT
        # ============================

        if pos.highest_price is None:
            pos.highest_price = pos.buy_price

        # ============================
        # NUEVO MÁXIMO
        # ============================

        if current_price > pos.highest_price:

            pos.highest_price = current_price

            # ============================
            # TRAILING ATR
            # ============================

            if atr:

                trailing_distance = (
                    atr *
                    settings.TRAILING_STOP_ATR_MULT
                )

            else:

                trailing_distance = (
                    current_price *
                    settings.TRAILING_STOP_PERCENT
                )

            new_sl = (
                current_price -
                trailing_distance
            )

            change_pct = (
                (
                    current_price /
                    (pos.last_sl_update_price or pos.buy_price)
                ) - 1
            ) * 100

            # ============================
            # MOVER STOP SOLO HACIA ARRIBA
            # ============================

            if (
                new_sl > pos.stop_loss and
                change_pct > 0.1
            ):

                pos.stop_loss = new_sl

                pos.last_sl_update_price = current_price

                persistence_manager.enqueue_position_update(
                    pos.id,
                    {
                        "stop_loss": new_sl,
                        "highest_price": current_price,
                        "last_sl_update_price": current_price
                    }
                )

            else:

                persistence_manager.enqueue_position_update(
                    pos.id,
                    {
                        "highest_price": current_price
                    }
                )

        # ============================
        # SOLO TRAILING STOP
        # ============================

        exit_reason = None

        if current_price <= pos.stop_loss:

            if current_price > pos.buy_price:
                exit_reason = "TRAILING_STOP"
            else:
                exit_reason = "STOP_LOSS"

        # ============================
        # TIEMPO MÍNIMO
        # ============================

        if exit_reason:

            if seconds_in_trade < 30:

                if current_price > pos.stop_loss * 0.9:

                    logger.debug(
                        f"Ignorando salida prematura "
                        f"para {symbol} "
                        f"({seconds_in_trade:.1f}s)"
                    )

                    return

                else:
                    exit_reason += "_CRITICAL"

            if getattr(pos, "closing", False):
                return

            pos.closing = True

            logger.info(
                f"🚀 SELL TRIGGER ({exit_reason}) -> "
                f"{symbol} | "
                f"Price: {current_price:.6f} | "
                f"SL: {pos.stop_loss:.6f} | "
                f"HIGH: {pos.highest_price:.6f} | "
                f"Time: {seconds_in_trade:.1f}s"
            )

            try:

                self.exit_queue.put_nowait(
                    (
                        pos,
                        current_price,
                        exit_reason
                    )
                )

            except asyncio.QueueFull:

                logger.error(
                    f"Cola de salidas llena para {symbol}, desviando a background task"
                )

                asyncio.create_task(self.execute_exit(
                    pos,
                    current_price,
                    exit_reason
                ))

    async def execute_exit(self, pos: Position, expected_price: float, reason: str):
        symbol = pos.symbol
        # Sacar de posiciones activas inmediatamente para evitar procesamiento paralelo de ticks
        self.active_positions.pop(symbol, None)

        # 1. Validar Balance Real
        real_balance = await self._validate_exit_balance(symbol)
        if real_balance == 0:
            logger.warning(f"👻 Posición fantasma detectada: {symbol}. Limpiando.")
            await self._cleanup_ghost_position(pos)
            return
        elif real_balance < 0:
            # Error de red, restaurar para reintento
            pos.closing = False
            self.active_positions[symbol] = pos
            return

        client_order_id = self._generate_client_id("sell")
        start_perf = time.perf_counter()

        # 2. Registrar Orden en DB
        await self._register_sell_order(pos, expected_price, client_order_id)

        # 3. Ejecutar Venta
        order_resp = await self._send_sell_order(symbol, expected_price, pos.quantity, client_order_id)

        if order_resp:
            if order_resp.get("status") == "INSUFFICIENT_BALANCE":
                await self.close_position_complete(pos, expected_price, "GHOST_CLEANUP", 0, 0, expected_price)
                return

            fill_price = float(order_resp.get('price', expected_price))
            if 'cummulativeQuoteQty' in order_resp and float(order_resp['executedQty']) > 0:
                fill_price = float(order_resp['cummulativeQuoteQty']) / float(order_resp['executedQty'])
            
            slippage = ((fill_price / expected_price) - 1) * 100 if expected_price > 0 else 0
            latency = (time.perf_counter() - start_perf) * 1000
            
            await self.close_position_complete(pos, fill_price, reason, slippage, latency, expected_price)
            await self._update_sell_order_filled(client_order_id, order_resp, fill_price, pos.quantity)
        else:
            logger.error(f"Fallo en ejecución de venta para {symbol}. Restaurando estado.")
            await self._update_sell_order_rejected(client_order_id)
            pos.closing = False 
            self.active_positions[symbol] = pos

    async def _validate_exit_balance(self, symbol: str) -> float:
        base_asset = symbol.replace("USDT", "")
        try:
            return await self.exchange.get_balance(base_asset)
        except Exception as e:
            logger.error(f"Error verificando balance real para {symbol}: {e}")
            return -1.0

    async def _cleanup_ghost_position(self, pos: Position):
        await SlotManager.release_slot(pos.slot_id)
        async with async_session() as session:
            from sqlalchemy import delete
            await session.execute(delete(Position).where(Position.id == pos.id))
            await session.commit()

    async def _register_sell_order(self, pos: Position, price: float, client_id: str):
        async with async_session() as session:
            db_order = Order(
                client_order_id=client_id,
                symbol=pos.symbol,
                side="SELL",
                status=OrderStatus.SUBMITTED,
                price=price,
                quantity=pos.quantity
            )
            session.add(db_order)
            await session.commit()

    async def _send_sell_order(self, symbol: str, price: float, quantity: float, client_id: str) -> dict:
        try:
            order_resp = await asyncio.wait_for(
                self.exchange.execute_limit_ioc_sell(symbol, price, quantity, client_order_id=client_id),
                timeout=10
            )
            if not order_resp:
                logger.warning(f"LIMIT IOC falló para {symbol}. Reintentando con MARKET.")
                order_resp = await asyncio.wait_for(
                    self.exchange.execute_market_sell(symbol, quantity, client_order_id=client_id),
                    timeout=10
                )
            return order_resp
        except Exception as e:
            logger.error(f"Error enviando orden de venta para {symbol}: {e}")
            return None

    async def _update_sell_order_filled(self, client_id: str, resp: dict, fill_price: float, qty: float):
        async with async_session() as session:
            from sqlalchemy import update
            await session.execute(update(Order).where(Order.client_order_id == client_id).values(
                status=OrderStatus.FILLED,
                fill_price=fill_price,
                executed_quantity=float(resp.get('executedQty', qty)),
                exchange_order_id=str(resp.get('orderId'))
            ))
            await session.commit()

    async def _update_sell_order_rejected(self, client_id: str):
        async with async_session() as session:
            from sqlalchemy import update
            await session.execute(update(Order).where(Order.client_order_id == client_id).values(status=OrderStatus.REJECTED))
            await session.commit()

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
