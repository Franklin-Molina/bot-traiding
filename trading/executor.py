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
        self.stop_loss_counts = {} # symbol: count
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
        momentum: float = 0,
        ml_features: dict = None
    ):
        start_perf = time.perf_counter()

        # 🔴 Validación Básica
        if price <= 0:
            logger.warning(f"Compra abortada para {symbol}: precio inválido")
            return

        atr_rel = (atr / price) if atr else 0.0

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

            sl_dist = atr * settings.TRAILING_STOP_ATR_MULT if atr else price * settings.TRAILING_STOP_PERCENT

            min_sl_dist = price * 0.006  # SL mínimo bajado a 0.6%
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
                    
                    # ============================
                    # DATA LAKE: T0 (Features)
                    # ============================
                    if ml_features:
                        from models.trading import MLTrainingData
                        ai_raw = ml_features.get("ai_raw", {})
                        ml_data = MLTrainingData(
                            trade_id=str(new_pos.id),
                            trade_type="REAL",
                            status="PENDING",
                            entry_price=fill_price,
                            symbol=symbol,
                            market_regime=ml_features.get("market_regime"),
                            tech_score=ml_features.get("tech_score"),
                            spread=ml_features.get("spread"),
                            momentum_15s=ml_features.get("momentum_15s"),
                            local_range_15s=ml_features.get("local_range_15s"),
                            ai_risk=ai_raw.get("risk", 0.0),
                            ai_manipulation=ai_raw.get("manipulation", 0.0),
                            ai_news=ai_raw.get("news_strength", 0.0),
                            ai_momentum=ai_raw.get("momentum", 0.0),
                            ai_confidence=ai_raw.get("confidence", 0.0)
                        )
                        session.add(ml_data)
                        await session.commit()

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
                trailing_distance = atr * settings.TRAILING_STOP_ATR_MULT
            else:
                trailing_distance = current_price * settings.TRAILING_STOP_PERCENT

            # ============================
            # FIX: CLAMP TRAILING DISTANCE
            # ============================
            min_sl_dist = current_price * 0.006  # SL mínimo de 0.6%
            max_sl_dist = current_price * 0.05
            
            trailing_distance = max(
                min(trailing_distance, max_sl_dist),
                min_sl_dist
            )

            new_sl = current_price - trailing_distance

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
        # BREAKEVEN MECHANISM
        # ============================
        pnl_pct = (current_price / pos.buy_price - 1) * 100
        
        # Breakeven escalonado
        if pnl_pct >= 2.5:
            breakeven_sl = pos.buy_price * (1 + 0.008) # +0.8%
        elif pnl_pct >= 1.5:
            breakeven_sl = pos.buy_price * (1 + 0.003) # +0.3%
        elif pnl_pct >= 1.0:
            breakeven_sl = pos.buy_price # Breakeven plano
        else:
            breakeven_sl = 0

        if breakeven_sl > pos.stop_loss:
            pos.stop_loss = breakeven_sl
            pos.last_sl_update_price = current_price
            logger.info(f"🛡️ BREAKEVEN ACTIVADO para {symbol}: SL movido a {breakeven_sl:.6f}")
            persistence_manager.enqueue_position_update(
                pos.id,
                {
                    "stop_loss": breakeven_sl,
                    "last_sl_update_price": current_price
                }
            )

        # ============================
        # TAKE PROFIT PARCIAL
        # ============================
        tp_level = getattr(pos, 'tp_level', 0)
        
        exit_reason = None
        
        if pnl_pct >= 4.0 and tp_level == 1:
            exit_reason = "PARTIAL_TP_2"
        elif pnl_pct >= 2.0 and tp_level == 0:
            exit_reason = "PARTIAL_TP_1"

        if current_price <= pos.stop_loss:

            if current_price > pos.buy_price:
                exit_reason = "TRAILING_STOP"
            else:
                exit_reason = "STOP_LOSS"

        # ============================
        # EST-6: TIMEOUT DE POSICIONES ESTANCADAS
        # ============================
        if exit_reason is None:
            minutes_in_trade = seconds_in_trade / 60
            pnl_ratio = (current_price / pos.buy_price) - 1
            if minutes_in_trade >= settings.MAX_POSITION_HOLD_MINUTES:
                if pnl_ratio < settings.MIN_PNL_TO_HOLD:
                    exit_reason = "TIMEOUT_STALE"
                    logger.warning(
                        f"⏰ TIMEOUT: {symbol} lleva {minutes_in_trade:.0f}min "
                        f"con PnL {pnl_ratio:.2%} < {settings.MIN_PNL_TO_HOLD:.2%}. Cerrando."
                    )

        # ============================
        # EJECUCIÓN DE SALIDA
        # ============================

        if exit_reason:

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
        is_partial = reason.startswith("PARTIAL_TP")
        if not is_partial:
            self.active_positions.pop(symbol, None)

        # 1. Validar Balance Real
        real_balance = await self._validate_exit_balance(symbol)
        if real_balance == 0:
            logger.warning(f"👻 Posición fantasma detectada: {symbol}. Limpiando.")
            await self._cleanup_ghost_position(pos)
            return
        elif real_balance < 0:
            pos.closing = False
            self.active_positions[symbol] = pos
            return
            
        USDT_POR_TP_PARCIAL = 5.5
        if expected_price > 0:
            qty_parcial = min(USDT_POR_TP_PARCIAL / expected_price, pos.quantity * 0.5)
        else:
            qty_parcial = pos.quantity * 0.33
            
        qty_to_sell = qty_parcial if is_partial else pos.quantity

        client_order_id = self._generate_client_id("sell")
        start_perf = time.perf_counter()

        await self._register_sell_order(pos, expected_price, client_order_id)
        order_resp = await self._send_sell_order(symbol, expected_price, qty_to_sell, client_order_id)

        if order_resp:
            if order_resp.get("status") == "INSUFFICIENT_BALANCE":
                if not is_partial:
                    await self.close_position_complete(pos, expected_price, "GHOST_CLEANUP", 0, 0, expected_price)
                return

            fill_price = float(order_resp.get('price', expected_price))
            if 'cummulativeQuoteQty' in order_resp and float(order_resp['executedQty']) > 0:
                fill_price = float(order_resp['cummulativeQuoteQty']) / float(order_resp['executedQty'])
            
            slippage = ((fill_price / expected_price) - 1) * 100 if expected_price > 0 else 0
            latency = (time.perf_counter() - start_perf) * 1000
            
            executed = float(order_resp.get('executedQty', qty_to_sell))
            
            if is_partial:
                pos.quantity -= executed
                if reason == "PARTIAL_TP_1": pos.tp_level = 1
                elif reason == "PARTIAL_TP_2": pos.tp_level = 2
                pos.closing = False
                
                async with async_session() as session:
                    from sqlalchemy import update
                    await session.execute(update(Position).where(Position.id == pos.id).values(quantity=pos.quantity, tp_level=pos.tp_level))
                    
                    history = TradeHistory(
                        symbol=pos.symbol, buy_price=pos.buy_price, sell_price=fill_price, quantity=executed,
                        pnl=(fill_price - pos.buy_price) * executed, pnl_percent=(fill_price / pos.buy_price - 1) * 100,
                        reason=reason, expected_sell_price=expected_price, slippage_sell_pct=slippage, latency_ms=latency
                    )
                    session.add(history)
                    await session.commit()
                    
                self._send_alert(f"💸 TAKE PROFIT PARCIAL ({reason}): {symbol} @ {fill_price:.6f}")
                logger.success(f"Take Profit Parcial ejecutado: {symbol} - Qty: {executed}")
            else:
                await self.close_position_complete(pos, fill_price, reason, slippage, latency, expected_price)
                
            await self._update_sell_order_filled(client_order_id, order_resp, fill_price, executed)
        else:
            logger.error(f"Fallo en ejecución de venta para {symbol}. Restaurando estado.")
            await self._update_sell_order_rejected(client_order_id)
            pos.closing = False 
            if not is_partial:
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
                self.exchange.execute_limit_ioc_sell(symbol, price, quantity, client_order_id=client_id, slippage_tolerance=settings.MAX_SLIPPAGE_PERCENT),
                timeout=10
            )
            
            executed_qty = float(order_resp.get('executedQty', 0)) if order_resp else 0
            is_expired = order_resp and order_resp.get('status') in ['EXPIRED', 'REJECTED']
            
            if not order_resp or (is_expired and executed_qty == 0):
                if order_resp and order_resp.get("status") == "INSUFFICIENT_BALANCE":
                    return order_resp
                    
                status_msg = order_resp.get('status') if order_resp else 'None'
                logger.warning(f"LIMIT IOC falló/expiró para {symbol} (Status: {status_msg}). Reintentando con MARKET.")
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
        pnl_usd = (sell_price - pos.buy_price) * pos.quantity
        pnl_pct = (sell_price / pos.buy_price - 1) * 100
        
        # 🛡️ Circuit Breaker: Actualizar PnL Diario y Semanal basado en CAPITAL TOTAL
        pnl_pct_total_capital = (pnl_usd / settings.TOTAL_CAPITAL_USD) * 100
        system_state.daily_pnl += pnl_pct_total_capital
        system_state.weekly_pnl += pnl_pct_total_capital
        
        if system_state.weekly_pnl <= system_state.max_weekly_loss_pct:
            logger.critical(f"🛑 CIRCUIT BREAKER SEMANAL ACTIVADO: Pérdida de {system_state.weekly_pnl:.2f}% excede límite.")
            system_state.set_paused(True)
            self._send_alert(f"🛑 EMERGENCY STOP: Sistema apagado por pérdida semanal de {system_state.weekly_pnl:.2f}%")
        elif system_state.daily_pnl <= system_state.max_daily_loss_pct:
            logger.critical(f"🛑 CIRCUIT BREAKER DIARIO ACTIVADO: Pérdida de {system_state.daily_pnl:.2f}% excede límite. Pausa de 4H.")
            system_state.emergency_stop_until = time.time() + (4 * 3600)
            self._send_alert(f"🛑 PAUSA 4 HORAS: Límite diario de pérdida alcanzado ({system_state.daily_pnl:.2f}%)")

        # Aplicar Cooldown de 15 minutos (900s) por defecto
        cooldown_time = 900
        
        if reason == "STOP_LOSS":
            self.stop_loss_counts[pos.symbol] = self.stop_loss_counts.get(pos.symbol, 0) + 1
            if self.stop_loss_counts[pos.symbol] >= 2:
                logger.warning(f"🚫 BLACKLIST ACTIVADO: {pos.symbol} tocó STOP_LOSS 2 veces. Bloqueado por 1 hora.")
                cooldown_time = 3600
                self.stop_loss_counts[pos.symbol] = 0 # Reset para la próxima
        
        self.cooldowns[pos.symbol] = time.time() + cooldown_time
        logger.info(f"Cooldown de {cooldown_time//60}min activado para {pos.symbol}")

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
            
            # ============================
            # DATA LAKE: T1 (Target)
            # ============================
            from models.trading import MLTrainingData
            from sqlalchemy import update
            from datetime import datetime, UTC
            
            if pnl_pct < -0.1:
                target_class = 0
            elif pnl_pct <= 0.2:
                target_class = 1
            elif pnl_pct <= 1.0:
                target_class = 2
            else:
                target_class = 3
            
            await session.execute(
                update(MLTrainingData)
                .where(MLTrainingData.trade_id == str(pos.id))
                .values(
                    profit_pct=pnl_pct,
                    target_class=target_class,
                    status="CLOSED",
                    exit_time=datetime.now(UTC)
                )
            )

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
