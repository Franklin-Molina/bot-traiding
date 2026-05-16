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
        self.failure_counter = 0 
        self.max_failures = 3

    async def load_active_positions(self):
        """Carga posiciones abiertas de DB (Persistence Replay)."""
        async with async_session() as session:
            from sqlalchemy import select
            query = select(Position).where(Position.status == "OPEN")
            result = await session.execute(query)
            positions = result.scalars().all()
            for pos in positions:
                if not pos.highest_price:
                    pos.highest_price = pos.buy_price
                self.active_positions[pos.symbol] = pos
            if positions:
                logger.info(f"🔄 Persistence Replay: {len(positions)} posiciones activas recuperadas.")

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

    async def try_buy(self, symbol: str, price: float, score: int, atr: float = None, timestamp: int = None):
        """Intenta abrir posición con Order State Machine y Idempotencia."""
        start_perf = time.perf_counter()
        
        if price <= 0 or (atr is not None and atr < settings.MIN_ATR_THRESHOLD):
            return

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

            # Risk parameters
            sl_dist = (atr * 1.5) if atr else (price * settings.RISK_PER_TRADE)
            stop_loss = price - sl_dist
            take_profit = price + (sl_dist * 2)

            await SlotManager.lock_slot(slot.id)

            # 2. Ejecutar en Exchange con Idempotencia
            order_resp = await asyncio.wait_for(
                self.exchange.execute_market_buy(symbol, float(quantity), client_order_id=client_order_id),
                timeout=10
            )

            if order_resp:
                # 3. Procesar respuesta y Slippage
                fill_price = float(order_resp.get('price', price))
                if 'cummulativeQuoteQty' in order_resp and float(order_resp['executedQty']) > 0:
                    fill_price = float(order_resp['cummulativeQuoteQty']) / float(order_resp['executedQty'])
                
                slippage = ((fill_price / price) - 1) * 100
                latency = (time.perf_counter() - start_perf) * 1000

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
                
                logger.success(f"COMPRA FILLED: {symbol} @ {fill_price:.4f} (Slippage: {slippage:.2f}%)")
                await event_logger.log_event("POSITION_OPENED", symbol, {"price": fill_price, "slippage": slippage, "latency": latency})
                self._send_alert(f"✅ COMPRA: {symbol} a {fill_price:.4f} (Slip: {slippage:.2f}%)")
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

    async def monitor_and_exit(self, symbol: str, current_price: float):
        if symbol not in self.active_positions: return
        pos = self.active_positions[symbol]
        
        # Asegurar inicialización de highest_price
        if pos.highest_price is None:
            pos.highest_price = pos.buy_price

        # 1. Trailing Stop con Hysteresis
        # Solo actualizamos si el precio subió significativamente (ej. > 0.2% desde la última actualización de SL)
        if current_price > pos.highest_price:
            pos.highest_price = current_price
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
            await self.execute_exit(pos, current_price, exit_reason)

    async def execute_exit(self, pos: Position, expected_price: float, reason: str):
        symbol = pos.symbol

        # 🔴 evitar múltiples ejecuciones
        if getattr(pos, "closing", False):
            return
        pos.closing = True

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

            # 🔴 vender TODO usando balance real
            try:
                order_resp = await asyncio.wait_for(
                    self.exchange.execute_market_sell(symbol, None, client_order_id=client_order_id),
                    timeout=10
                )

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
