import asyncio
from datetime import datetime, UTC

from loguru import logger
from sqlalchemy import select, update

from infrastructure.database import async_session
from infrastructure.exchange_interface import ExchangeInterface
from infrastructure.event_logger import event_logger
from models.trading import PositionStatus

from models.trading import (
    Order,
    OrderStatus,
    Position,
    PositionStatus
)

from trading.slots import SlotManager


class ReconciliationEngine:
    """
    Motor de Reconciliación:
    Sincroniza estados entre DB local y Exchange.
    """

    def __init__(self, exchange: ExchangeInterface, interval: int = 30):
        self.exchange = exchange
        self.interval = interval
        self.is_running = False
        self._task = None

    async def start(self):
        if self.is_running:
            logger.warning("ReconciliationEngine ya está corriendo.")
            return

        self.is_running = True
        self._task = asyncio.create_task(self._run_loop())

        logger.info(
            f"🔄 ReconciliationEngine iniciado "
            f"(interval={self.interval}s)"
        )

    async def stop(self):
        self.is_running = False

        if self._task:
            self._task.cancel()

            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("🛑 ReconciliationEngine detenido.")

    async def _run_loop(self):
        while self.is_running:

            try:
                await self.reconcile_orders()

            except asyncio.CancelledError:
                logger.warning("Loop cancelado.")
                break

            except Exception as e:
                logger.exception(
                    f"Error crítico en reconciliation loop: {e}"
                )

            await asyncio.sleep(self.interval)

    async def reconcile_orders(self):

        async with async_session() as session:

            try:
                query = select(Order).where(
                    Order.status.in_([
                        OrderStatus.PENDING,
                        OrderStatus.SUBMITTED,
                        OrderStatus.PARTIALLY_FILLED
                    ])
                )

                result = await session.execute(query)

                pending_orders = result.scalars().all()

                if not pending_orders:
                    logger.debug("No hay órdenes pendientes.")
                    return

                logger.info(
                    f"🔎 Reconciliando {len(pending_orders)} órdenes..."
                )

                for order in pending_orders:

                    try:
                        remote_order = await asyncio.wait_for(
                            self.exchange.get_order_status(
                                symbol=order.symbol,
                                client_order_id=order.client_order_id
                            ),
                            timeout=10
                        )

                        if not remote_order:
                            logger.warning(
                                f"Orden no encontrada: "
                                f"{order.client_order_id}"
                            )
                            continue

                        remote_status = remote_order.get("status")

                        new_status = self._map_status(remote_status)

                        if new_status == order.status:
                            continue

                        executed_qty = float(
                            remote_order.get("executedQty", 0)
                        )

                        fill_price = self._extract_fill_price(
                            remote_order,
                            executed_qty
                        )

                        logger.info(
                            f"📌 Sync {order.client_order_id}: "
                            f"{order.status} -> {new_status}"
                        )

                        await session.execute(
                            update(Order)
                            .where(Order.id == order.id)
                            .values(
                                status=new_status,
                                fill_price=fill_price,
                                executed_quantity=executed_qty,
                                exchange_order_id=str(
                                    remote_order.get("orderId")
                                ),
                                updated_at=datetime.now(UTC)
                            )
                        )

                        # importante para reflejar cambios inmediatamente
                        await session.flush()

                        # reconstrucción de posición perdida
                        if (
                            order.side == "BUY"
                            and new_status == OrderStatus.FILLED
                        ):
                            await self._ensure_position_exists(
                                session=session,
                                order=order,
                                fill_price=fill_price,
                                executed_qty=executed_qty
                            )

                        await event_logger.log_event(
                            "RECONCILIATION_SYNC",
                            order.symbol,
                            {
                                "client_order_id": order.client_order_id,
                                "status": new_status.value,
                                "fill_price": fill_price,
                                "executed_qty": executed_qty
                            }
                        )

                    except asyncio.TimeoutError:
                        logger.error(
                            f"Timeout reconciliando "
                            f"{order.client_order_id}"
                        )

                    except Exception as e:
                        logger.exception(
                            f"Error reconciliando "
                            f"{order.client_order_id}: {e}"
                        )

                await session.commit()

            except Exception as e:

                logger.exception(
                    f"Error global reconcile_orders: {e}"
                )

                await session.rollback()

    def _map_status(self, remote_status: str) -> OrderStatus:

        mapping = {
            "NEW": OrderStatus.SUBMITTED,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELLED,
            "PENDING_CANCEL": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED
        }

        return mapping.get(
            remote_status,
            OrderStatus.PENDING
        )

    def _extract_fill_price(
        self,
        remote_order: dict,
        executed_qty: float
    ) -> float:

        try:

            if executed_qty <= 0:
                return 0.0

            cumulative_quote = float(
                remote_order.get("cummulativeQuoteQty", 0)
            )

            if cumulative_quote > 0:
                return cumulative_quote / executed_qty

            return float(remote_order.get("price", 0))

        except Exception:
            return 0.0

    async def _ensure_position_exists(
        self,
        session,
        order,
        fill_price: float,
        executed_qty: float
    ):

        query = select(Position).where(
            Position.symbol == order.symbol,
            Position.status == PositionStatus.OPEN
        )

        result = await session.execute(query)

        existing_position = result.scalar_one_or_none()

        if existing_position:
            logger.debug(
                f"Posición ya existente para {order.symbol}"
            )
            return existing_position

        logger.warning(
            f"⚠️ Orden FILLED sin posición OPEN "
            f"para {order.symbol}"
        )

        slot = await SlotManager.get_available_slot()

        if not slot:
            logger.critical(
                f"❌ No hay slots disponibles "
                f"para recuperar {order.symbol}"
            )
            return None

        await SlotManager.lock_slot(slot.id)

        new_position = Position(
        symbol=order.symbol,
        buy_price=float(fill_price),
        quantity=float(executed_qty),
        slot_id=slot.id,

        status="OPEN",

        highest_price=float(fill_price),
        last_sl_update_price=float(fill_price),

        stop_loss=float(fill_price) * 0.98,
        take_profit=float(fill_price) * 1.05,

        opened_at=datetime.now(UTC)
        )

        session.add(new_position)

        # fuerza INSERT inmediato
        await session.flush()

        logger.success(
            f"✅ Posición reconstruida "
            f"{order.symbol} | slot={slot.id}"
        )

        return new_position