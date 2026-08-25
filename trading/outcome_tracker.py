import asyncio
import time
from datetime import datetime, UTC, timedelta
from loguru import logger
from sqlalchemy import select, update
from infrastructure.database import async_session
from models.trading import MLTrainingData
from infrastructure.binance_rest import get_binance_rest
from core.state import system_state

class OutcomeTracker:
    def __init__(self, interval_minutes: int = 15, timeout_minutes: int = 45):
        self.interval_minutes = interval_minutes
        self.timeout_minutes = timeout_minutes
        self.binance = get_binance_rest()  # ARQ-1: Singleton compartido
        self.is_running = False
        self._task = None
        
        # Triple Barrier thresholds
        self.tp_pct = 0.03   # +3%
        self.sl_pct = -0.01  # -1%

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._worker())
        logger.info(f"🔄 OutcomeTracker (Shadow Trades) iniciado (interval={self.interval_minutes}m)")

    async def stop(self):
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 OutcomeTracker detenido.")

    async def _worker(self):
        while self.is_running:
            try:
                await self._process_pending_shadows()
            except Exception as e:
                logger.error(f"Error en OutcomeTracker: {e}")
            
            await asyncio.sleep(self.interval_minutes * 60)

    async def _process_pending_shadows(self):
        now = datetime.now(UTC)
        cutoff_time = now - timedelta(minutes=self.timeout_minutes)

        async with async_session() as session:
            # Seleccionar shadows que ya pasaron su tiempo de maduración
            result = await session.execute(
                select(MLTrainingData)
                .where(MLTrainingData.trade_type == "SHADOW")
                .where(MLTrainingData.status == "PENDING")
                .where(MLTrainingData.entry_time <= cutoff_time)
            )
            shadows = result.scalars().all()

            if not shadows:
                return

            logger.info(f"🔍 Evaluando {len(shadows)} SHADOW TRADES pendientes (Triple Barrier)...")

            for shadow in shadows:
                try:
                    await self._evaluate_shadow(session, shadow)
                except Exception as e:
                    logger.error(f"Error evaluando shadow trade {shadow.trade_id}: {e}")
                
                # Respetar rate limits
                await asyncio.sleep(0.5)
                
            await session.commit()

    async def _evaluate_shadow(self, session, shadow: MLTrainingData):
        start_ts = int(shadow.entry_time.timestamp() * 1000)
        
        # Obtener klines desde la entrada hasta la salida esperada
        # Añadimos unos minutos extra por seguridad
        limit = self.timeout_minutes + 5 
        
        klines = await self.binance.get_klines(
            symbol=shadow.symbol,
            interval="1m",
            limit=limit,
            startTime=start_ts
        )

        if not klines:
            logger.warning(f"No klines para {shadow.symbol} desde {shadow.entry_time}")
            return

        entry_price = shadow.entry_price
        if not entry_price or entry_price <= 0:
            # Si por alguna razón falló el precio de entrada en macro.py, usamos la apertura de la primera vela
            entry_price = float(klines[0][1]) 

        mfe_pct = 0.0
        mae_pct = 0.0
        final_profit = 0.0
        target_class = 1 # Por defecto Break-even
        barrier_hit = "TIMEOUT"
        exit_time = shadow.entry_time + timedelta(minutes=self.timeout_minutes)

        highest_price = entry_price
        current_sl = entry_price * (1 + self.sl_pct)
        from core.config import settings

        for k in klines[:self.timeout_minutes]:
            high = float(k[2])
            low = float(k[3])
            close = float(k[4])
            kline_time = datetime.fromtimestamp(k[0] / 1000.0, UTC)
            
            # 1. Chequear Stop Loss (pesimista: asumimos que el low se alcanza primero)
            if low <= current_sl:
                barrier_hit = "TRAILING_STOP" if current_sl > entry_price else "STOP_LOSS"
                final_profit = (current_sl - entry_price) / entry_price
                exit_time = kline_time
                break
                
            # 2. Actualizar Highest Price y Trailing Stop
            if high > highest_price:
                highest_price = high
                # Simular Trailing Stop
                trailing_distance = highest_price * settings.TRAILING_STOP_PERCENT
                min_sl_dist = highest_price * 0.008
                max_sl_dist = highest_price * 0.05
                trailing_distance = max(min(trailing_distance, max_sl_dist), min_sl_dist)
                
                new_sl = highest_price - trailing_distance
                if new_sl > current_sl:
                    current_sl = new_sl
            
            # Calcular excursiones
            current_mfe = (highest_price - entry_price) / entry_price
            current_mae = (low - entry_price) / entry_price
            
            if current_mfe > mfe_pct: mfe_pct = current_mfe
            if current_mae < mae_pct: mae_pct = current_mae
            
            # Actualizar profit al cierre de la vela actual en caso de timeout
            final_profit = (close - entry_price) / entry_price

        # Calcular clase basada en final_profit
        if final_profit < -0.001:  # Menor a -0.1%
            target_class = 0
        elif final_profit <= 0.002: # Entre -0.1% y +0.2%
            target_class = 1
        elif final_profit <= 0.01:  # Entre +0.2% y +1.0%
            target_class = 2
        else:
            target_class = 3

        logger.debug(f"👻 SHADOW RES: {shadow.symbol} | Barrier: {barrier_hit} | MFE: {mfe_pct:.2%} | MAE: {mae_pct:.2%} | Class: {target_class}")

        update_values = {
            "profit_pct": final_profit,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "target_class": target_class,
            "status": "CLOSED",
            "exit_time": exit_time
        }
        
        # Si la fase macro no pudo grabar el precio de entrada, lo reescribimos con la realidad
        if not shadow.entry_price or shadow.entry_price <= 0:
            update_values["entry_price"] = entry_price

        # Update
        await session.execute(
            update(MLTrainingData)
            .where(MLTrainingData.trade_id == shadow.trade_id)
            .values(**update_values)
        )
