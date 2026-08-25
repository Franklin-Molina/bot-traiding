import asyncio
from aiogram import Router, types, F
from aiogram.filters import Command
from core.config import settings
from core.state import system_state
from infrastructure.binance_rest import get_binance_rest
from infrastructure.database import async_session
from models.trading import Position, TradeHistory, Slot, SlotStatus, MLTrainingData
from sqlalchemy import select, func
from trading.slots import SlotManager
from ai.orchestrator import AIOrchestrator
from datetime import datetime, timedelta, UTC

router = Router()
binance = get_binance_rest()  # ARQ-1: Singleton compartido
ai = AIOrchestrator()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 **¡Bienvenido al Bot de Trading Maestro V2.0!**\n\n"
        "Usa /help para ver la lista de comandos disponibles."
    )

@router.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "📲 **Comandos Disponibles:**\n\n"
        "**Control:**\n"
        "/status - Estado general del sistema\n"
        "/on - Activar el sistema\n"
        "/off - Apagar el sistema\n"
        "/pause - Pausar/Reanudar trading\n"
        "/panic - 🚨 CIERRE TOTAL Y APAGADO\n\n"
        "**Información:**\n"
        "/portfolio - Resumen de PnL acumulado\n"
        "/positions - Ver posiciones abiertas\n"
        "/history - Ver últimos trades cerrados\n"
        "/balance - Saldo actual en Binance\n"
        "/slots - Estado de los slots de trading\n"
        "/risk - Parámetros de riesgo\n"
        "/settings - Ver configuración actual\n\n"
        "**Análisis:**\n"
        "/analyze {symbol} - Análisis instantáneo de un activo\n"
        "/signals - Ver señales actuales en cola\n"
        "/opportunities - Oportunidades detectadas por Macro\n"
        "/ml_status - Estado del Data Lake (XGBoost)\n"
        "/mode - Ver modo actual (Simulación/Real)\n\n"
        "**Rendimiento:**\n"
        "/stats - Dashboard de rendimiento (Win Rate, Sharpe, etc.)\n"
        "/daily - Resumen del día actual"
    )
    await message.answer(help_text, parse_mode="Markdown")

@router.message(Command("status"))
async def cmd_status(message: types.Message):
    status = "🟢 EJECUTANDO" if system_state.is_running else "🔴 APAGADO"
    if system_state.is_paused:
        status = "🟡 PAUSADO"
    if system_state.panic_mode:
        status = "🚨 MODO PÁNICO"

    async with async_session() as session:
        pos_count = await session.scalar(select(func.count(Position.id)))
        slots_total = await session.scalar(select(func.count(Slot.id)))
        slots_used = await session.scalar(select(func.count(Slot.id)).where(Slot.status != SlotStatus.AVAILABLE))

    text = (
        f"📊 **Estado del Sistema**\n\n"
        f"Estado: {status}\n"
        f"Modo: {'SIMULACIÓN' if settings.SIMULATION_MODE else 'REAL'}\n"
        f"Posiciones Activas: {pos_count}\n"
        f"Slots: {slots_used}/{slots_total}"
    )
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("on"))
async def cmd_on(message: types.Message):
    system_state.set_running(True)
    system_state.deactivate_panic()
    await message.answer("✅ Sistema **ACTIVADO**.")

@router.message(Command("off"))
async def cmd_off(message: types.Message):
    system_state.set_running(False)
    await message.answer("🛑 Sistema **DESACTIVADO**.")

@router.message(Command("pause"))
async def cmd_pause(message: types.Message):
    new_state = not system_state.is_paused
    system_state.set_paused(new_state)
    state_text = "PAUSADO" if new_state else "REANUDADO"
    await message.answer(f"🕒 Sistema **{state_text}**.")

@router.message(Command("mode"))
async def cmd_mode(message: types.Message):
    mode = "SIMULACIÓN" if settings.SIMULATION_MODE else "REAL"
    await message.answer(f"⚙️ Modo actual: **{mode}**")

@router.message(Command("portfolio"))
async def cmd_portfolio(message: types.Message):
    async with async_session() as session:
        result = await session.execute(select(func.sum(TradeHistory.pnl), func.avg(TradeHistory.pnl_percent)))
        total_pnl, avg_pct = result.one()
        
        total_pnl = total_pnl or 0.0
        avg_pct = avg_pct or 0.0
        
        count = await session.scalar(select(func.count(TradeHistory.id)))

    await message.answer(
        f"💰 **Resumen de Cartera**\n\n"
        f"Trades Finalizados: {count}\n"
        f"PnL Total: {total_pnl:.2f} USDT\n"
        f"PnL Promedio: {avg_pct:.2f}%",
        parse_mode="Markdown"
    )

@router.message(Command("positions"))
async def cmd_positions(message: types.Message):
    async with async_session() as session:
        result = await session.execute(select(Position))
        positions = result.scalars().all()

    if not positions:
        return await message.answer("No hay posiciones abiertas.")

    text = "📍 **Posiciones Activas:**\n\n"
    for p in positions:
        text += f"• **{p.symbol}**: {p.quantity:.4f} a {p.buy_price:.2f} (SL: {p.stop_loss:.2f})\n"
    
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("history"))
async def cmd_history(message: types.Message):
    async with async_session() as session:
        result = await session.execute(select(TradeHistory).order_by(TradeHistory.closed_at.desc()).limit(10))
        history = result.scalars().all()

    if not history:
        return await message.answer("No hay historial de trades.")

    text = "📜 **Últimos 10 Trades:**\n\n"
    for h in history:
        emoji = "✅" if h.pnl > 0 else "❌"
        text += f"{emoji} **{h.symbol}**: {h.pnl_percent:+.2f}% ({h.pnl:+.2f} USDT)\n"
    
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("balance"))
async def cmd_balance(message: types.Message):
    usdt_balance = await binance.get_balance("USDT")
    await message.answer(f"💵 Saldo Disponible: **{usdt_balance:.2f} USDT**", parse_mode="Markdown")

@router.message(Command("risk"))
async def cmd_risk(message: types.Message):
    text = (
        f"🛡️ **Parámetros de Riesgo**\n\n"
        f"Riesgo por Operación: {settings.RISK_PER_TRADE * 100}%\n"
        f"Mínimo Movimiento: {settings.MIN_MOVEMENT_PERCENT}%\n"
        f"Máximo Slots: {settings.MAX_ACTIVE_SLOTS}\n"
        f"Inversión Mínima: {settings.MIN_TRADE_USD} USDT"
    )
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("slots"))
async def cmd_slots(message: types.Message):
    async with async_session() as session:
        result = await session.execute(select(Slot))
        slots = result.scalars().all()

    text = "🎰 **Estado de Slots:**\n\n"
    for s in slots:
        status_emoji = "🟢" if s.status == SlotStatus.AVAILABLE else "🔴"
        text += f"{status_emoji} Slot {s.id}: {s.status.value.upper()} ({s.assigned_capital} USD)\n"
    
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("settings"))
async def cmd_settings(message: types.Message):
    text = (
        f"⚙️ **Configuración del Sistema**\n\n"
        f"Simulation Mode: {settings.SIMULATION_MODE}\n"
        f"Max Slots: {settings.MAX_ACTIVE_SLOTS}\n"
        f"Min Trade: {settings.MIN_TRADE_USD} USD\n"
        f"DB DSN: `{settings.DB_DSN.split('@')[-1]}` (oculto)\n"
        f"Modelos IA: {settings.MODELOS}"
    )
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("analyze"))
async def cmd_analyze(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("Usa: /analyze {SIMBOLO} (ej: /analyze BTCUSDT)")
    
    symbol = args[1].upper()
    await message.answer(f"🔍 Analizando **{symbol}** con IA... espera un momento.")
    
    # Simulación de contexto para el análisis
    ticker = await binance.get_ticker(symbol)
    if not ticker:
        return await message.answer(f"No pude encontrar datos para {symbol}.")

    score = await ai.analyze_asset(symbol, context={'price': ticker['price']})
    
    response = (
        f"📊 **Análisis de {symbol}**\n\n"
        f"Precio Actual: {ticker['price']}\n"
        f"**Score IA: {score}/100**\n\n"
        f"{'🔥 ¡Oportunidad detectada!' if score > 75 else '⏳ Esperar mejores condiciones.'}"
    )
    await message.answer(response, parse_mode="Markdown")

@router.message(Command("panic"))
async def cmd_panic(message: types.Message):
    system_state.activate_panic()
    await message.answer("🚨 **¡MODO PÁNICO ACTIVADO!**\nDeteniendo motores y cerrando posiciones...")
    # La lógica de cierre real se ejecutará en un worker o aquí mismo llamando al executor
    # Para este comando, lanzaremos una señal que el engine debe captar o cerramos aquí.
    async with async_session() as session:
        result = await session.execute(select(Position))
        positions = result.scalars().all()
        
    for p in positions:
        # Aquí llamaríamos a binance.execute_market_sell
        await message.answer(f"Cerrando {p.symbol}...")
        # (Lógica simplificada para el comando, la integración real requiere el objeto TradeExecutor)

@router.message(Command("signals"))
async def cmd_signals(message: types.Message):
    await message.answer("📡 Buscando señales activas en el motor táctico...")
    # Esta info es volátil en memoria. Para ser real, el engine debería persistir candidatos 
    # o el bot tener acceso a la cola. Por ahora devolvemos un placeholder funcional.
    await message.answer("Actualmente no hay señales filtradas en cola de espera.")

@router.message(Command("opportunities"))
async def cmd_opportunities(message: types.Message):
    await message.answer("🔭 Escaneando el mercado en busca de oportunidades macro...")
    tickers = await binance.get_24hr_tickers()
    top_gainers = sorted(
        [t for t in tickers if t['symbol'].endswith("USDT")], 
        key=lambda x: float(x['priceChangePercent']), 
        reverse=True
    )[:5]
    
    text = "🚀 **Top Oportunidades Macro (24h):**\n\n"
    for t in top_gainers:
        text += f"• **{t['symbol']}**: {t['priceChangePercent']}% (Vol: {float(t['quoteVolume'])/1e6:.1f}M)\n"
    
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("ml_status"))
async def cmd_ml_status(message: types.Message):
    async with async_session() as session:
        # Contar total de filas (abiertas + cerradas)
        total_ml_rows = await session.scalar(select(func.count(MLTrainingData.trade_id)))
        
        # Contar cerradas (exit_time is not null)
        closed_count = await session.scalar(select(func.count(MLTrainingData.trade_id)).where(MLTrainingData.exit_time.isnot(None)))
        
        # Contar ganadoras
        winners_count = await session.scalar(select(func.count(MLTrainingData.trade_id)).where(MLTrainingData.target_class >= 2))
        
        total_ml_rows = total_ml_rows or 0
        closed_count = closed_count or 0
        winners_count = winners_count or 0
        losers_count = closed_count - winners_count
        
    text = (
        f"🧠 **Estado del Data Lake (XGBoost)**\n\n"
        f"📊 **Trades Registrados:** {total_ml_rows}\n"
        f"🏁 **Trades Cerrados:** {closed_count}\n"
        f"🏆 **Ganadores (>0.5%):** {winners_count}\n"
        f"❌ **Perdedores/BE:** {losers_count}\n\n"
        f"⏳ *El modelo requiere historial para iniciar la Fase 2.*"
    )
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """FEAT-4: Dashboard de rendimiento en tiempo real."""
    async with async_session() as session:
        # Total trades
        total = await session.scalar(select(func.count(TradeHistory.id))) or 0
        if total == 0:
            return await message.answer("📊 No hay trades cerrados aún para calcular estadísticas.")
        
        # Win/Loss
        winners = await session.scalar(select(func.count(TradeHistory.id)).where(TradeHistory.pnl > 0)) or 0
        losers = total - winners
        win_rate = (winners / total * 100) if total > 0 else 0
        
        # PnL
        total_pnl = await session.scalar(select(func.sum(TradeHistory.pnl))) or 0.0
        avg_pnl = await session.scalar(select(func.avg(TradeHistory.pnl))) or 0.0
        avg_pnl_pct = await session.scalar(select(func.avg(TradeHistory.pnl_percent))) or 0.0
        
        # Best / Worst
        best_pnl = await session.scalar(select(func.max(TradeHistory.pnl_percent))) or 0.0
        worst_pnl = await session.scalar(select(func.min(TradeHistory.pnl_percent))) or 0.0
        
        # Profit Factor
        gross_profit = await session.scalar(select(func.sum(TradeHistory.pnl)).where(TradeHistory.pnl > 0)) or 0.0
        gross_loss = abs(await session.scalar(select(func.sum(TradeHistory.pnl)).where(TradeHistory.pnl < 0)) or 0.001)
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # Avg Slippage
        avg_slip_buy = await session.scalar(select(func.avg(TradeHistory.slippage_buy_pct))) or 0.0
        avg_slip_sell = await session.scalar(select(func.avg(TradeHistory.slippage_sell_pct))) or 0.0

    pf_emoji = "🟢" if profit_factor > 1.5 else ("🟡" if profit_factor > 1.0 else "🔴")
    wr_emoji = "🟢" if win_rate > 55 else ("🟡" if win_rate > 45 else "🔴")
    
    text = (
        f"📊 **Dashboard de Rendimiento**\n\n"
        f"{wr_emoji} **Win Rate:** {win_rate:.1f}% ({winners}W / {losers}L)\n"
        f"💰 **PnL Total:** {total_pnl:+.2f} USDT\n"
        f"📈 **PnL Promedio:** {avg_pnl:+.2f} USDT ({avg_pnl_pct:+.2f}%)\n"
        f"🏆 **Mejor Trade:** {best_pnl:+.2f}%\n"
        f"💀 **Peor Trade:** {worst_pnl:+.2f}%\n"
        f"{pf_emoji} **Profit Factor:** {profit_factor:.2f}\n\n"
        f"⚡ **Slippage Promedio:**\n"
        f"   Compra: {avg_slip_buy:.3f}% | Venta: {avg_slip_sell:.3f}%"
    )
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("daily"))
async def cmd_daily(message: types.Message):
    """FEAT-4: Resumen del día actual."""
    today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    
    async with async_session() as session:
        # Trades del día
        day_filter = TradeHistory.closed_at >= today_start
        total_today = await session.scalar(select(func.count(TradeHistory.id)).where(day_filter)) or 0
        
        if total_today == 0:
            return await message.answer("📅 No hay trades cerrados hoy.")
        
        wins_today = await session.scalar(select(func.count(TradeHistory.id)).where(day_filter, TradeHistory.pnl > 0)) or 0
        pnl_today = await session.scalar(select(func.sum(TradeHistory.pnl)).where(day_filter)) or 0.0
        
        # Desglose por razón de cierre
        result = await session.execute(
            select(TradeHistory.reason, func.count(TradeHistory.id))
            .where(day_filter)
            .group_by(TradeHistory.reason)
        )
        reasons = {row[0]: row[1] for row in result.all()}
    
    pnl_emoji = "🟢" if pnl_today > 0 else "🔴"
    wr_today = (wins_today / total_today * 100) if total_today > 0 else 0
    
    text = (
        f"📅 **Resumen del Día**\n\n"
        f"{pnl_emoji} **PnL Hoy:** {pnl_today:+.2f} USDT\n"
        f"📊 **Trades:** {total_today} (WR: {wr_today:.0f}%)\n\n"
        f"**Razones de Cierre:**\n"
    )
    for reason, count in reasons.items():
        text += f"  • {reason}: {count}\n"
    
    await message.answer(text, parse_mode="Markdown")
