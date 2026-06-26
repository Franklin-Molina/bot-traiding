import asyncio
from enum import Enum
from loguru import logger

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    PANIC = "panic"
    SHUTDOWN = "shutdown"

class TaskRegistry:
    """
    Registro centralizado para auditar tareas asíncronas y detectar leaks.
    """
    def __init__(self):
        self._tasks = set()

    def register(self, task: asyncio.Task, name: str = "unnamed"):
        task.set_name(name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        logger.debug(f"Tarea registrada: {name}. Total: {len(self._tasks)}")

    def get_all_tasks(self):
        return list(self._tasks)

    @property
    def active_count(self):
        return len(self._tasks)

class SystemState:
    """
    Gestiona el estado global y de salud del bot.
    """
    def __init__(self):
        self._is_running = True
        self._is_paused = False
        self._panic_mode = False
        self._health = HealthStatus.HEALTHY
        self.ai_enabled = True
        self.task_registry = TaskRegistry()
        self.daily_pnl = 0.0
        self.max_daily_loss_pct = -5.0
        self.max_weekly_loss_pct = -10.0
        self.emergency_stop_until = 0.0
        self.invalidated_symbols = set()
        
        from datetime import datetime, UTC
        self._last_pnl_date = datetime.now(UTC).date()
        self._last_pnl_week = datetime.now(UTC).isocalendar()[1]

    @property
    def is_running(self):
        import time
        from datetime import datetime, UTC
        
        current_date = datetime.now(UTC).date()
        current_week = datetime.now(UTC).isocalendar()[1]
        
        if self._last_pnl_date != current_date:
            self.daily_pnl = 0.0
            self._last_pnl_date = current_date
            logger.info("📅 Reseteando Drawdown Diario a 0.0 (Nuevo día)")
            
        if self._last_pnl_week != current_week:
            self.weekly_pnl = 0.0
            self._last_pnl_week = current_week
            logger.info("📅 Reseteando Drawdown Semanal a 0.0 (Nueva semana)")

        if time.time() < self.emergency_stop_until:
            return False
        return self._is_running and not self._panic_mode

    @property
    def is_paused(self):
        return self._is_paused

    @property
    def panic_mode(self):
        return self._panic_mode

    @property
    def health(self):
        return self._health

    def set_health(self, status: HealthStatus):
        if self._health != status:
            logger.warning(f"Cambio de estado de salud: {self._health.value} -> {status.value}")
            self._health = status

    def set_running(self, value: bool):
        self._is_running = value

    def set_paused(self, value: bool):
        self._is_paused = value

    def activate_panic(self):
        self._health = HealthStatus.PANIC
        self._panic_mode = True
        self._is_running = False

    def deactivate_panic(self):
        self._health = HealthStatus.HEALTHY
        self._panic_mode = False
        self._is_running = True

    def invalidate_symbol_cache(self, symbol: str):
        """
        Marca un símbolo para que el Orquestador de IA invalide su caché 
        debido a anomalías tácticas graves (Flash crash, pump, etc).
        """
        self.invalidated_symbols.add(symbol)

# Instancia global
system_state = SystemState()
