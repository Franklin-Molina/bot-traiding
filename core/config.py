from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    # Simulation Mode
    SIMULATION_MODE: bool = False

    # Binance Keys
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # API Keys
    OPENROUTER_API_KEYS: str = "mock_key"
    MODELOS: str = "openai/gpt-4o,anthropic/claude-3-opus"
    
    # Trading Rules
   # Trading Rules
    MIN_TRADE_USD: float = 6.0            # Margen seguro sobre mínimo Binance ($5)
    MIN_MOVEMENT_PERCENT: float = 0.0015     # 0.15% — empuje realista para momentum de corto plazo
    MAX_ACTIVE_SLOTS: int = 2             # Slots reservados simultáneamente
    MAX_OPEN_POSITIONS: int = 2           # Posiciones ejecutadas simultáneamente
    USDT_PER_SLOT: float = 16.0          # Capital por operación en USDT (Aumentado para permitir parciales)
    TOTAL_CAPITAL_USD: float = 1000.0    # Capital total base para calcular drawdown global
    RISK_PER_TRADE: float = 0.025        # 2.5% del capital total como riesgo máximo por trade
    MIN_ATR_RELATIVE: float = 0.0020     # Volatilidad mínima (0.20%+)
    MAX_SPREAD_PERCENT: float = 0.0005   # Spread máximo aceptable (0.05%) - Estricto para scalping
    MAX_SLIPPAGE_PERCENT: float = 0.005  # Slippage máximo aceptable (0.50%)
    MAX_PRICE_AGE_MS: int = 1500         # Precio válido máximo 1.5 segundos (Tolerancia de latencia de red)
    STRATEGY_EVAL_INTERVAL: float = 0.25  # Evaluar estrategia máximo cada 250ms
    MARKET_QUEUE_MAXSIZE: int = 1000
    MACRO_SCAN_INTERVAL_MINUTES: int = 3  # ESCANEO DE MONEDAS 3 MINUTOS
    
    # Macro Advanced Filtering
    COOLDOWN_SPREAD_MINUTES: int = 15
    COOLDOWN_FLASH_CRASH_MINUTES: int = 120
    COOLDOWN_REJECT_MINUTES: int = 60
    MIN_TECHNICAL_SCORE_AI: int = 65
    
    # Trailing Stop: se usa ATR si hay datos suficientes, porcentaje como fallback
    TRAILING_STOP_ATR_MULT: float = 3.0  # Multiplicador ATR para trailing stop dinámico (reducido a 3.0)
    TRAILING_STOP_PERCENT: float = 0.012 # 1.2% trailing stop fijo (fallback sin ATR)
    MIN_TRAILING_STOP_PERCENT: float = 0.006 # 0.6% distancia mínima permitida para el trailing stop
    
    # Breakeven Mechanism
    BREAKEVEN_TRIGGER_PERCENT: float = 0.015  # 1.5% trigger para asegurar breakeven
    BREAKEVEN_PROFIT_PERCENT: float = 0.005   # Asegurar al menos +0.5% cuando se activa breakeven
    

    # Database
    DB_DSN: str = "postgresql://user:pass@localhost:5432/trading_db"
    
    # Telegram
    TELEGRAM_BOT_TOKEN: str = "mock_token"
    ALLOWED_TELEGRAM_IDS: str
    
    @property
    def telegram_ids_list(self) -> List[int]:
        return [int(id.strip()) for id in self.ALLOWED_TELEGRAM_IDS.split(",")]

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
