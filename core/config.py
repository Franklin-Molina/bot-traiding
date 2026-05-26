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
    MIN_MOVEMENT_PERCENT: float = 0.010     # 0.5% — más realista para entradas frecuentes
    MAX_ACTIVE_SLOTS: int = 3             # Slots reservados simultáneamente
    MAX_OPEN_POSITIONS: int = 3           # Posiciones ejecutadas simultáneamente
    USDT_PER_SLOT: float = 6.5          # Capital por operación en USDT
    RISK_PER_TRADE: float = 0.025        # 2.5% del capital total como riesgo máximo por trade
    MIN_ATR_RELATIVE: float = 0.0020     # Volatilidad mínima (0.20%+)
    MAX_SPREAD_PERCENT: float = 0.0004   # Spread máximo aceptable (0.04%) - Endurecido para evitar Whipsaws
    MAX_SLIPPAGE_PERCENT: float = 0.005  # Slippage máximo aceptable (0.50%)
    MAX_PRICE_AGE_MS: int = 500          # Precio válido máximo 0.5 segundos (HFT-lite)
    STRATEGY_EVAL_INTERVAL: float = 0.25  # Evaluar estrategia máximo cada 250ms
    MARKET_QUEUE_MAXSIZE: int = 1000
    MACRO_SCAN_INTERVAL_MINUTES: int = 5  # Intervalo de escaneo del Motor Macro (minutos)
    
    # Trailing Stop: se usa ATR si hay datos suficientes, porcentaje como fallback
    TRAILING_STOP_ATR_MULT: float = 3.5  # Multiplicador ATR para trailing stop dinámico (aumentado)
    TRAILING_STOP_PERCENT: float = 0.012 # 1.2% trailing stop fijo (fallback sin ATR)
    
    # Breakeven Mechanism
    BREAKEVEN_TRIGGER_PERCENT: float = 0.008  # 0.8% trigger para asegurar breakeven
    BREAKEVEN_PROFIT_PERCENT: float = 0.002   # Asegurar al menos +0.2% cuando se activa breakeven
    

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
