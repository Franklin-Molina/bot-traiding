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
    MIN_TRADE_USD: float = 7.0            # Margen seguro sobre mínimo Binance ($5)
    MIN_MOVEMENT_PERCENT: float = 0.5     # 0.5% — más realista para entradas frecuentes
    MAX_ACTIVE_SLOTS: int = 3             # Slots reservados simultáneamente
    MAX_OPEN_POSITIONS: int = 3           # Posiciones ejecutadas simultáneamente
    USDT_PER_SLOT: float = 6.0           # Capital por operación en USDT
    RISK_PER_TRADE: float = 0.025        # 2.5% del capital total como riesgo máximo por trade
    MIN_ATR_RELATIVE: float = 0.0010     # Volatilidad mínima (0.10%)
    MAX_SPREAD_PERCENT: float = 0.0015   # Spread máximo aceptable (0.15%)
    MAX_SLIPPAGE_PERCENT: float = 0.005  # Slippage máximo aceptable (0.50%)
    MAX_PRICE_AGE_MS: int = 2000         # Precio válido máximo 2 segundos
    # Trailing Stop: se usa ATR si hay datos suficientes, porcentaje como fallback
    TRAILING_STOP_ATR_MULT: float = 1.5  # Multiplicador ATR para trailing stop dinámico
    TRAILING_STOP_PERCENT: float = 0.020 # 2.0% trailing stop fijo (fallback sin ATR)
    
    
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
