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
    MIN_TRADE_USD: float = 10.0
    MIN_MOVEMENT_PERCENT: float = 5.0
    MAX_ACTIVE_SLOTS: int = 5
    MAX_OPEN_POSITIONS: int = 5   # Alias para compatibilidad con main.py
    USDT_PER_SLOT: float = 6.0   # Capital asignado a cada operación
    RISK_PER_TRADE: float = 0.01  # 1% de riesgo inicial
    MIN_ATR_RELATIVE: float = 0.003 # 0.3% de volatilidad mínima
    MAX_SPREAD_PERCENT: float = 0.0015 # 0.15% de spread máximo
    MAX_SLIPPAGE_PERCENT: float = 0.005 # 0.5% de slippage máximo
    MAX_PRICE_AGE_MS: int = 2000
    TRAILING_STOP_ATR_MULT: float = 1.5 # Multiplicador ATR para Trailing Stop
    TRAILING_STOP_PERCENT: float = 0.015 # Fallback de 1.5% de trail
    
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
