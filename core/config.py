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
    MAX_SPREAD_PERCENT: float = 0.0010   # Spread máximo aceptable (0.10%) - Aumentado para Altcoins
    MAX_SLIPPAGE_PERCENT: float = 0.005  # Slippage máximo aceptable (0.50%)
    MAX_PRICE_AGE_MS: int = 1500         # Precio válido máximo 1.5 segundos (Tolerancia de latencia de red)
    STRATEGY_EVAL_INTERVAL: float = 0.25  # Evaluar estrategia máximo cada 250ms
    MARKET_QUEUE_MAXSIZE: int = 1000
    MACRO_SCAN_INTERVAL_MINUTES: int = 3  # ESCANEO DE MONEDAS 3 MINUTOS
    
    # Macro Advanced Filtering
    COOLDOWN_SPREAD_MINUTES: int = 15
    COOLDOWN_FLASH_CRASH_MINUTES: int = 120
    COOLDOWN_REJECT_MINUTES: int = 60
    
    # ML & Feature Engineering
    MIN_TECHNICAL_SCORE_AI: int = 65      # Reducido un poco para dar más peso al ML (antes 70)
    MAX_RSI_ENTRY: float = 75.0           # Evitar comprar en sobrecompra extrema
    MIN_VOLUME_M: float = 10.0            # Volumen 24h mínimo en millones
    
    # Advanced Trade Management
    TRAILING_STOP_ACTIVATION: float = 0.015   # Activar TS al 1.5% de ganancia
    TRAILING_STOP_CALLBACK: float = 0.005     # Callback de 0.5% (Se cierra si retrocede un 0.5% desde el pico)
    TRAILING_STOP_PERCENT: float = 0.005      # Distancia de trailing (usado si ATR no está disponible)
    TRAILING_STOP_ATR_MULT: float = 2.0       # Multiplicador ATR para el Stop Loss dinámico
    BREAKEVEN_TRIGGER_PERCENT: float = 0.008  # 0.8% trigger para asegurar breakeven (Más rápido en mercado DEAD)
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
