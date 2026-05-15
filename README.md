Arquitectura Maestro — Bot de Trading (V2.0)

Versión definitiva diseñada para:

Alta concurrencia
Baja latencia
Resiliencia en producción

👉 Eliminando cuellos de botella de concurrencia y latencia

🎯 OBJETIVO GENERAL

Crear un bot de trading:

Automatizado
Analítico
Resiliente
Prioridades del sistema:
Gestión de riesgo estricta (slots)
Uso eficiente y asíncrono de IA
Ejecución de alta frecuencia con WebSockets

👉 Objetivo:
Minimizar slippage y maximizar precisión en entornos de alta concurrencia

🧱 STACK TECNOLÓGICO (Alta Concurrencia)
Lenguaje: Python 3.11+
Gestor de entorno: uv
Bot Telegram: aiogram (async)
📡 Conexión al mercado:
REST: binance-connector
Streaming: websockets
🗄️ Persistencia:
PostgreSQL + asyncpg
⚡ Cache / Memoria:
Redis
Rate limits
Circuit breakers
Colas rápidas
⏱️ Scheduler:
APScheduler
Rotación
Scoring IA
Limpieza
📊 Logs:
JSON estructurado
Listo para observabilidad
⚙️ CONFIGURACIÓN (.env)
OPENROUTER_API_KEYS="key1,key2,key3"
MODELOS="modelo1,modelo2,modelo3"

MIN_TRADE_USD=8
MIN_MOVEMENT_PERCENT=5
MAX_ACTIVE_SLOTS=10
RISK_PER_TRADE=0.02

DB_DSN="postgresql://user:pass@localhost:5432/trading_db"

ALLOWED_TELEGRAM_IDS="123456789,987654321"
🔁 Circuit Breakers (Resiliencia)
Rotación automática de API keys y modelos
Bloqueo temporal en Redis ante:
Rate limit
Timeout
Rehabilitación con cooldown dinámico
🧠 SISTEMA DE MEMORIA Y ALMACENAMIENTO
🗄️ 1. Persistente (PostgreSQL)

Optimizado para concurrencia asíncrona:

📌 Tablas
Slots
estado (disponible / en_uso)
capital asignado
Posiciones
precio compra
cantidad
take profit dinámico
stop loss
slot asociado
Historial
operaciones cerradas
PnL
métricas
IA_Scores
decisiones históricas por activo
⚡ 2. Temporal (Redis / RAM)
Monedas candidatas del ciclo
Colas internas:
market_queue
strategy_queue
Estados de circuit breakers
⚡ ARQUITECTURA DESACOPLADA (ANTI-SLIPPAGE)

El sistema se divide en dos motores paralelos:

🧠 Motor 1: Estrategia Macro + IA

Frecuencia: cada 15 min / 1 hora

🔍 Flujo:
Pre-filtro duro
volumen suficiente
movimiento ≥ MIN_MOVEMENT_PERCENT
Consenso de IA
múltiples modelos analizan
modelo final consolida
Resultado
score (0–100)
✅ Condición:

👉 Activo operable si:
Score > 70

⚡ Motor 2: Ejecución Táctica (Tiempo Real)
WebSocket (tick-by-tick)
Indicadores:
RSI
EMA
Volatilidad
Soportes
🎯 Gatillo:

👉 IA valida + señal técnica → ejecución inmediata

📈 Gestión dinámica:
Stop Loss en tiempo real
Take Profit dinámico (trailing)
Sin depender de velas (latencia mínima)
💰 SISTEMA DE SLOTS Y RIESGO
🧱 Aislamiento de capital
Capital dividido en slots
1 operación = 1 slot
📊 Reglas:
Riesgo por trade: 1–2%
Stop Loss automático (OCO)
Take Profit:
dinámico
mínimo +0.5%
🚨 Kill Switch
Pausa automática tras N pérdidas consecutivas
Protección de drawdown
💎 DETECCIÓN DE OPORTUNIDADES ("JOYITAS")
Escaneo asíncrono de nuevos pares
📌 Filtros:
Market Cap
Liquidez
Volumen
⚠️ Restricción:
Máx. 5 candidatas activas
🎯 Condición de entrada:

👉 IA + confirmación técnica

📲 BOT DE TELEGRAM (UI)
🔐 Seguridad
Solo IDs permitidos (ALLOWED_TELEGRAM_IDS)
Interfaz:
lectura
control manual de emergencia
📌 Comandos
/start
/help
/status
/portfolio
/positions
/balance
/history
/analyze {symbol}
/on
/off
/pause
/panic
🔔 Notificaciones automáticas
Compras / ventas
Stop Loss
Alertas:
caída WebSocket
rotación de keys
errores críticos
🔐 SEGURIDAD Y HARDENING
🛡️ Medidas clave
Whitelist Telegram
Logs sanitizados (sin secretos)
VPS con firewall estricto
🔒 Infraestructura
DB y Redis no expuestos
comunicación interna únicamente
acceso SSH con llaves
🔑 API Binance
Solo Spot Trading
IP restringida
❌ Sin permisos de retiro

---

## 📦 ESTRUCTURA DEL PROYECTO

```text
/trading_bot
├── /bot                # aiogram handlers, middlewares (Whitelist auth), UI Telegram
├── /core               # Configuración (Pydantic settings), Logger estructurado, Excepciones
├── /infrastructure     # Adaptadores externos (Binance REST/WS, OpenRouter, DB Pool)
├── /trading            # Lógica de negocio core (Motor WS, TA, Slots, Risk Manager)
├── /ai                 # Orquestación de modelos, Circuit Breaker de APIs, Prompts
├── /models             # Entidades de base de datos (SQLAlchemy / Dataclasses)
└── main.py             # Entrypoint, asyncio.gather() para arrancar WebSockets, Bot y Scheduler
```
