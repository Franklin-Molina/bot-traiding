import asyncio
from core.config import settings
# Forzar modo simulación antes de importar infraestructura
settings.SIMULATION_MODE = False

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import infrastructure.database
import trading.slots
import trading.executor
import trading.reconciliation
import infrastructure.event_logger

from infrastructure.database import Base
from trading.executor import TradeExecutor
from infrastructure.paper_exchange import PaperExchange

@pytest_asyncio.fixture(scope="session")
def event_loop():
    """Crea un event loop para toda la sesión de tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """Motor de base de datos en memoria para tests."""
    # Usar el mismo engine que la infraestructura para consistencia
    engine = infrastructure.database.engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine

@pytest_asyncio.fixture
async def db_session(test_engine):
    """Sesión de base de datos limpia para cada test."""
    # Crear una única sesión para el test
    connection = await test_engine.connect()
    transaction = await connection.begin()
    session = AsyncSession(bind=connection, expire_on_commit=False)
    
    # Mock de la factoría para que devuelva SIEMPRE esta misma sesión
    # Forzamos que commit() sea flush() y close() sea no-op para tests
    original_commit = session.commit
    original_close = session.close
    
    async def mock_commit():
        await session.flush()
    
    async def mock_close():
        pass

    session.commit = mock_commit
    session.close = mock_close

    class SessionFactory:
        def __call__(self):
            return self
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
        def __getattr__(self, name):
            return getattr(session, name)

    mock_factory = SessionFactory()
    
    # Inyectar el mock en todos los módulos clave
    original_session = infrastructure.database.async_session
    infrastructure.database.async_session = mock_factory
    trading.slots.async_session = mock_factory
    trading.executor.async_session = mock_factory
    trading.reconciliation.async_session = mock_factory
    infrastructure.event_logger.async_session = mock_factory
    
    yield session
    
    # Cleanup
    await session.close()
    await transaction.rollback()
    await connection.close()
    
    # Restaurar
    infrastructure.database.async_session = original_session
    trading.slots.async_session = original_session
    trading.executor.async_session = original_session
    trading.reconciliation.async_session = original_session
    infrastructure.event_logger.async_session = original_session

@pytest.fixture
def mock_exchange():
    return PaperExchange()

@pytest_asyncio.fixture
async def alert_queue():
    return asyncio.Queue()

@pytest_asyncio.fixture
async def executor(alert_queue, mock_exchange):
    return TradeExecutor(alert_queue, exchange=mock_exchange)
