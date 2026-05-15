from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from core.config import settings
from models.trading import Base

# Crear motor asíncrono
if settings.SIMULATION_MODE:
    # SQLite in-memory para pruebas
    dsn = "sqlite+aiosqlite:///:memory:"
    engine = create_async_engine(dsn, echo=False)
else:
    # Asegurarse de que el DSN use postgresql+asyncpg://
    dsn = settings.DB_DSN
    if dsn.startswith("postgresql://"):
        dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20
    )

# Factory para sesiones asíncronas
async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def init_db():
    """
    Inicializa las tablas en la base de datos.
    """
    async with engine.begin() as conn:
        # En producción se recomienda usar Alembic
        await conn.run_sync(Base.metadata.create_all)

async def get_db():
    """
    Dependency para obtener una sesión de base de datos.
    """
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
