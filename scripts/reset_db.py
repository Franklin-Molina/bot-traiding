import asyncio
import sys
import os
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infrastructure.database import engine, Base, init_db

async def reset_database():
    try:
        print("Borrando todas las tablas...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        print("Tablas borradas. Recreando esquema...")
        await init_db()
        print("¡Esquema recreado exitosamente con las nuevas columnas!")
    except Exception as e:
        print(f"Error reseteando DB: {e}")

if __name__ == "__main__":
    asyncio.run(reset_database())
