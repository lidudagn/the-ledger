"""
DB initialization script.
Reads DATABASE_URL from .env and executes src/schema.sql.
"""

import asyncio
import os
from pathlib import Path

import asyncpg
from dotenv import load_dotenv


async def init_db() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set in .env")
        return

    schema_path = Path(__file__).parent.parent / "src" / "schema.sql"
    schema_sql = schema_path.read_text()

    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(schema_sql)
        print("Schema applied successfully.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(init_db())
