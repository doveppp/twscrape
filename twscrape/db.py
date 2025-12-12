import atexit
import warnings
from urllib.parse import urlparse

import aiomysql

from .logger import logger


async def migrate_mysql(pool):
    """Run database migrations"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS _meta (`key` VARCHAR(50) PRIMARY KEY, `value` VARCHAR(255))"
                )
            await cur.execute("SELECT value FROM _meta WHERE `key`='user_version'")
            rs = await cur.fetchone()
            uv = int(rs["value"]) if rs else 0

            async def v1():
                qs = """
                CREATE TABLE IF NOT EXISTS accounts (
                    username VARCHAR(255) PRIMARY KEY NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT NOT NULL,
                    email_password TEXT NOT NULL,
                    user_agent TEXT NOT NULL,
                    active TINYINT(1) DEFAULT 0 NOT NULL,
                    locks JSON NOT NULL,
                    headers JSON NOT NULL,
                    cookies JSON NOT NULL,
                    proxy TEXT DEFAULT NULL,
                    error_msg TEXT DEFAULT NULL
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"""
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    await cur.execute(qs)

            async def v2():
                await cur.execute("ALTER TABLE accounts ADD COLUMN stats JSON")
                await cur.execute("UPDATE accounts SET stats = '{}' WHERE stats IS NULL")
                await cur.execute(
                    "ALTER TABLE accounts ADD COLUMN last_used TIMESTAMP NULL DEFAULT NULL"
                )

            async def v3():
                await cur.execute("ALTER TABLE accounts ADD COLUMN _tx VARCHAR(255) DEFAULT NULL")

            async def v4():
                await cur.execute(
                    "ALTER TABLE accounts ADD COLUMN mfa_code VARCHAR(255) DEFAULT NULL"
                )

            migrations = {1: v1, 2: v2, 3: v3, 4: v4}

            for i in range(uv + 1, len(migrations) + 1):
                logger.info(f"Running migration to v{i}")
                await migrations[i]()
                if i == 2:
                    await cur.execute("UPDATE accounts SET stats = '{}' WHERE stats IS NULL")

                await cur.execute(
                    "INSERT INTO _meta (`key`, `value`) VALUES ('user_version', %s) ON DUPLICATE KEY UPDATE `value`=%s",
                    (i, i),
                )
                await conn.commit()


class DBPool:
    """Singleton MySQL connection pool"""

    _pool = None
    _db_path = None
    _initialized = False
    _cleanup_registered = False

    @classmethod
    def _sync_close(cls):
        """Synchronous cleanup for atexit hook"""
        if cls._pool:
            try:
                cls._pool.close()
            except Exception:
                pass

    @classmethod
    async def close_all(cls):
        """Async cleanup - properly closes pool"""
        if cls._pool:
            try:
                cls._pool.close()
                await cls._pool.wait_closed()
            except Exception:
                pass
        cls._pool = None
        cls._initialized = False

    @classmethod
    async def get_pool(cls, db_path: str):
        """Get or create the singleton connection pool"""
        # Register cleanup on first use
        if not cls._cleanup_registered:
            atexit.register(cls._sync_close)
            cls._cleanup_registered = True

        # Create pool if not exists or db_path changed
        if cls._pool is None or cls._db_path != db_path:
            # Close old pool if exists
            if cls._pool:
                await cls.close_all()

            p = urlparse(db_path)
            cls._pool = await aiomysql.create_pool(
                host=p.hostname,
                port=p.port or 3306,
                user=p.username,
                password=p.password,
                db=p.path.lstrip("/"),
                cursorclass=aiomysql.DictCursor,
                autocommit=True,
            )
            cls._db_path = db_path

            # Run migrations on first connection
            if not cls._initialized:
                await migrate_mysql(cls._pool)
                cls._initialized = True

        return cls._pool


# Legacy class for backward compatibility
class DB:
    @classmethod
    async def close_all(cls):
        await DBPool.close_all()


async def execute(db_path: str, qs: str, params: dict | None = None):
    """Execute a query without returning results"""
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(qs, params)


async def fetchone(db_path: str, qs: str, params: dict | None = None):
    """Execute a query and return one result"""
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(qs, params)
            return await cur.fetchone()


async def fetchall(db_path: str, qs: str, params: dict | None = None):
    """Execute a query and return all results"""
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(qs, params)
            return await cur.fetchall()


async def executemany(db_path: str, qs: str, params: list[dict]):
    """Execute a query with multiple parameter sets"""
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(qs, params)
