import asyncio
import atexit
import warnings
from urllib.parse import parse_qs, urlparse

import asyncmy
from asyncmy.cursors import DictCursor
from asyncmy.pool import Pool

from .logger import logger

# Connection pool defaults, overridable per DSN, eg.
#   mysql://user:pass@host:3306/db?charset=utf8mb4&pool_min=1&pool_max=10&pool_recycle=3600
POOL_MIN, POOL_MAX, POOL_RECYCLE = 1, 10, 3600


def _as_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


async def _migration_v1(cur):
    """Create the accounts table with its indexes."""
    await cur.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            username VARCHAR(255) NOT NULL PRIMARY KEY,
            password TEXT NOT NULL,
            email VARCHAR(255) NOT NULL,
            email_password TEXT NOT NULL,
            user_agent TEXT NOT NULL,
            active TINYINT NOT NULL DEFAULT 0,
            locks JSON NOT NULL,
            stats JSON NOT NULL,
            headers JSON NOT NULL,
            cookies JSON NOT NULL,
            proxy TEXT DEFAULT NULL,
            error_msg TEXT DEFAULT NULL,
            last_used DATETIME DEFAULT NULL,
            _tx VARCHAR(64) DEFAULT NULL,
            mfa_code VARCHAR(255) DEFAULT NULL,
            xclid_fail_count INT NOT NULL DEFAULT 0,
            unknown_fail_count INT NOT NULL DEFAULT 0,
            global_locked_until DATETIME DEFAULT NULL,
            KEY idx_accounts_active (active),
            KEY idx_accounts_tx (_tx),
            KEY idx_accounts_global_lock (active, global_locked_until)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """
    )


# Ordered migrations, append new versions instead of editing old ones.
MIGRATIONS = [_migration_v1]


async def migrate(pool: Pool) -> None:
    """Create or upgrade the schema, tracking the applied version in `_meta`."""
    async with pool.acquire() as conn, conn.cursor() as cur:
        # Probe first: `CREATE TABLE IF NOT EXISTS` on an existing table reports a
        # server warning on every pool creation.
        await cur.execute("SHOW TABLES LIKE '_meta'")
        if await cur.fetchone() is None:
            await cur.execute(
                "CREATE TABLE _meta ("
                "`key` VARCHAR(50) NOT NULL PRIMARY KEY, "
                "`value` VARCHAR(255) NOT NULL"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
        await cur.execute("SELECT `value` FROM _meta WHERE `key` = 'user_version'")
        rs = await cur.fetchone()
        uv = int(rs["value"]) if rs else 0

        for i in range(uv + 1, len(MIGRATIONS) + 1):
            logger.info(f"Running migration to v{i}")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                await MIGRATIONS[i - 1](cur)
            await cur.execute(
                "INSERT INTO _meta (`key`, `value`) VALUES ('user_version', %(v)s) "
                "ON DUPLICATE KEY UPDATE `value` = %(v)s",
                {"v": i},
            )
            await conn.commit()


async def _create_pool(db_path: str) -> Pool:
    p = urlparse(db_path)
    if p.scheme not in ("mysql", "mysql+asyncmy"):
        raise ValueError(
            f"Invalid database url: {db_path!r}. Expected: mysql://user:pass@host:3306/db"
        )

    qs = {k: v[-1] for k, v in parse_qs(p.query).items()}
    return await asyncmy.create_pool(
        host=p.hostname or "127.0.0.1",
        port=p.port or 3306,
        user=p.username,
        password=p.password,
        db=p.path.lstrip("/"),
        charset=qs.get("charset", "utf8mb4"),
        cursor_cls=DictCursor,
        autocommit=True,
        # timestamps are stored/compared as UTC throughout (NOW(), FROM_UNIXTIME(),
        # lock_until values), so pin the session time zone instead of trusting the
        # server's SYSTEM time zone
        init_command="SET time_zone = '+00:00'",
        minsize=_as_int(qs.get("pool_min"), POOL_MIN),
        maxsize=_as_int(qs.get("pool_max"), POOL_MAX),
        pool_recycle=_as_int(qs.get("pool_recycle"), POOL_RECYCLE),
    )


class DBPool:
    """Singleton MySQL connection pool, one per DSN."""

    _pool: Pool | None = None
    _db_path: str | None = None
    _initialized = False
    _cleanup_registered = False
    _lock = asyncio.Lock()

    @classmethod
    def _sync_close(cls):
        """Best-effort close for the atexit hook (Pool.close is sync)."""
        if cls._pool is not None:
            try:
                cls._pool.close()
            except Exception:
                pass

    @classmethod
    async def close_all(cls):
        """Close the pool, so the next get_pool() reconnects and re-migrates."""
        pool, cls._pool, cls._initialized = cls._pool, None, False
        if pool is not None:
            try:
                pool.close()
                await pool.wait_closed()
            except Exception:
                pass

    @classmethod
    async def get_pool(cls, db_path: str) -> Pool:
        if not cls._cleanup_registered:
            atexit.register(cls._sync_close)
            cls._cleanup_registered = True

        # Serialize "create pool + migrate" so concurrent first calls share one pool.
        async with cls._lock:
            if cls._pool is None or cls._db_path != db_path:
                await cls.close_all()
                cls._pool = await _create_pool(db_path)
                cls._db_path = db_path

            if not cls._initialized:
                await migrate(cls._pool)
                cls._initialized = True

        return cls._pool


# Legacy name kept for callers that only need cleanup.
class DB:
    @classmethod
    async def close_all(cls):
        await DBPool.close_all()


async def execute(db_path: str, qs: str, params: dict | None = None):
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(qs, params)


async def fetchone(db_path: str, qs: str, params: dict | None = None) -> dict | None:
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(qs, params)
        return await cur.fetchone()


async def fetchall(db_path: str, qs: str, params: dict | None = None) -> list[dict]:
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(qs, params)
        return await cur.fetchall()


async def executemany(db_path: str, qs: str, params: list[dict]):
    pool = await DBPool.get_pool(db_path)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.executemany(qs, params)
