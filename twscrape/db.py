"""MySQL storage for account data.

The connection pool belongs to one :class:`DB` instance and therefore to
one event loop. ``AccountsPool`` owns such an instance; close it before leaving the
loop. The schema matches the former MySQL branch so existing databases keep working.
"""

import asyncio
import os
import warnings
from urllib.parse import parse_qs, unquote, urlparse

import asyncmy
from asyncmy.cursors import DictCursor
from asyncmy.pool import Pool

from .logger import logger

DEFAULT_DB_DSN = "mysql://root@127.0.0.1:3306/twscrape"


def default_db() -> str:
    """Resolve TWS_DB at call time, so setting it after import still works."""
    return os.getenv("TWS_DB", DEFAULT_DB_DSN)


POOL_MIN, POOL_MAX, POOL_RECYCLE = 1, 10, 3600


def _as_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


async def _migration_v1(cur):
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
async def _migration_v2(cur):
    """Add health columns to databases created before account health tracking."""
    await cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'"
    )
    existing = {x["COLUMN_NAME"] for x in await cur.fetchall()}
    columns = {
        "mfa_code": "VARCHAR(255) DEFAULT NULL",
        "xclid_fail_count": "INT NOT NULL DEFAULT 0",
        "unknown_fail_count": "INT NOT NULL DEFAULT 0",
        "global_locked_until": "DATETIME DEFAULT NULL",
    }
    for name, definition in columns.items():
        if name not in existing:
            await cur.execute(f"ALTER TABLE accounts ADD COLUMN {name} {definition}")


MIGRATIONS = [_migration_v1, _migration_v2]


async def migrate(pool: Pool) -> None:
    """Create or upgrade the schema, tracking the applied version in ``_meta``."""
    async with pool.acquire() as conn, conn.cursor() as cur:
        # Serialize first-use DDL across processes as well as local coroutines.
        await cur.execute(
            "SELECT GET_LOCK(CONCAT('twscrape:', LEFT(DATABASE(), 50)), 30) AS acquired"
        )
        row = await cur.fetchone()
        if not row or row["acquired"] != 1:
            raise RuntimeError("Could not acquire MySQL schema migration lock")

        try:
            await cur.execute(
                "SELECT 1 FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_meta'"
            )
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
        finally:
            await cur.execute("SELECT RELEASE_LOCK(CONCAT('twscrape:', LEFT(DATABASE(), 50)))")


async def _create_pool(db_path: str) -> Pool:
    p = urlparse(db_path)
    if p.scheme not in ("mysql", "mysql+asyncmy") or not p.path.strip("/"):
        raise ValueError(
            f"Invalid database url: {db_path!r}. Expected: mysql://user:pass@host:3306/db"
        )

    qs = {k: v[-1] for k, v in parse_qs(p.query).items()}
    return await asyncmy.create_pool(
        host=p.hostname or "127.0.0.1",
        port=p.port or 3306,
        user=unquote(p.username or ""),
        password=unquote(p.password or ""),
        db=unquote(p.path.lstrip("/")),
        charset=qs.get("charset", "utf8mb4"),
        cursor_cls=DictCursor,
        autocommit=True,
        # Timestamps are stored and compared as UTC throughout, so pin the session
        # time zone instead of trusting the server's SYSTEM time zone.
        init_command="SET time_zone = '+00:00'",
        minsize=_as_int(qs.get("pool_min"), POOL_MIN),
        maxsize=_as_int(qs.get("pool_max"), POOL_MAX),
        pool_recycle=_as_int(qs.get("pool_recycle"), POOL_RECYCLE),
    )


class DB:
    """One MySQL pool per DSN, created lazily and closed explicitly."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._pool: Pool | None = None
        self._lock = asyncio.Lock()

    async def pool(self) -> Pool:
        async with self._lock:
            if self._pool is None:
                pool = await _create_pool(self.db_path)
                try:
                    await migrate(pool)
                except BaseException:
                    pool.close()
                    await pool.wait_closed()
                    raise
                self._pool = pool
            return self._pool

    async def execute(self, qs: str, params: dict | None = None):
        pool = await self.pool()
        async with pool.acquire() as conn, conn.cursor() as cur:
            return await cur.execute(qs, params)

    async def fetchone(self, qs: str, params: dict | None = None) -> dict | None:
        pool = await self.pool()
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(qs, params)
            return await cur.fetchone()

    async def fetchall(self, qs: str, params: dict | None = None) -> list[dict]:
        pool = await self.pool()
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(qs, params)
            return await cur.fetchall()

    async def executemany(self, qs: str, params: list[dict]):
        pool = await self.pool()
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.executemany(qs, params)

    async def aclose(self):
        async with self._lock:
            pool, self._pool = self._pool, None
            if pool is not None:
                pool.close()
                await pool.wait_closed()


async def get_server_version(db_path: str) -> str:
    database = DB(db_path)
    try:
        row = await database.fetchone("SELECT VERSION() AS version")
        return str(row["version"]) if row else "unknown"
    finally:
        await database.aclose()
