import asyncio
import os
import uuid
from urllib.parse import parse_qs, unquote, urlparse

import asyncmy
import pytest
import pytest_asyncio

from twscrape import telemetry
from twscrape.account import Account
from twscrape.accounts_pool import AccountsPool
from twscrape.api import API
from twscrape.logger import set_log_level
from twscrape.queue_client import QueueClient, XClIdGenStore
from twscrape.utils import rate_limiter

from .mock_http import MockClient

set_log_level("CRITICAL")
# the limiter is process-wide: disable the default 5 req/s so tests keep their timing,
# limiting behaviour itself is covered by test_ratelimit.py
rate_limiter.configure(None)

TEST_DSN_ENV = "TWS_TEST_DB"


class ClIdGenMock:
    def calc(*args, **kwargs):
        return "mocked-clid"


def _admin_options(dsn: str) -> dict:
    p = urlparse(dsn)
    if p.scheme not in ("mysql", "mysql+asyncmy"):
        raise ValueError(f"{TEST_DSN_ENV} must be a mysql:// DSN")
    qs = {k: v[-1] for k, v in parse_qs(p.query).items()}
    return {
        "host": p.hostname or "127.0.0.1",
        "port": p.port or 3306,
        "user": unquote(p.username or ""),
        "password": unquote(p.password or ""),
        "charset": qs.get("charset", "utf8mb4"),
        "autocommit": True,
        "init_command": "SET time_zone = '+00:00'",
    }


async def _admin_exec(dsn: str, sql: str):
    conn = await asyncmy.connect(**_admin_options(dsn))
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql)
    finally:
        conn.close()


@pytest.fixture
def db_dsn():
    """Disposable database per test; the path in TWS_TEST_DB is never touched."""
    base = os.getenv(TEST_DSN_ENV)
    if not base:
        pytest.skip(f"set {TEST_DSN_ENV} (mysql://user:pass@host:3306/db) to run tests")
    name = "twscrape_test_" + uuid.uuid4().hex[:16]
    asyncio.run(_admin_exec(base, f"CREATE DATABASE `{name}`"))
    p = urlparse(base)
    dsn = f"{p.scheme}://{p.netloc}/{name}"
    if p.query:
        dsn += "?" + p.query
    try:
        yield dsn
    finally:
        asyncio.run(_admin_exec(base, f"DROP DATABASE `{name}`"))


@pytest.fixture(autouse=True)
def reset_telemetry():
    telemetry.reset()


@pytest.fixture(autouse=True)
def reset_healed_features(monkeypatch):
    monkeypatch.setattr(QueueClient, "healed_features", {})


@pytest.fixture(autouse=True)
def mock_xclidgenstore(monkeypatch):
    async def mock_get(*args, **kwargs):
        return ClIdGenMock()

    monkeypatch.setattr(XClIdGenStore, "get", classmethod(mock_get))


@pytest_asyncio.fixture(loop_scope="function")
async def pool_mock(db_dsn):
    pool = AccountsPool(db_dsn)
    try:
        yield pool
    finally:
        await pool.aclose()


@pytest_asyncio.fixture(loop_scope="function")
async def client_fixture(pool_mock: AccountsPool, monkeypatch):
    mock_clt = MockClient()
    monkeypatch.setattr(Account, "make_client", lambda self, proxy=None: mock_clt)
    pool_mock._order_by = "username"

    for x in range(1, 3):
        await pool_mock.add_account(
            f"user{x}",
            f"pass{x}",
            f"email{x}",
            f"email_pass{x}",
            cookies=f"auth_token=token{x}; ct0=csrf{x}",
        )
        await pool_mock.set_active(f"user{x}", True)

    client = QueueClient(pool_mock, "SearchTimeline")
    yield pool_mock, client, mock_clt


@pytest_asyncio.fixture(loop_scope="function")
async def api_mock(pool_mock: AccountsPool):
    await pool_mock.add_account(
        "user1",
        "pass1",
        "email1",
        "email_pass1",
        cookies="auth_token=token1; ct0=csrf1",
    )
    await pool_mock.set_active("user1", True)

    api = API(pool_mock)
    yield api
