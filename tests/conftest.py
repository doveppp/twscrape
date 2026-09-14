import os

import pytest

from dovexscrape import telemetry
from dovexscrape.account import Account
from dovexscrape.accounts_pool import AccountsPool
from dovexscrape.api import API
from dovexscrape.db import DB, execute
from dovexscrape.logger import set_log_level
from dovexscrape.queue_client import QueueClient, XClIdGenStore
from dovexscrape.ratelimit import rate_limiter

from .mock_http import MockClient

set_log_level("CRITICAL")
# the limiter is process-wide: disable the default 5 req/s so tests keep their timing,
# limiting behaviour itself is covered by test_ratelimit.py
rate_limiter.configure(None)


class ClIdGenMock:
    def calc(*args, **kwargs):
        return "mocked-clid"


@pytest.fixture(autouse=True)
def reset_telemetry():
    telemetry.reset()


@pytest.fixture(autouse=True)
def mock_xclidgenstore(monkeypatch):
    async def mock_get(*args, **kwargs):
        return ClIdGenMock()

    monkeypatch.setattr(XClIdGenStore, "get", classmethod(mock_get))


@pytest.fixture
def db_dsn():
    """MySQL url the tests run against, eg. mysql://user:pass@host:3306/db."""
    dsn = os.getenv("DOVEXSCRAPE_DB", "")
    if not dsn:
        pytest.skip("set DOVEXSCRAPE_DB (mysql://user:pass@host:3306/db) to run database tests")
    return dsn


@pytest.fixture
async def pool_mock(db_dsn):
    # tests own the database, so start from an empty schema; dropping the tables and
    # closing the pool makes the next connection re-run the migrations
    await execute(db_dsn, "DROP TABLE IF EXISTS accounts")
    await execute(db_dsn, "DROP TABLE IF EXISTS _meta")
    await DB.close_all()

    yield AccountsPool(db_dsn)

    await DB.close_all()


@pytest.fixture
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


@pytest.fixture
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
