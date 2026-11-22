"""DB-free tests for the SearchTimeline account check inside QueueClient.req().

A tiny in-memory pool stands in for AccountsPool, so these run without MySQL.
Known route failures are an empty 404 (served by Ctx.req when the check is
active) or a 200 "Internal server error" with no data.
"""

import asyncio
from typing import cast
from unittest.mock import AsyncMock

import pytest

import twscrape.queue_client as queue_client_module
from twscrape.account import Account
from twscrape.accounts_pool import AccountsPool
from twscrape.http import HttpMethod, Response
from twscrape.queue_client import AbortReqError, ApiError, QueueClient

from .mock_http import MockClient

URL = "https://example.com/api"
PARAMS = {"variables": {"rawQuery": "needle", "count": 20, "cursor": "abc"}, "features": "{}"}
SEARCH_404 = ("response", 404, {}, "", None)
SEARCH_OK = ("response", 200, {"data": {"search_by_raw_query": {"x": [1]}}}, "", None)
ROUTE_FAILED = (
    "response",
    200,
    {"errors": [{"code": -1, "message": "Internal server error"}]},
    "",
    None,
)
# What X actually sends for an account-level search restriction: the error plus a
# non-empty data envelope whose timeline holds only cursors.
ROUTE_FAILED_EMPTY_TIMELINE = (
    "response",
    200,
    {
        "data": {
            "search_by_raw_query": {
                "search_timeline": {
                    "timeline": {
                        "instructions": [
                            {
                                "type": "TimelineAddEntries",
                                "entries": [
                                    {"entryId": "cursor-bottom-1234567890"},
                                    {"entryId": "messageprompt-follow"},
                                ],
                            }
                        ]
                    }
                }
            }
        },
        "errors": [{"code": -1, "message": "Internal server error"}],
    },
    "",
    None,
)

POSITIVE = {"updated": False, "can_search": True, "reason": "ok"}
NEGATIVE = {"updated": True, "can_search": False, "reason": "search_internal_error"}
INCONCLUSIVE = {"updated": False, "can_search": None, "reason": "empty_search"}


class Recorded(MockClient):
    """MockClient that records kwargs; ``hold`` hangs from the second request on."""

    def __init__(self, *, hold: bool = False):
        super().__init__()
        self.calls: list[dict] = []
        self.hold = hold
        self.started = asyncio.Event()

    def add(self, *items: tuple) -> "Recorded":
        self._queue.extend(items)
        return self

    async def request(self, method: HttpMethod, url: str, **kwargs) -> Response:
        self.calls.append(kwargs)
        if self.hold and len(self.calls) > 1:
            self.started.set()
            await asyncio.Event().wait()
        return await super().request(method, url, **kwargs)


class FakePool:
    """The pool calls QueueClient makes, minus any persistence.

    ``sticky`` hands the same account back until the check marks it unsearchable.
    """

    def __init__(self, accounts: list[Account], *, sticky: bool = False):
        self.accounts = accounts
        self.sticky = sticky
        self.handed: set[str] = set()
        self.leases: set[str] = set()

    async def get_for_queue_or_wait(self, queue: str) -> Account | None:
        for account in self.accounts:
            if not account.active or account.username in self.handed:
                continue
            if account.locks.get(queue) is not None:
                continue
            if not self.sticky:
                self.handed.add(account.username)
            return account
        return None

    async def unlock(self, username: str, queue: str, req_count=0, tx=None):
        self.leases.discard(username)

    async def lock_until(self, username, queue, unlock_at, req_count=0, tx=None, **kw):
        self.leases.discard(username)

    async def incr_fail_count(self, username: str, kind: str) -> int:
        return 1  # below the deactivate threshold: rotate with a lease lock

    async def reset_fail_count(self, username: str, kind: str):
        pass


def make_account(username="user1") -> Account:
    return Account(
        username=username,
        password="_",
        email="",
        email_password="",
        user_agent="@chrome",
        active=True,
        cookies={"auth_token": "token", "ct0": "csrf"},
    )


def make_client(monkeypatch, pool: FakePool, callback, hold=False):
    mock = Recorded(hold=hold)
    monkeypatch.setattr(Account, "make_client", lambda self, proxy=None: mock)
    monkeypatch.setattr(queue_client_module.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(queue_client_module.XClIdGenStore, "items", {})

    client = QueueClient(cast(AccountsPool, pool), "SearchTimeline", search_check=callback)
    return client, mock


def recorder(result=POSITIVE, error: Exception | None = None):
    calls: list[str] = []

    async def callback(username):
        calls.append(username)
        if error:
            raise error
        return dict(result)

    return callback, calls


def assert_same_request(mock: Recorded):
    """The replay resends the very same params object."""
    assert len(mock.calls) == 2
    assert mock.calls[0]["params"] is mock.calls[1]["params"] is PARAMS


async def test_empty_404_positive_replays_same_request(monkeypatch):
    callback, calls = recorder()
    pool = FakePool([make_account()])
    client, mock = make_client(monkeypatch, pool, callback)
    mock.add(SEARCH_404, SEARCH_OK)

    rep = await client.get(URL, params=PARAMS)

    assert isinstance(rep, Response)
    assert getattr(rep, "__username") == "user1"
    assert calls == ["user1"]  # one 404, one probe, no xclid retry loop
    assert_same_request(mock)
    assert mock.calls[1]["params"]["variables"]["cursor"] == "abc"
    assert pool.accounts[0].xclid_fail_count == 0


class FakeGen:
    def calc(self, *args, **kwargs):
        return "clid"


async def test_positive_recovery_pops_stale_signing_key(monkeypatch):
    callback, _ = recorder()
    client, mock = make_client(monkeypatch, FakePool([make_account()]), callback)
    stale, fresh = FakeGen(), FakeGen()
    items: dict = {"user1": stale}
    gens = iter([stale, fresh])

    async def fake_get(cls, username, proxy=None, cookies=None, fresh=False):
        gen = next(gens)
        items[username] = gen
        return gen

    monkeypatch.setattr(queue_client_module.XClIdGenStore, "items", items)
    monkeypatch.setattr(queue_client_module.XClIdGenStore, "get", classmethod(fake_get))
    mock.add(SEARCH_404, SEARCH_OK)

    rep = await client.get(URL, params=PARAMS)

    assert rep is not None
    # replayed with a freshly signed request, not the cached generator
    assert len(mock.calls) == 2
    assert items["user1"] is fresh


async def test_persistent_failure_probes_once_then_aborts(monkeypatch):
    callback, calls = recorder()
    client, mock = make_client(monkeypatch, FakePool([make_account()]), callback)
    mock.add(ROUTE_FAILED, ROUTE_FAILED)

    with pytest.raises(ApiError, match="account check passed.*identical replay") as exc:
        await client.get(URL, params=PARAMS)

    assert "search_internal_error" in str(exc.value)
    assert "inconclusive" not in str(exc.value)
    assert calls == ["user1"]
    assert len(mock.calls) == 2


async def test_inconclusive_probe_aborts_without_replay(monkeypatch):
    callback, calls = recorder(INCONCLUSIVE)
    client, mock = make_client(monkeypatch, FakePool([make_account()]), callback)
    mock.add(ROUTE_FAILED)

    with pytest.raises(ApiError, match="inconclusive"):
        await client.get(URL, params=PARAMS)

    assert calls == ["user1"]
    assert len(mock.calls) == 1


async def test_negative_probe_rotates_without_replay(monkeypatch):
    callback, calls = recorder(NEGATIVE)
    pool = FakePool([make_account("user1"), make_account("user2")])
    client, mock = make_client(monkeypatch, pool, callback)
    mock.add(ROUTE_FAILED, SEARCH_OK)

    rep = await client.get(URL, params=PARAMS)

    assert getattr(rep, "__username") == "user2"
    assert calls == ["user1"]
    assert mock.calls[0]["params"] is mock.calls[1]["params"] is PARAMS
    assert pool.accounts[0].active and pool.accounts[0].locks == {}


async def test_empty_timeline_internal_error_is_probed_and_rotated(monkeypatch):
    # 线上形态：200 + Internal server error + 只有 cursor 的 timeline
    callback, calls = recorder(NEGATIVE)
    pool = FakePool([make_account("user1"), make_account("user2")])
    client, mock = make_client(monkeypatch, pool, callback)
    mock.add(ROUTE_FAILED_EMPTY_TIMELINE, SEARCH_OK)

    rep = await client.get(URL, params=PARAMS)

    assert getattr(rep, "__username") == "user2"
    assert calls == ["user1"]
    assert len(mock.calls) == 2


async def test_negative_probe_same_account_twice_aborts(monkeypatch):
    callback, calls = recorder(NEGATIVE)
    client, mock = make_client(monkeypatch, FakePool([make_account()], sticky=True), callback)
    mock.add(ROUTE_FAILED, ROUTE_FAILED)

    with pytest.raises(AbortReqError, match="repeatedly"):
        await client.get(URL, params=PARAMS)

    assert calls == ["user1"]
    assert len(mock.calls) == 2


async def test_callback_error_propagates_unchanged(monkeypatch):
    callback, calls = recorder(error=RuntimeError("db unavailable"))
    client, mock = make_client(monkeypatch, FakePool([make_account()]), callback)
    mock.add(ROUTE_FAILED)

    with pytest.raises(RuntimeError, match="db unavailable"):
        await client.get(URL, params=PARAMS)

    assert calls == ["user1"]
    assert len(mock.calls) == 1


async def test_unrelated_route_error_bypasses_callback(monkeypatch):
    # only known search route failures (empty 404, 200 internal error) are probed
    callback, calls = recorder()
    client, mock = make_client(monkeypatch, FakePool([make_account()]), callback)
    not_found = (
        "response",
        200,
        {"errors": [{"code": 34, "message": "Sorry, that page does not exist."}]},
        "",
        None,
    )
    mock.add(not_found)

    with pytest.raises(ApiError, match="page does not exist"):
        await client.get(URL, params=PARAMS)

    assert calls == []
    assert len(mock.calls) == 1


async def test_cancellation_during_replay_releases_lease(monkeypatch):
    callback, calls = recorder()
    pool = FakePool([make_account()])
    client, mock = make_client(monkeypatch, pool, callback, hold=True)
    mock.add(ROUTE_FAILED, SEARCH_OK)  # first served, replay hangs

    task = asyncio.create_task(client.get(URL, params=PARAMS))
    await mock.started.wait()  # probe done, replay in flight
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls == ["user1"]
    await client.__aexit__(None, None, None)
    assert pool.leases == set()
