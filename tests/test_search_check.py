import asyncio
import json
import os
from typing import cast

import pytest

from twscrape import api as api_module
from twscrape.account import Account
from twscrape.accounts_pool import AccountsPool
from twscrape.api import API, GQL_FEATURES, GQL_URL, OP_SearchTimeline
from twscrape.http import NetworkError
from twscrape.queue_client import XClIdGenStore
from twscrape.utils import rate_limiter
from twscrape.xclid import XClIdParseError

from .mock_http import MockClient

DATA_DIR = os.path.join(os.path.dirname(__file__), "mocked-data")


def load(name: str):
    with open(os.path.join(DATA_DIR, f"{name}.json")) as f:
        return json.load(f)


class FakePool:
    def __init__(self, account: Account | None):
        self.account = account
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, bool]] = []
        self.write_error: Exception | None = None

    async def get_account(self, username: str):
        self.get_calls.append(username)
        return self.account if self.account and self.account.username == username else None

    async def set_can_search(self, username: str, can_search: bool):
        if self.write_error:
            raise self.write_error
        self.set_calls.append((username, can_search))
        if self.account:
            self.account.can_search = can_search


class RecordingClient(MockClient):
    def __init__(self):
        super().__init__()
        self.calls: list[tuple[str, str, dict]] = []
        self.closed = 0

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return await super().request(method, url, **kwargs)

    async def aclose(self):
        self.closed += 1


class HangingClient(RecordingClient):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        self.started.set()
        await asyncio.Event().wait()


def make_account(
    *,
    username="user1",
    cookies=None,
    can_search=None,
    active=True,
    proxy=None,
) -> Account:
    return Account(
        username=username,
        password="_",
        email="",
        email_password="",
        user_agent="@chrome",
        active=active,
        cookies={"auth_token": "token", "ct0": "csrf"} if cookies is None else cookies,
        can_search=can_search,
        proxy=proxy,
    )


def make_api(pool: FakePool, proxy: str | None = None) -> API:
    api = API.__new__(API)
    api.pool = cast(AccountsPool, pool)
    api.proxy = proxy
    api.debug = False
    api.max_empty_pages = 0
    return api


def patch_client(monkeypatch, client: MockClient) -> list[str | None]:
    calls: list[str | None] = []

    def make_client(self, proxy=None):
        calls.append(proxy)
        return client

    monkeypatch.setattr(Account, "make_client", make_client)
    return calls


def patch_gen(monkeypatch, calls=None, error: Exception | None = None):
    calls = [] if calls is None else calls

    class Gen:
        def calc(self, method, path):
            return f"tx:{method}:{path}"

    async def fake_get(cls, username, proxy=None, cookies=None, fresh=False):
        calls.append({"username": username, "proxy": proxy, "cookies": cookies, "fresh": fresh})
        if error:
            raise error
        return Gen()

    monkeypatch.setattr(XClIdGenStore, "get", classmethod(fake_get))
    return calls


def patch_limiter(monkeypatch) -> list[bool]:
    calls: list[bool] = []

    async def acquire():
        calls.append(True)

    monkeypatch.setattr(rate_limiter, "acquire", acquire)
    return calls


def assert_search_request(call, *, signed_path: str | None = None):
    method, url, kwargs = call
    assert method == "GET"
    assert url == f"{GQL_URL}/{OP_SearchTimeline}"
    assert json.loads(kwargs["params"]["variables"]) == {
        "rawQuery": "hello",
        "count": 20,
        "product": "Latest",
        "querySource": "typed_query",
    }
    assert json.loads(kwargs["params"]["features"]) == GQL_FEATURES
    assert json.loads(kwargs["params"]["fieldToggles"]) == {"withArticleRichContentState": False}
    path = signed_path or f"/i/api/graphql/{OP_SearchTimeline}"
    assert kwargs["headers"]["x-client-transaction-id"] == f"tx:GET:{path}"


async def test_positive_marks_true_and_uses_normal_search_request(monkeypatch):
    account = make_account(active=False, can_search=False)
    pool = FakePool(account)
    api = make_api(pool, proxy="http://proxy.example:8080")
    client = RecordingClient()
    client.add_response(json=load("raw_search"))
    make_client_calls = patch_client(monkeypatch, client)
    gen_calls = patch_gen(monkeypatch)
    limiter_calls = patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": True,
        "updated": True,
        "reason": "ok",
    }
    assert pool.get_calls == ["user1"]
    assert pool.set_calls == [("user1", True)]
    assert account.can_search is True
    assert make_client_calls == ["http://proxy.example:8080"]
    assert len(client.calls) == 1
    assert_search_request(client.calls[0])
    assert gen_calls == [
        {
            "username": "user1",
            "proxy": "http://proxy.example:8080",
            "cookies": account.cookies,
            "fresh": False,
        }
    ]
    assert limiter_calls == [True]
    assert client.closed == 1


@pytest.mark.parametrize(
    "response,reason",
    [
        ({"status_code": 404}, "search_404"),
        (
            {"json": {"errors": [{"code": -1, "message": "Internal server error"}]}},
            "search_internal_error",
        ),
    ],
)
async def test_repeated_known_failure_marks_false_with_fresh_id(monkeypatch, response, reason):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(**response)
    client.add_response(**response)
    patch_client(monkeypatch, client)
    gen_calls = patch_gen(monkeypatch)
    limiter_calls = patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": False,
        "updated": True,
        "reason": reason,
    }
    assert pool.set_calls == [("user1", False)]
    assert account.can_search is False
    assert [x["fresh"] for x in gen_calls] == [False, True]
    assert limiter_calls == [True, True]
    assert len(client.calls) == 2
    assert_search_request(client.calls[1])
    assert client.closed == 1


async def test_404_then_positive_marks_true(monkeypatch):
    account = make_account(can_search=False)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(status_code=404)
    client.add_response(json=load("raw_search"))
    patch_client(monkeypatch, client)
    gen_calls = patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": True,
        "updated": True,
        "reason": "ok",
    }
    assert pool.set_calls == [("user1", True)]
    assert [x["fresh"] for x in gen_calls] == [False, True]


async def test_single_known_failure_then_empty_stays_inconclusive(monkeypatch):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(status_code=404)
    client.add_response(json={"data": {}})
    patch_client(monkeypatch, client)
    gen_calls = patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": "empty_search",
    }
    assert pool.set_calls == []
    assert account.can_search is True
    assert [x["fresh"] for x in gen_calls] == [False, True]
    assert client.closed == 1


@pytest.mark.parametrize("message", ["something went wrong", "Internal server error"])
@pytest.mark.parametrize("data_kind", ["tweets", "unparsed"])
async def test_errors_with_data_stays_inconclusive(monkeypatch, message, data_kind):
    payload: dict = (
        load("raw_search")
        if data_kind == "tweets"
        else {"data": {"search_by_raw_query": {"unexpected": "data"}}}
    )
    payload["errors"] = [{"code": 1, "message": message}]
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(json=payload)
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": "http_200_errors",
    }
    assert pool.set_calls == []
    assert account.can_search is True
    assert len(client.calls) == 1
    assert client.closed == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {}},
        {
            "data": {
                "search_by_raw_query": {
                    "search_timeline": {
                        "timeline": {
                            "instructions": [
                                {
                                    "entries": [
                                        {
                                            "entryId": "cursor-bottom-1",
                                            "content": {
                                                "__typename": "TimelineTimelineCursor",
                                                "cursorType": "Bottom",
                                                "value": "cursor-value",
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        },
    ],
)
async def test_empty_or_cursor_only_does_not_overwrite(monkeypatch, payload):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(json=payload)
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result["can_search"] is None
    assert result["updated"] is False
    assert result["reason"] == "empty_search"
    assert pool.set_calls == []
    assert account.can_search is True
    assert client.closed == 1


@pytest.mark.parametrize(
    "status,reason",
    [(401, "http_401"), (403, "http_403"), (429, "http_429"), (500, "http_500")],
)
async def test_other_http_failures_do_not_overwrite(monkeypatch, status, reason):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(status_code=status)
    patch_client(monkeypatch, client)
    gen_calls = patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": reason,
    }
    assert pool.set_calls == []
    assert account.can_search is True
    assert [x["fresh"] for x in gen_calls] == [False]
    assert len(client.calls) == 1
    assert client.closed == 1


async def test_invalid_json_and_transport_error_do_not_overwrite(monkeypatch):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    client.add_invalid_json_response(status_code=200)
    client.add_exception(NetworkError("private proxy detail"))
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    invalid = await api.check_search("user1")
    transport = await api.check_search("user1")

    assert invalid["can_search"] is None
    assert invalid["reason"] == "JSONDecodeError"
    assert transport["can_search"] is None
    assert transport["reason"] == "NetworkError"
    assert "private proxy detail" not in transport["reason"]
    assert pool.set_calls == []
    assert account.can_search is True
    assert client.closed == 2


async def test_signing_error_does_not_overwrite(monkeypatch):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = RecordingClient()
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch, error=XClIdParseError("private cookie detail"))
    limiter_calls = patch_limiter(monkeypatch)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": "XClIdParseError",
    }
    assert result["reason"] == "XClIdParseError"
    assert "private cookie detail" not in result["reason"]
    assert pool.set_calls == []
    assert account.can_search is True
    assert client.calls == []
    assert limiter_calls == []
    assert client.closed == 1


async def test_timeout_does_not_overwrite_and_closes_client(monkeypatch):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = HangingClient()
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)
    monkeypatch.setattr(api_module, "SEARCH_CHECK_TIMEOUT", 0.02)

    result = await api.check_search("user1")

    assert result == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": "TimeoutError",
    }
    assert pool.set_calls == []
    assert account.can_search is True
    assert client.closed == 1


async def test_cancellation_closes_client(monkeypatch):
    account = make_account(can_search=True)
    pool = FakePool(account)
    api = make_api(pool)
    client = HangingClient()
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    task = asyncio.create_task(api.check_search("user1"))
    await client.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert pool.set_calls == []
    assert account.can_search is True
    assert client.closed == 1


async def test_missing_account_and_session_leave_db_unchanged(monkeypatch):
    missing_pool = FakePool(None)
    missing_api = make_api(missing_pool)

    missing = await missing_api.check_search("user1")

    assert missing == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": "account_not_found",
    }
    assert missing_pool.set_calls == []

    account = make_account(cookies={}, can_search=True)
    session_pool = FakePool(account)
    session_api = make_api(session_pool)
    client = RecordingClient()
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    no_session = await session_api.check_search("user1")

    assert no_session == {
        "username": "user1",
        "can_search": None,
        "updated": False,
        "reason": "missing_session",
    }
    assert session_pool.set_calls == []
    assert account.can_search is True
    assert client.calls == []


async def test_db_write_error_propagates(monkeypatch):
    account = make_account()
    pool = FakePool(account)
    pool.write_error = RuntimeError("db unavailable")
    api = make_api(pool)
    client = RecordingClient()
    client.add_response(json=load("raw_search"))
    patch_client(monkeypatch, client)
    patch_gen(monkeypatch)
    patch_limiter(monkeypatch)

    with pytest.raises(RuntimeError, match="db unavailable"):
        await api.check_search("user1")

    assert client.closed == 1
