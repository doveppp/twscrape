import base64
import json
import os
from typing import Any

import pytest

from twscrape.account import Account
from twscrape.accounts_pool import NoAccountError
from twscrape.api import API, OP_SearchTimeline
from twscrape.models import parse_tweets
from twscrape.queue_client import AbortReqError, ApiError
from twscrape.utils import gather, get_env_bool

from .mock_http import MockClient

DATA_DIR = os.path.join(os.path.dirname(__file__), "mocked-data")

CLOUDFLARE_BLOCK = {
    "status_code": 403,
    "text": "<html>blocked</html>",
    "headers": {"content-type": "text/html", "cf-ray": "abc123"},
}
# Seen in production: "API unknown error: 200 - ... - (-1) DeadlineExceeded: Unspecified"
DEADLINE_EXCEEDED = {"json": {"errors": [{"code": -1, "message": "DeadlineExceeded: Unspecified"}]}}


class MockedError(Exception):
    pass


class RecordingClient(MockClient):
    """MockClient that records each request's encoded params."""

    def __init__(self):
        super().__init__()
        self.calls: list[dict] = []

    async def request(self, method, url, **kwargs):
        self.calls.append(kwargs.get("params") or {})
        return await super().request(method, url, **kwargs)


def load(name: str) -> Any:
    with open(os.path.join(DATA_DIR, f"{name}.json")) as f:
        return json.load(f)


# Seen in production on UserByRestId: the error comes with an empty `user`
DEADLINE_EXCEEDED_EMPTY_USER = {"json": load("_issue_346_deadline_exceeded_empty_user")}

FAILED_REQUESTS = [
    (CLOUDFLARE_BLOCK, 1, AbortReqError),
    # DeadlineExceeded is retried on the same account before it raises
    (DEADLINE_EXCEEDED, 3, ApiError),
    (DEADLINE_EXCEEDED_EMPTY_USER, 3, ApiError),
]


@pytest.fixture
def api_http(api_mock: API, monkeypatch):
    async def fake_sleep(secs):
        pass

    mock = RecordingClient()
    monkeypatch.setattr("twscrape.queue_client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(Account, "make_client", lambda self, proxy=None: mock)
    return api_mock, mock


GQL_GEN = [
    "search",
    "tweet_replies",
    "tweet_thread",
    "retweeters",
    "followers",
    "following",
    "user_tweets",
    "user_tweets_and_replies",
    "list_timeline",
    "trends",
    "list_members",
    "community_members",
    "community_moderators",
    "community_tweets",
]


async def test_check_search_persists_result_without_changing_account_health(api_http):
    api, mock = api_http
    await api.pool.set_can_search("user1", False)
    await api.pool._db.execute(
        "UPDATE accounts SET _tx = 'existing-lease', xclid_fail_count = 2, "
        "global_locked_until = '2099-01-01 00:00:00' WHERE username = 'user1'"
    )
    before = (await api.pool.get("user1")).to_rs()
    mock.add_response(json=load("raw_search"))

    result = await api.check_search("user1")

    assert result["can_search"] is True
    assert result["updated"] is True
    before["can_search"] = True
    assert (await api.pool.get("user1")).to_rs() == before

    mock.add_response(status_code=429)
    result = await api.check_search("user1")

    assert result["can_search"] is None
    assert result["updated"] is False
    assert (await api.pool.get("user1")).to_rs() == before


SEARCH_KNOWN_FAILURES = [
    pytest.param({"status_code": 404}, id="empty-404"),
    pytest.param(
        {"json": {"errors": [{"code": -1, "message": "Internal server error"}]}},
        id="ise-200",
    ),
]


@pytest.mark.parametrize("failure", SEARCH_KNOWN_FAILURES)
async def test_search_auto_probe_rotates_and_replays_original_request(api_http, failure):
    api, mock = api_http
    api.pool._order_by = "username"
    await api.pool.add_account(
        "user2", "pass2", "email2", "email_pass2", cookies="auth_token=t2; ct0=c2"
    )
    await api.pool.set_active("user2", True)

    mock.add_response(**failure)  # original page on user1
    mock.add_response(**failure)  # real check_search probe 1
    mock.add_response(**failure)  # real check_search probe 2 (fresh transaction id)
    mock.add_response(json=load("raw_search"))  # original page replayed on user2

    results = await gather(api.search("foo", limit=1, kv={"cursor": "abc"}))

    assert results
    variables = [json.loads(x["variables"]) for x in mock.calls]
    assert len(variables) == 4
    assert variables[0] == variables[3]
    assert variables[0]["rawQuery"] == "foo"
    assert variables[0]["cursor"] == "abc"
    assert [x["rawQuery"] for x in variables[1:3]] == ["hello", "hello"]

    user1 = await api.pool.get("user1")
    assert user1.can_search is False
    assert user1.active is True
    assert user1.global_locked_until is None
    assert user1.xclid_fail_count == 0
    assert user1.unknown_fail_count == 0
    assert user1.locks == {}

    user2 = await api.pool.get("user2")
    assert user2.can_search is None
    assert user2.active is True
    assert user2.locks == {}


async def test_gql_params(api_mock: API, monkeypatch):
    for func in GQL_GEN:
        args = []

        def mock_gql_items(*a, **kw):
            args.append((a, kw))
            raise MockedError()

        try:
            monkeypatch.setattr(api_mock, "_gql_items", mock_gql_items)
            await gather(getattr(api_mock, func)("user1", limit=100, kv={"count": 100}))
        except MockedError:
            pass

        assert len(args) == 1, f"{func} not called once"
        # Parsed generators count unique items, so raw pagination runs until
        # enough distinct results have passed the parser.
        assert args[0][1]["limit"] == -1, f"raw limit not disabled in {func}"
        assert args[0][0][1]["count"] == 100, f"count not changed in {func}"


async def test_search_only_passes_check_search_to_queue_client(api_mock: API, monkeypatch):
    seen = []

    class FakeQueueClient:
        def __init__(self, pool, queue, debug=False, proxy=None, search_check=None):
            seen.append((queue, search_check))

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, *args, **kwargs):
            return None

    monkeypatch.setattr("twscrape.api.QueueClient", FakeQueueClient)
    assert await gather(api_mock.search("foo")) == []
    await api_mock.user_by_id(1)
    await api_mock._gql_item(OP_SearchTimeline, {})

    assert seen == [
        ("SearchTimeline", api_mock.check_search),
        ("UserByRestId", None),
        ("SearchTimeline", api_mock.check_search),
    ]


async def test_trends_timeline_ids(api_mock: API, monkeypatch):
    # X answers an unpadded timelineId with "(-1) Internal server error"
    sent = []

    def mock_gql_items(*a, **kw):
        sent.append(a[1]["timelineId"])
        raise MockedError()

    monkeypatch.setattr(api_mock, "_gql_items", mock_gql_items)
    tabs = ("trending", "news", "sport", "entertainment")
    for tab in tabs:
        with pytest.raises(MockedError):
            await gather(api_mock.trends(tab))

    for tab, timeline_id in zip(tabs, sent):
        assert len(timeline_id) % 4 == 0, f"{tab}: {timeline_id} is not padded"
        decoded = base64.b64decode(timeline_id, validate=True)
        assert decoded.startswith(b"Timeline:") and tab.encode() in decoded, tab


async def test_article_rich_content_only_in_tweet_details(api_mock: API, monkeypatch):
    # full article content state is ~80 KB per tweet, so timelines request previews only
    args = []

    def mock_gql_items(*a, **kw):
        args.append(kw)
        raise MockedError()

    async def mock_gql_item(*a, **kw):
        args.append(kw)

    monkeypatch.setattr(api_mock, "_gql_items", mock_gql_items)
    monkeypatch.setattr(api_mock, "_gql_item", mock_gql_item)
    for func in ("search", "list_timeline"):
        with pytest.raises(MockedError):
            await gather(getattr(api_mock, func)("1", limit=10))

    await api_mock.tweet_details(1)
    rich = [x["toggles"]["withArticleRichContentState"] for x in args]
    assert rich == [False, False, True]


async def test_raise_when_no_account(api_mock: API):
    await api_mock.pool.delete_accounts(["user1"])
    assert len(await api_mock.pool.get_all()) == 0

    assert get_env_bool("TWS_RAISE_WHEN_NO_ACCOUNT") is False
    os.environ["TWS_RAISE_WHEN_NO_ACCOUNT"] = "1"
    assert get_env_bool("TWS_RAISE_WHEN_NO_ACCOUNT") is True

    with pytest.raises(NoAccountError):
        await gather(api_mock.search("foo", limit=10))

    with pytest.raises(NoAccountError):
        await api_mock.user_by_id(123)

    del os.environ["TWS_RAISE_WHEN_NO_ACCOUNT"]
    assert get_env_bool("TWS_RAISE_WHEN_NO_ACCOUNT") is False


# https://github.com/vladkens/twscrape/issues/346
# A failed request must raise; None (or an empty generator) is kept for X's own
# "no data" answers, recorded from X in the _issue_346_*.json files.


@pytest.mark.parametrize(
    "method,arg,data",
    [
        ("user_by_id", 1, "_issue_346_user_by_id_not_found"),
        ("user_by_login", "coalition_peaks", "_issue_346_user_by_login_not_found"),
        ("user_by_id", 17161988, "_issue_346_user_suspended"),
    ],
)
async def test_user_lookup_returns_none_when_x_has_no_user(api_http, method, arg, data):
    api, mock = api_http
    mock.add_response(json=load(data))

    assert await getattr(api, method)(arg) is None


@pytest.mark.parametrize("response,tries,error", FAILED_REQUESTS)
@pytest.mark.parametrize("method,arg", [("user_by_id", 123), ("user_by_login", "someone")])
async def test_user_lookup_raises_when_request_fails(api_http, method, arg, response, tries, error):
    api, mock = api_http
    for _ in range(tries):
        mock.add_response(**response)

    with pytest.raises(error):
        await getattr(api, method)(arg)


async def test_user_tweets_of_unavailable_user_ends_without_error(api_http):
    # Protected or suspended account: X answers with UserUnavailable and no timeline
    api, mock = api_http
    mock.add_response(json=load("_issue_346_user_tweets_unavailable"))

    assert await gather(api.user_tweets(123)) == []


@pytest.mark.parametrize("response,tries,error", FAILED_REQUESTS)
async def test_user_tweets_raises_when_page_fails_mid_pagination(api_http, response, tries, error):
    api, mock = api_http
    page = load("raw_user_tweets")  # has a Bottom cursor
    mock.add_response(json=page)
    for _ in range(tries):
        mock.add_response(**response)

    tweets = []
    with pytest.raises(error):
        async for tweet in api.user_tweets(123):
            tweets.append(tweet)

    # the caller keeps what was yielded before the failure
    assert [x.id for x in tweets] == [x.id for x in parse_tweets(page)]
    assert mock._queue == []
