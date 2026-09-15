"""
Regression tests for pagination bugs:
  - #265 / #247: followers() / following() stops early when X returns a promo-only page
    (all entries have entryId starting with "who-to-follow-", leaving els=[] after filter)
  - TweetDetail: replies() stops after the first page when the response only carries a
    Bottom cursor (no ShowMoreThreads), and walks endless empty pages when a page holds
    only collapsed conversation threads with empty `items`
"""

import json
import os

import pytest

from dovexscrape import API, gather
from dovexscrape.queue_client import QueueClient

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "mocked-data")


class FakeRep:
    def __init__(self, data: dict):
        self._data = data

    def json(self):
        return self._data


def make_promo_page(cursor: str | None = "next_cursor"):
    """One page where every real entry is a who-to-follow promo (plus an optional cursor)."""
    entries = [{"entryId": "who-to-follow-123", "content": {}}]
    if cursor:
        entries.append(
            {
                "entryId": "cursor-bottom-1",
                "content": {
                    "__typename": "TimelineTimelineCursor",
                    "cursorType": "Bottom",
                    "entryType": "TimelineTimelineCursor",
                    "value": cursor,
                },
                "sortIndex": "1",
            }
        )
    return {"data": {"entries": entries}}


def make_content_page(entry_id: str, cursor: str | None, cursor_type: str = "Bottom"):
    entries = [{"entryId": entry_id, "content": {"itemContent": {}}}]
    if cursor:
        entries.append(
            {
                "entryId": f"cursor-{cursor}",
                "content": {
                    "__typename": "TimelineTimelineCursor",
                    "cursorType": cursor_type,
                    "value": cursor,
                },
            }
        )
    return {"data": {"entries": entries}}


def make_collapsed_thread_page(cursor: str | None):
    """TweetDetail page holding only collapsed conversation threads.

    X returns these for the LowQuality section: `items` is empty and the tweet ids are
    only listed in `metadata.conversationMetadata.allTweetIds`, so parsing yields nothing.
    """
    entries = [
        {
            "entryId": f"conversationthread-{2099831927606344893 + i}",
            "content": {
                "__typename": "TimelineTimelineModule",
                "displayType": "VerticalConversation",
                "items": [],
                "clientEventInfo": {
                    "details": {"conversationDetails": {"conversationSection": "LowQuality"}}
                },
                "metadata": {
                    "conversationMetadata": {"allTweetIds": [str(2095337070676038025 + i)]}
                },
            },
            "sortIndex": str(2099831927606344893 + i),
        }
        for i in range(3)
    ]
    if cursor:
        entries.append(
            {
                "entryId": f"cursor-{cursor}",
                "content": {
                    "__typename": "TimelineTimelineCursor",
                    "cursorType": "Bottom",
                    "value": cursor,
                },
            }
        )
    return {"data": {"entries": entries}}


async def test_followers_continues_past_promo_pages(monkeypatch, api_mock: API):
    """followers() must not stop when X returns a page consisting entirely of promo entries."""
    with open(os.path.join(DATA_DIR, "raw_followers.json")) as f:
        users_page = json.load(f)

    # page 1: only who-to-follow entries + cursor  →  els=[] after filter, but cursor exists
    # page 2: real user entries  →  should be reached and parsed
    pages = [make_promo_page(cursor="next_cursor"), users_page]
    idx = 0

    async def mock_get(self, url, params=None):
        nonlocal idx
        if idx >= len(pages):
            return None
        data = pages[idx]
        idx += 1
        return FakeRep(data)

    monkeypatch.setattr(QueueClient, "get", mock_get)

    users = await gather(api_mock.followers(123))

    assert idx >= 2, (
        f"pagination stopped after {idx} page(s); promo-only page should not terminate pagination"
    )
    assert len(users) > 0, "expected users from page 2 but got none"


async def test_following_continues_past_promo_pages(monkeypatch, api_mock: API):
    """following() must not stop when X returns a page consisting entirely of promo entries."""
    with open(os.path.join(DATA_DIR, "raw_following.json")) as f:
        users_page = json.load(f)

    pages = [make_promo_page(cursor="next_cursor"), users_page]
    idx = 0

    async def mock_get(self, url, params=None):
        nonlocal idx
        if idx >= len(pages):
            return None
        data = pages[idx]
        idx += 1
        return FakeRep(data)

    monkeypatch.setattr(QueueClient, "get", mock_get)

    users = await gather(api_mock.following(123))

    assert idx >= 2, (
        f"pagination stopped after {idx} page(s); promo-only page should not terminate pagination"
    )
    assert len(users) > 0, "expected users from page 2 but got none"


async def test_followers_stops_after_too_many_consecutive_empty_pages(monkeypatch, api_mock: API):
    """Safeguard: if X returns many consecutive promo-only pages with cursors, pagination must stop."""
    idx = 0

    async def mock_get(self, url, params=None):
        nonlocal idx
        idx += 1
        return FakeRep(make_promo_page(cursor=f"cursor_{idx}"))

    monkeypatch.setattr(QueueClient, "get", mock_get)

    users = await gather(api_mock.followers(123))

    assert len(users) == 0
    assert idx < 10, f"too many requests ({idx}); should have stopped after a few empty pages"


async def test_gql_items_stops_on_repeated_cursor(monkeypatch, api_mock: API):
    page = make_content_page("user-1", "same-cursor")
    calls = 0

    async def mock_get(self, url, params=None):
        nonlocal calls
        calls += 1
        return FakeRep(page)

    monkeypatch.setattr(QueueClient, "get", mock_get)
    reps = [x async for x in api_mock._gql_items("hash/Followers", {})]

    assert len(reps) == 1
    assert calls == 2


async def test_search_stops_on_repeated_page_with_new_cursor(monkeypatch, api_mock: API):
    pages = [
        make_content_page("tweet-1", "cursor-1"),
        make_content_page("tweet-1", "cursor-2"),
    ]
    calls = 0

    async def mock_get(self, url, params=None):
        nonlocal calls
        page = pages[min(calls, len(pages) - 1)]
        calls += 1
        return FakeRep(page)

    monkeypatch.setattr(QueueClient, "get", mock_get)
    reps = [x async for x in api_mock._gql_items("hash/SearchTimeline", {})]

    assert len(reps) == 1
    assert calls == 2


async def test_tweet_replies_pages_with_bottom_cursor_only(monkeypatch, api_mock: API):
    """TweetDetail ships only a Bottom cursor: replies must page on instead of stopping."""
    cursors: list[str | None] = []

    async def mock_get(self, url, params=None):
        variables = json.loads((params or {}).get("variables", "{}"))
        cursors.append(variables.get("cursor"))
        n = len(cursors)
        # two pages carrying a Bottom cursor, then a last page without any cursor
        return FakeRep(make_content_page(f"tweet-{n}", f"bottom-{n}" if n <= 2 else None))

    monkeypatch.setattr(QueueClient, "get", mock_get)
    reps = [x async for x in api_mock.tweet_replies_raw(123)]

    assert cursors == [None, "bottom-1", "bottom-2"], "Bottom cursor was not used to page"
    assert len(reps) == 3, "each of the three pages should be yielded"


async def test_pages_of_collapsed_threads_count_as_empty(monkeypatch, api_mock: API):
    """A page of collapsed threads (empty items, nothing to parse) counts as an empty page."""
    calls = 0

    async def mock_get(self, url, params=None):
        nonlocal calls
        calls += 1
        # a fresh cursor every page, so the repeated-cursor guard cannot be what stops us;
        # the stuck cursor at the end only bounds a broken implementation
        cur = f"cursor-{calls}" if calls <= 10 else "cursor-stuck"
        return FakeRep(make_collapsed_thread_page(cur))

    monkeypatch.setattr(QueueClient, "get", mock_get)
    reps = [x async for x in api_mock._gql_items("hash/TweetDetail", {}, cursor_type="Bottom")]

    assert reps == []
    assert calls == 3, f"expected to stop after 3 empty pages, made {calls} requests"


async def test_thread_module_with_items_is_not_dropped(monkeypatch, api_mock: API):
    """Only module entries without items are dropped: a real conversation module must stay."""
    page = {
        "data": {
            "entries": [
                {
                    "entryId": "conversationthread-2099831927606344893",
                    "content": {
                        "__typename": "TimelineTimelineModule",
                        "displayType": "VerticalConversation",
                        "items": [{"entryId": "tweet-1", "item": {"itemContent": {}}}],
                    },
                },
                {
                    "entryId": "cursor-bottom-1",
                    "content": {
                        "__typename": "TimelineTimelineCursor",
                        "cursorType": "Bottom",
                        "value": "bottom-1",
                    },
                },
            ]
        }
    }
    calls = 0

    async def mock_get(self, url, params=None):
        nonlocal calls
        calls += 1
        return FakeRep(page)

    monkeypatch.setattr(QueueClient, "get", mock_get)
    reps = [x async for x in api_mock._gql_items("hash/TweetDetail", {})]

    assert len(reps) == 1, "module with items should still be treated as page content"
    assert calls == 2, "page 2 repeats the same cursor and should stop pagination"


# --- max_empty_pages ---


async def count_requests_until_stop(api: API, monkeypatch) -> int:
    calls = 0

    async def mock_get(self, url, params=None):
        nonlocal calls
        calls += 1
        return FakeRep(make_promo_page(cursor=f"cursor_{calls}"))

    monkeypatch.setattr(QueueClient, "get", mock_get)
    await gather(api.followers(123))
    return calls


@pytest.mark.parametrize("max_empty_pages", [0, 1])
async def test_max_empty_pages_zero_or_one_stops_on_first_empty_page(
    monkeypatch, api_mock: API, max_empty_pages: int
):
    api = API(api_mock.pool, max_empty_pages=max_empty_pages)
    calls = await count_requests_until_stop(api, monkeypatch)

    assert calls == 1, f"expected to stop on the first empty page, made {calls} request(s)"


async def test_max_empty_pages_two_tolerates_one_empty_page(monkeypatch, api_mock: API):
    api = API(api_mock.pool, max_empty_pages=2)
    calls = await count_requests_until_stop(api, monkeypatch)

    assert calls == 2, f"expected to stop on the second empty page, made {calls} request(s)"


async def test_max_empty_pages_env_and_param(monkeypatch, api_mock: API):
    monkeypatch.setenv("DOVEXSCRAPE_MAX_EMPTY_PAGES", "1")
    assert API(api_mock.pool).max_empty_pages == 1
    assert API(api_mock.pool, max_empty_pages=5).max_empty_pages == 5  # param wins

    monkeypatch.setenv("DOVEXSCRAPE_MAX_EMPTY_PAGES", "not-a-number")
    assert API(api_mock.pool).max_empty_pages == 3

    monkeypatch.setenv("DOVEXSCRAPE_MAX_EMPTY_PAGES", "-2")
    assert API(api_mock.pool).max_empty_pages == 0
