import asyncio

import pytest

from twscrape.utils import (
    GlobalRateLimiter,
    configure_rate_limit,
    get_env_bool,
    parse_cookies,
    parse_proxy,
    parse_rate_limit,
    rate_limiter,
    resolve_rate_limit,
    to_old_obj,
)


def test_cookies_parse():
    val = "abc=123; def=456; ghi=789"
    assert parse_cookies(val) == {"abc": "123", "def": "456", "ghi": "789"}

    val = '{"abc": "123", "def": "456", "ghi": "789"}'
    assert parse_cookies(val) == {"abc": "123", "def": "456", "ghi": "789"}

    val = '[{"name": "abc", "value": "123"}, {"name": "def", "value": "456"}, {"name": "ghi", "value": "789"}]'
    assert parse_cookies(val) == {"abc": "123", "def": "456", "ghi": "789"}

    val = "eyJhYmMiOiAiMTIzIiwgImRlZiI6ICI0NTYiLCAiZ2hpIjogIjc4OSJ9"
    assert parse_cookies(val) == {"abc": "123", "def": "456", "ghi": "789"}

    val = "W3sibmFtZSI6ICJhYmMiLCAidmFsdWUiOiAiMTIzIn0sIHsibmFtZSI6ICJkZWYiLCAidmFsdWUiOiAiNDU2In0sIHsibmFtZSI6ICJnaGkiLCAidmFsdWUiOiAiNzg5In1d"
    assert parse_cookies(val) == {"abc": "123", "def": "456", "ghi": "789"}

    val = '{"cookies": {"abc": "123", "def": "456", "ghi": "789"}}'
    assert parse_cookies(val) == {"abc": "123", "def": "456", "ghi": "789"}

    assert parse_cookies('{"abc": 123}') == {"abc": "123"}

    with pytest.raises(ValueError, match="Invalid cookie value"):
        val = "{invalid}"
        assert parse_cookies(val) == {}


def test_proxy_parse():
    assert parse_proxy(None) is None
    assert parse_proxy("") is None

    # already has scheme
    assert parse_proxy("http://1.2.3.4:8080") == "http://1.2.3.4:8080"
    assert parse_proxy("http://user:pass@1.2.3.4:8080") == "http://user:pass@1.2.3.4:8080"
    assert parse_proxy("socks5://1.2.3.4:1080") == "socks5://1.2.3.4:1080"
    assert parse_proxy("socks5://user:pass@1.2.3.4:1080") == "socks5://user:pass@1.2.3.4:1080"

    # host:port
    assert parse_proxy("1.2.3.4:8080") == "http://1.2.3.4:8080"

    # host:port:user:pass
    assert parse_proxy("1.2.3.4:8080:user:pass") == "http://user:pass@1.2.3.4:8080"

    # user:pass@host:port (no scheme)
    assert parse_proxy("user:pass@1.2.3.4:8080") == "http://user:pass@1.2.3.4:8080"


def test_get_env_bool(monkeypatch):
    monkeypatch.delenv("TEST_BOOL_FLAG", raising=False)
    assert get_env_bool("TEST_BOOL_FLAG") is False
    assert get_env_bool("TEST_BOOL_FLAG", default_val=True) is True

    for truthy in ("1", "true", "yes", "True", "YES"):
        monkeypatch.setenv("TEST_BOOL_FLAG", truthy)
        assert get_env_bool("TEST_BOOL_FLAG") is True

    for falsy in ("0", "false", "no", ""):
        monkeypatch.setenv("TEST_BOOL_FLAG", falsy)
        assert get_env_bool("TEST_BOOL_FLAG") is False


def test_to_old_obj_user_new_schema():
    obj = {
        "__typename": "User",
        "rest_id": "12345",
        "core": {
            "screen_name": "testuser",
            "name": "Test User",
            "created_at": "Mon Jan 01 00:00:00 +0000 2020",
        },
        "avatar": {"image_url": "https://example.com/avatar.jpg"},
        "location": {"location": "Earth"},
        "privacy": {"protected": False},
        "verification": {"verified": True},
        "profile_bio": {
            "description": "A test bio",
            "entities": {"description": {}, "url": {}},
        },
        "action_counts": {"favorites_count": 11},
        "banner": {"image_url": "https://example.com/banner.jpg"},
        "pinned_items": {"tweet_ids_str": ["67890"]},
        "relationship_counts": {"followers": 12, "following": 13},
        "tweet_counts": {"media_tweets": 14, "tweets": 15},
        "is_blue_verified": True,
    }

    flat = to_old_obj(obj)
    assert flat["screen_name"] == "testuser"
    assert flat["profile_image_url_https"] == "https://example.com/avatar.jpg"
    assert flat["profile_banner_url"] == "https://example.com/banner.jpg"
    assert flat["location"] == "Earth"
    assert flat["protected"] is False
    assert flat["verified"] is True
    assert flat["description"] == "A test bio"
    assert flat["entities"] == {"description": {}, "url": {}}
    assert flat["favourites_count"] == 11
    assert flat["followers_count"] == 12
    assert flat["friends_count"] == 13
    assert flat["media_count"] == 14
    assert flat["statuses_count"] == 15
    assert flat["pinned_tweet_ids_str"] == ["67890"]
    assert flat["is_blue_verified"] is True
    assert flat["id"] == 12345


def test_to_old_obj_tweet_new_schema():
    obj = {
        "__typename": "Tweet",
        "rest_id": "9876",
        "source": "<a>Twitter Web App</a>",
    }

    flat = to_old_obj(obj)
    assert flat["source"] == "<a>Twitter Web App</a>"
    assert flat["id"] == 9876


# --- Rate limiting ---


@pytest.mark.parametrize(
    "value, expected",
    [
        (5, 5.0),
        ("5", 5.0),
        ("5/s", 5.0),
        ("5rps", 5.0),
        ("5 req/s", 5.0),
        (0, None),
        ("off", None),
        ("none", None),
        ("", None),
        (-1, None),
        ("invalid", 5.0),
        ("nan", 5.0),
        ("inf", 5.0),
    ],
)
def test_parse_rate_limit(value, expected):
    assert parse_rate_limit(value) == expected


def test_resolve_prefers_argument_over_environment(monkeypatch):
    monkeypatch.setenv("TWS_RATE_LIMIT", "9")
    assert resolve_rate_limit() == 9.0
    assert resolve_rate_limit(2) == 2.0
    monkeypatch.delenv("TWS_RATE_LIMIT")
    assert resolve_rate_limit() == 5.0


def test_configure_keeps_existing_rate_when_omitted():
    old = rate_limiter.qps
    try:
        configure_rate_limit(None)
        assert rate_limiter.qps == old
        configure_rate_limit(3)
        assert rate_limiter.qps == 3.0
    finally:
        rate_limiter.configure(old)


async def test_limiter_spaces_concurrent_requests():
    limiter = GlobalRateLimiter()
    limiter.configure(40)
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.gather(*(limiter.acquire() for _ in range(4)))
    assert loop.time() - start >= 0.06


def test_limiter_can_be_reused_after_asyncio_run():
    limiter = GlobalRateLimiter()
    limiter.configure(100)
    asyncio.run(limiter.acquire())
    first = limiter._limiter
    asyncio.run(limiter.acquire())
    assert first is not limiter._limiter


async def test_disabled_limiter_does_not_wait():
    limiter = GlobalRateLimiter()
    limiter.configure(None)
    await asyncio.gather(*(limiter.acquire() for _ in range(100)))


def test_environment_is_read_lazily(monkeypatch):
    monkeypatch.delenv("TWS_RATE_LIMIT", raising=False)
    monkeypatch.delenv("DOVEXSCRAPE_RATE_LIMIT", raising=False)
    limiter = GlobalRateLimiter()

    # setting the environment after importing twscrape still applies
    assert limiter.qps == 5.0
    monkeypatch.setenv("TWS_RATE_LIMIT", "3")
    assert limiter.qps == 3.0
    assert limiter.qps == 3.0

    # legacy fallback and later changes are picked up until explicitly configured
    monkeypatch.delenv("TWS_RATE_LIMIT")
    monkeypatch.setenv("DOVEXSCRAPE_RATE_LIMIT", "7")
    assert limiter.qps == 7.0
    monkeypatch.setenv("TWS_RATE_LIMIT", "9")
    assert limiter.qps == 9.0

    limiter.configure(2)
    monkeypatch.setenv("TWS_RATE_LIMIT", "11")
    assert limiter.qps == 2.0


def test_rate_limit_logs_effective_value_once(monkeypatch):
    monkeypatch.delenv("TWS_RATE_LIMIT", raising=False)
    monkeypatch.delenv("DOVEXSCRAPE_RATE_LIMIT", raising=False)
    logs = []
    from twscrape.logger import logger

    monkeypatch.setattr(logger, "info", logs.append)

    limiter = GlobalRateLimiter()
    limiter.log_config()
    limiter.log_config()

    assert logs == ["Global rate limit: 5 req/s"]
