import asyncio
import time

import pytest

from dovexscrape.accounts_pool import AccountsPool
from dovexscrape.queue_client import QueueClient
from dovexscrape.ratelimit import (
    DEFAULT_RATE_LIMIT,
    configure_rate_limit,
    parse_rate_limit,
    rate_limiter,
    resolve_rate_limit,
)

from .mock_http import MockClient

URL = "https://example.com/api"
CF = tuple[AccountsPool, QueueClient, MockClient]


@pytest.fixture
def limiter():
    """Enable the global limiter for one test and restore the disabled default after."""
    yield rate_limiter.configure
    rate_limiter.configure(None)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("5", 5.0),
        ("5/s", 5.0),
        ("5RPS", 5.0),
        (" 6 ", 6.0),
        (3.5, 3.5),
        (7, 7.0),
        ("0.5", 0.5),
    ],
)
def test_parse_rate_limit_accepts_variants(value, expected):
    assert parse_rate_limit(value) == expected


@pytest.mark.parametrize("value", [None, 0, 0.0, "", "  ", "off", "OFF", "none", "disabled", -1])
def test_parse_rate_limit_disables(value):
    assert parse_rate_limit(value) is None


def test_parse_rate_limit_invalid_falls_back_to_default():
    assert parse_rate_limit("abc") == DEFAULT_RATE_LIMIT
    assert parse_rate_limit("5/") == 5.0  # trailing slash still parses


def test_resolve_rate_limit_prefers_argument_over_env(monkeypatch):
    monkeypatch.setenv("DOVEXSCRAPE_RATE_LIMIT", "9")
    assert resolve_rate_limit() == 9.0
    assert resolve_rate_limit("3") == 3.0
    assert resolve_rate_limit("off") is None

    monkeypatch.delenv("DOVEXSCRAPE_RATE_LIMIT")
    assert resolve_rate_limit() == DEFAULT_RATE_LIMIT


def test_configure_rate_limit_is_noop_without_value(monkeypatch):
    monkeypatch.delenv("DOVEXSCRAPE_RATE_LIMIT", raising=False)
    rate_limiter.configure(7)
    try:
        configure_rate_limit()  # nothing configured: must not reset the process limiter
        assert rate_limiter.qps == 7
    finally:
        rate_limiter.configure(None)


def test_configure_rate_limit_applies_explicit_value(monkeypatch):
    monkeypatch.setenv("DOVEXSCRAPE_RATE_LIMIT", "11")
    try:
        configure_rate_limit("4/s")
        assert rate_limiter.qps == 4

        configure_rate_limit("off")
        assert rate_limiter.qps is None

        configure_rate_limit(2)
        assert rate_limiter.qps == 2
    finally:
        rate_limiter.configure(None)


async def test_limiter_spaces_acquires_evenly(limiter):
    limiter(25)  # one slot every 40ms

    start = time.monotonic()
    for _ in range(4):
        await rate_limiter.acquire()
    elapsed = time.monotonic() - start

    # first slot is free, the other three wait 3 x 40ms
    assert 0.10 <= elapsed <= 0.60, elapsed


async def test_limiter_is_shared_across_callers(limiter):
    limiter(25)

    async def worker():
        for _ in range(2):
            await rate_limiter.acquire()

    start = time.monotonic()
    await asyncio.gather(worker(), worker())
    elapsed = time.monotonic() - start

    # 4 requests over one global budget -> 3 waits, not 2 (per-caller) waits
    assert 0.10 <= elapsed <= 0.60, elapsed


async def test_disabled_limiter_does_not_wait(limiter):
    limiter(None)

    start = time.monotonic()
    for _ in range(5):
        await rate_limiter.acquire()
    assert time.monotonic() - start < 0.05


def test_limiter_rebinds_to_a_new_event_loop():
    # AsyncLimiter caches the loop it first ran on, so the wrapper must rebuild it
    # when the process moves to another loop (eg. a second asyncio.run()).
    rate_limiter.configure(50)
    try:
        loop_ids = set()
        for _ in range(2):
            asyncio.run(rate_limiter.acquire())
            loop_ids.add(id(rate_limiter._loop))
        assert len(loop_ids) == 2
    finally:
        rate_limiter.configure(None)


async def test_rate_limiter_does_not_disturb_business_backoff(
    client_fixture: CF, limiter, monkeypatch
):
    _, client, mock = client_fixture
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    # aiolimiter waits via loop.call_at, so patching asyncio.sleep must not hide it
    monkeypatch.setattr("dovexscrape.queue_client.asyncio.sleep", fake_sleep)
    limiter(50)

    await client.__aenter__()
    mock.add_response(
        status_code=429,
        text="<html>limited</html>",
        headers={"content-type": "text/html", "cf-ray": "abc123"},
    )
    mock.add_response(json={"ok": True})

    rep = await client.get(URL)
    assert rep is not None
    assert sleeps == [2]  # only the CF backoff, no limiter waits mixed in

    await client.__aexit__(None, None, None)
