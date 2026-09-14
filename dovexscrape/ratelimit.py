import asyncio
import os

from aiolimiter import AsyncLimiter

from .logger import logger

ENV_KEY = "DOVEXSCRAPE_RATE_LIMIT"
# Conservative default: outgoing requests to X are shared by every account and queue in
# this process, so a low ceiling keeps the edge (Cloudflare) and the API calm.
DEFAULT_RATE_LIMIT = 5.0

_OFF_VALUES = {"", "0", "off", "none", "false", "no", "disable", "disabled"}


def parse_rate_limit(value: str | int | float | None) -> float | None:
    """Parse one configured rate limit into requests per second (None = disabled).

    Accepts `5`, `5.0`, "5" and "5/s"; `0`, "off", "none" and "" disable the limiter.
    Invalid values fall back to DEFAULT_RATE_LIMIT: a typo must not silently drop the
    protection, and it must not break scraping either.
    """
    if value is None:
        return None

    if isinstance(value, int | float):
        qps = float(value)
    else:
        text = str(value).strip().lower()
        if text in _OFF_VALUES:
            return None

        # "5/s" / "5rps" / "5 req/s" -> "5"
        text = text.split("/", 1)[0].replace("rps", "").replace("req", "").strip()
        try:
            qps = float(text)
        except ValueError:
            logger.warning(f"Invalid rate limit {value!r}, using {DEFAULT_RATE_LIMIT} req/s")
            return DEFAULT_RATE_LIMIT

    if qps <= 0:
        return None
    return qps


def resolve_rate_limit(value: str | int | float | None = None) -> float | None:
    """Explicit argument wins over the environment; both absent -> the default."""
    raw = value if value is not None else os.getenv(ENV_KEY)
    if raw is None:
        return DEFAULT_RATE_LIMIT
    return parse_rate_limit(raw)


class GlobalRateLimiter:
    """Process-wide QPS limiter shared by every QueueClient.

    Wraps aiolimiter's leaky bucket with a capacity of one slot, so requests leave
    evenly spaced (one every 1/qps seconds) instead of bursting.
    """

    def __init__(self):
        self._qps: float | None = None
        self._limiter: AsyncLimiter | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def qps(self) -> float | None:
        return self._qps

    def configure(self, qps: float | None) -> None:
        """Set the rate (None disables limiting) and reset the bucket."""
        self._qps = qps
        self._limiter = None
        self._loop = None
        logger.info(f"Global rate limit: {qps:g} req/s" if qps else "Global rate limit: disabled")

    async def acquire(self) -> None:
        qps = self._qps
        if qps is None:
            return

        # AsyncLimiter binds itself to the first running loop it sees, so recreate it
        # when the process moves on to another loop (eg. a second asyncio.run()).
        loop = asyncio.get_running_loop()
        if self._limiter is None or self._loop is not loop:
            self._limiter = AsyncLimiter(1, 1 / qps)
            self._loop = loop

        await self._limiter.acquire()


rate_limiter = GlobalRateLimiter()
rate_limiter.configure(resolve_rate_limit())


def configure_rate_limit(value: str | int | float | None = None) -> None:
    """Apply an explicit rate limit, if one was given.

    The environment (and otherwise the default) is applied once at import time, so
    callers such as API() never reset the process-wide limiter by adding nothing.
    """
    if value is None:
        return
    rate_limiter.configure(resolve_rate_limit(value))
