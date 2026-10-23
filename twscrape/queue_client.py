import asyncio
import json
import math
import os
import re
from datetime import timezone
from email.utils import parsedate_to_datetime
from enum import Enum, auto
from typing import Any
from urllib.parse import urlparse

from . import telemetry
from .account import Account, has_required_cookies
from .accounts_pool import AccountsPool
from .http import (
    ConnectError,
    HttpClient,
    HttpMethod,
    HttpStatusError,
    NetworkError,
    Response,
    format_error,
)
from .logger import LogOnce, logger
from .utils import rate_limiter, utc
from .xclid import XClIdAccountError, XClIdGen, XClIdParseError

ReqParams = dict[str, str | int] | None
MISSING_FEATURES_RE = re.compile(r"The following features cannot be null: ([^;]+)")
TMP_TS = utc.now().isoformat().split(".")[0].replace("T", "_").replace(":", "-")[0:16]

# Per-request cap: how many accounts may hit XClId 404 exhaustion before aborting.
MAX_XCLID_FAILED_ACCOUNTS = 10
# Consecutive XClIdRetryExhaustedError per account before deactivating it.
XCLID_FAIL_DEACTIVATE_THRESHOLD = 3
# Lock duration applied to an account on each XClIdRetryExhaustedError (seconds).
XCLID_FAIL_LOCK_SECONDS = 1 * 60
# Consecutive unknown errors (retry budget exhausted) before deactivating an account.
UNKNOWN_FAIL_DEACTIVATE_THRESHOLD = 3
# Lock duration applied to an account on each unknown error (seconds).
UNKNOWN_FAIL_LOCK_SECONDS = 60 * 15
# Lock duration applied when X serves a logged-out web build (seconds).
XCLID_ACCOUNT_FAIL_LOCK_SECONDS = 60 * 15
# Upper bound for honouring a Retry-After header on an edge (Cloudflare) rate limit.
BLOCKED_MAX_SLEEP_SECONDS = 60 * 10


class HandledError(Exception): ...


class AbortReqError(Exception): ...


class XClIdRetryExhaustedError(AbortReqError):
    """HTTP 404 persists after 3 retries with a fresh x-client-transaction-id."""


class BlockedError(Exception):
    """Edge (Cloudflare) rate limit: retryable after a backoff, unlike a hard block."""

    def __init__(self, msg: str, retry_after: float | None = None):
        super().__init__(msg)
        self.retry_after = retry_after


class GqlFeaturesOutdatedError(AbortReqError):
    """GQL_FEATURES in api.py no longer matches the X API and self-healing did not resolve it."""


class ApiError(AbortReqError):
    """X answered with errors and without data."""

    # errors is optional so that pickle, which calls ApiError(msg), can rebuild it
    def __init__(self, msg: str, errors: list[str] | None = None):
        super().__init__(msg)
        self.errors = errors or []


class FailKind(Enum):
    TRANSPORT = auto()
    LOADSHED = auto()
    BLOCKED = auto()
    UNKNOWN = auto()


class XClIdGenStore:
    items: dict[str, XClIdGen] = {}

    @classmethod
    async def get(
        cls,
        username: str,
        proxy: str | None = None,
        cookies: dict[str, str] | None = None,
        fresh=False,
    ) -> XClIdGen:
        if username in cls.items and not fresh:
            return cls.items[username]

        clid_gen = await XClIdGen.create(proxy=proxy, cookies=cookies)
        cls.items[username] = clid_gen
        return clid_gen


class Ctx:
    def __init__(self, acc: Account, clt: HttpClient, proxy: str | None = None):
        self.req_count = 0
        self.acc = acc
        self.clt = clt
        self.proxy = proxy
        self.fails = dict.fromkeys(FailKind, 0)

    def fail(self, kind: FailKind) -> bool:
        """Count a failed attempt of this kind, return whether it's still worth retrying."""
        fail_limit, total_fail_limit = 3, 4
        self.fails[kind] += 1
        return self.fails[kind] < fail_limit and sum(self.fails.values()) < total_fail_limit

    async def retry(self, kind: FailKind, delay: float | None = None) -> bool:
        """Count a failure and back off while retry budget remains."""
        if not self.fail(kind):
            return False
        await asyncio.sleep(delay if delay is not None else 2 ** self.fails[kind])
        return True

    async def aclose(self):
        await self.clt.aclose()

    async def req(self, method: HttpMethod, url: str, params: ReqParams = None) -> Response:
        # if code 404 on first try then generate new x-client-transaction-id and retry
        # https://github.com/vladkens/twscrape/issues/248
        path = urlparse(url).path or "/"

        tries = 0
        while tries < 3:
            gen = await XClIdGenStore.get(
                self.acc.username,
                proxy=self.proxy,
                cookies=self.acc.cookies,
                fresh=tries > 0,
            )
            hdr = {"x-client-transaction-id": gen.calc(method, path)}
            await rate_limiter.acquire()  # process-wide QPS ceiling, every real request
            rep = await self.clt.request(method, url, params=params, headers=hdr)
            if rep.status_code != 404:
                return rep

            tries += 1
            logger.debug(f"Retrying request with new x-client-transaction-id: {url}")
            await asyncio.sleep(1)

        raise XClIdRetryExhaustedError(
            "Failed to get XClIdGen after 3 retries. "
            "See: https://github.com/vladkens/twscrape/issues/248"
        )


def req_id(rep: Response):
    lr = str(rep.headers.get("x-rate-limit-remaining", -1))
    ll = str(rep.headers.get("x-rate-limit-limit", -1))
    sz = max(len(lr), len(ll))
    lr, ll = lr.rjust(sz), ll.rjust(sz)

    username = getattr(rep, "__username", "<UNKNOWN>")
    return f"{lr}/{ll} - {username}"


def has_data(rep: Response, res: Any) -> bool:
    """Return True for successful responses with at least one non-empty data field."""
    if rep.status_code != 200 or not isinstance(res, dict):
        return False

    # An empty field is no data: on a timeout X sends {"data": {"user": {}}}
    # with DeadlineExceeded at path user.result.
    data = res.get("data")
    return isinstance(data, dict) and any(value not in (None, {}, []) for value in data.values())


def has_error(errors: list[str], prefix: str) -> bool:
    return any(error.startswith(prefix) for error in errors)


def retry_after_seconds(rep: Response) -> float | None:
    """Read Retry-After (delta-seconds or HTTP-date), capped and clamped to >= 0."""
    value = rep.headers.get("retry-after")
    if not value:
        return None

    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        when = when if when.tzinfo is not None else when.replace(tzinfo=timezone.utc)
        seconds = (when - utc.now()).total_seconds()

    if not math.isfinite(seconds):
        return None
    return min(max(seconds, 0.0), BLOCKED_MAX_SLEEP_SECONDS)


def parse_missing_features(msg: str) -> list[str]:
    """Feature flags X named in a (336) error: "...cannot be null: a, b"."""
    match = MISSING_FEATURES_RE.search(msg)
    return [x.strip() for x in match.group(1).split(",") if x.strip()] if match else []


def add_missing_features(params: ReqParams, missing: list[str]) -> bool:
    """Set `missing` to true in params["features"]. False if there is nothing to patch."""
    if not isinstance(params, dict) or "features" not in params:
        return False

    features = params["features"]
    if isinstance(features, str):  # encode_params() has already run
        try:
            features = json.loads(features)
        except json.JSONDecodeError:
            return False

    if not isinstance(features, dict):
        return False

    if all(features.get(x) is True for x in missing):
        return False  # already true, so a retry would just loop

    features.update(dict.fromkeys(missing, True))
    params["features"] = json.dumps(features, separators=(",", ":"))
    return True


def dump_rep(rep: Response):
    count = getattr(dump_rep, "__count", -1) + 1
    setattr(dump_rep, "__count", count)

    acc = getattr(rep, "__username", "<unknown>")
    outfile = f"{count:05d}_{rep.status_code}_{acc}.txt"
    outfile = f"/tmp/twscrape-{TMP_TS}/{outfile}"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    msg = []
    msg.append(f"{count:,d} - {req_id(rep)}")
    msg.append(f"{rep.status_code} {rep.request.method} {rep.request.url}")
    msg.append("\n")
    # msg.append("\n".join([str(x) for x in list(rep.request.headers.items())]))
    msg.append("\n".join([str(x) for x in list(rep.headers.items())]))
    msg.append("\n")

    try:
        msg.append(json.dumps(rep.json(), indent=2))
    except json.JSONDecodeError:
        msg.append(rep.text)

    txt = "\n".join(msg)
    with open(outfile, "w") as f:
        f.write(txt)


class QueueClient:
    # Flags self-healed per operation, shared so later clients don't repeat the failed request
    healed_features: dict[str, set[str]] = {}

    def __init__(self, pool: AccountsPool, queue: str, debug=False, proxy: str | None = None):
        self.pool = pool
        self.queue = queue
        self.debug = debug
        self.ctx: Ctx | None = None
        self.proxy = proxy

    async def __aenter__(self):
        await self._get_ctx()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_ctx()

    async def _close_ctx(
        self, reset_at=-1, inactive=False, msg: str | None = None, global_lock=False
    ):
        if self.ctx is None:
            return

        ctx, self.ctx, self.req_count = self.ctx, None, 0
        username = ctx.acc.username
        tx = ctx.acc._tx
        await ctx.aclose()

        if inactive:
            await self.pool.mark_inactive(username, msg, tx=tx)
            return

        if reset_at > 0:
            await self.pool.lock_until(
                username, self.queue, reset_at, ctx.req_count, tx=tx, global_lock=global_lock
            )
            return

        await self.pool.unlock(username, self.queue, ctx.req_count, tx=tx)

    async def _xclid_failure(self, ctx: Ctx, exc: Exception, lock_seconds: int):
        """Persist an account-level transaction-id failure and rotate it out."""
        username = ctx.acc.username
        count = await self.pool.incr_fail_count(username, "xclid")
        self._xclid_failed_accounts += 1
        message = self._format_ctx_error(ctx, exc)

        if count >= XCLID_FAIL_DEACTIVATE_THRESHOLD:
            logger.warning(
                f"Transaction-id failure {count}/{XCLID_FAIL_DEACTIVATE_THRESHOLD}; "
                f"deactivating account: {username}"
            )
            await self._close_ctx(inactive=True, msg=message)
        else:
            logger.warning(
                f"Transaction-id failure {count}/{XCLID_FAIL_DEACTIVATE_THRESHOLD}; "
                f"locking {username} for {lock_seconds // 60} minutes (global)"
            )
            await self._close_ctx(utc.ts() + lock_seconds, global_lock=True)

        if self._xclid_failed_accounts >= MAX_XCLID_FAILED_ACCOUNTS:
            raise AbortReqError(
                f"Transaction-id failures persist after trying {MAX_XCLID_FAILED_ACCOUNTS} accounts"
            ) from exc

    async def _unknown_failure(self, ctx: Ctx, err: str):
        """Persist an exhausted unknown-error budget and rotate the account out."""
        username = ctx.acc.username
        count = await self.pool.incr_fail_count(username, "unknown")

        if count >= UNKNOWN_FAIL_DEACTIVATE_THRESHOLD:
            logger.warning(
                f"Unknown error {count}/{UNKNOWN_FAIL_DEACTIVATE_THRESHOLD}; "
                f"deactivating account: {username}; Err: {err}"
            )
            await self._close_ctx(inactive=True, msg=err)
        else:
            lock_min = UNKNOWN_FAIL_LOCK_SECONDS // 60
            logger.warning(
                f"Unknown error {count}/{UNKNOWN_FAIL_DEACTIVATE_THRESHOLD}; "
                f"locking {username} for {lock_min} minutes (global). Err: {err}"
            )
            await self._close_ctx(utc.ts() + UNKNOWN_FAIL_LOCK_SECONDS, global_lock=True)

    async def _get_ctx(self) -> Ctx | None:
        if self.ctx:
            return self.ctx

        acc = await self.pool.get_for_queue_or_wait(self.queue)
        if acc is None:
            return None

        try:
            clt = acc.make_client(proxy=self.proxy)
            self.ctx = Ctx(acc, clt, proxy=acc.resolve_proxy(self.proxy))
        except BaseException:
            await self.pool.unlock(acc.username, self.queue, tx=acc._tx)
            raise
        return self.ctx

    def _format_ctx_error(self, ctx: Ctx, error: Exception | str) -> str:
        message = format_error(error) if isinstance(error, Exception) else error
        return (
            f"{message}; username={ctx.acc.username}; queue={self.queue}; "
            f"backend={getattr(ctx.clt, 'backend', 'unknown')}; proxy={bool(ctx.proxy)}"
        )

    async def _check_rep(self, rep: Response) -> None:
        """
        This function can raise Exception and request will be retried or aborted
        Or if None is returned, response will passed to api parser as is
        """

        if self.debug:
            dump_rep(rep)

        if "text/html" in rep.headers.get("content-type", "") and rep.status_code >= 400:
            src = "Cloudflare" if "cf-ray" in rep.headers else "HTML"
            msg = f"Blocked by {src}: {rep.status_code} - {req_id(rep)}"
            logger.warning(msg)
            if rep.status_code == 429:
                # Edge rate limit, not an account problem: back off and retry same account
                raise BlockedError(msg, retry_after_seconds(rep))
            raise AbortReqError(msg)

        try:
            res = rep.json()
        except json.JSONDecodeError:
            res: Any = {"_raw": rep.text}

        limit_remaining = int(rep.headers.get("x-rate-limit-remaining", -1))
        limit_reset = int(rep.headers.get("x-rate-limit-reset", -1))
        # limit_max = int(rep.headers.get("x-rate-limit-limit", -1))

        errors: list[str] = []
        if isinstance(res, dict) and "errors" in res:
            errors = [f"({x.get('code', -1)}) {x['message']}" for x in res["errors"]]
            errors = list(dict.fromkeys(errors))

        err_msg = "; ".join(errors) or "OK"
        log_key = (self.queue, rep.status_code, frozenset(errors))

        # Request logs identify an account; summaries group errors across accounts.
        request_log = f"{rep.status_code:3d} - {req_id(rep)} - {err_msg}"
        summary_log = f"{rep.status_code:3d} - {self.queue} - {err_msg}"
        logger.trace(request_log)

        # for dev: need to add some features in api.py
        if has_error(errors, "(336) The following features cannot be null"):
            raise GqlFeaturesOutdatedError(f"Update GQL_FEATURES in api.py: {err_msg}")

        # general api rate limit
        if limit_remaining == 0 and limit_reset > 0:
            logger.debug(f"Rate limited: {request_log}")
            await self._close_ctx(limit_reset)
            raise HandledError()

        # no way to check is account banned in direct way, but this check should work
        if has_error(errors, "(88) Rate limit exceeded") and limit_remaining > 0:
            logger.warning(f"Ban detected: {request_log}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if has_error(errors, "(326) Authorization: Denied by access control"):
            logger.warning(f"Ban detected: {request_log}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if has_error(errors, "(32) Could not authenticate you"):
            logger.warning(f"Session expired or banned: {request_log}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg == "OK" and rep.status_code == 403:
            logger.warning(f"Session expired or banned: {request_log}")
            await self._close_ctx(-1, inactive=True, msg=None)
            raise HandledError()

        # content not found
        if rep.status_code == 200 and "_Missing: No status found with that ID" in err_msg:
            return  # ignore this error

        if err_msg != "OK":
            if has_data(rep, res):
                LogOnce.throttled(log_key, "DEBUG", f"API warning with data: {summary_log}")
                return

            # X is overloaded or timed out; retry without penalizing the account.
            if has_error(errors, "(-1) LoadShed") or has_error(errors, "(-1) DeadlineExceeded"):
                LogOnce.throttled(log_key, "WARNING", f"API busy: {summary_log}")
                ctx = self.ctx
                if ctx is not None and await ctx.retry(FailKind.LOADSHED):
                    raise HandledError()
            else:
                LogOnce.once(log_key, "WARNING", f"API unknown error: {summary_log}")

            # Without data the parser would find nothing, and the caller would read
            # a failure as an empty result.
            raise ApiError(f"API error: {summary_log}", errors)

        try:
            rep.raise_for_status()
        except HttpStatusError:
            logger.error(f"Unhandled API response code: {request_log}")
            await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes
            raise HandledError()

    async def get(self, url: str, params: ReqParams = None) -> Response | None:
        return await self.req("GET", url, params=params)

    async def req(self, method: HttpMethod, url: str, params: ReqParams = None) -> Response | None:
        healed = self.healed_features.setdefault(self.queue, set())
        if healed:
            add_missing_features(params, list(healed))

        features_retried = False
        self._xclid_failed_accounts = 0

        while True:
            # 1. same ctx until _close_ctx() clears it — that's retry vs rotate
            # 2. no aclose() needed here, __aexit__ handles it
            ctx = await self._get_ctx()
            if ctx is None:
                return None

            if not has_required_cookies(ctx.acc.cookies):
                msg = "Missing authentication cookies"
                logger.warning(self._format_ctx_error(ctx, msg))
                await self._close_ctx(inactive=True, msg=msg)
                continue

            try:
                source = telemetry.current_source()
                telemetry.capture(
                    "gql_request",
                    {
                        "operation": self.queue,
                        "http_method": method,
                        "http_backend": getattr(ctx.clt, "backend", "unknown"),
                        "source": source,
                        "$current_url": f"{source}://twscrape/gql/{self.queue}",
                    },
                )
                rep = await ctx.req(method, url, params=params)
                setattr(rep, "__username", ctx.acc.username)
                await self._check_rep(rep)

                ctx.req_count += 1  # count only successful
                ctx.fails = dict.fromkeys(ctx.fails, 0)  # budget is per request, not per crawl
                await self._reset_fail_counts(ctx.acc)
                return rep
            except GqlFeaturesOutdatedError as e:
                # X names the flags it wants, so set them and retry once. GQL_FEATURES still
                # needs a permanent update — the warning says so — but a stale dict shouldn't
                # take every caller down until the next release.
                missing = parse_missing_features(str(e))
                if features_retried or not add_missing_features(params, missing):
                    LogOnce.once((self.queue, str(e)), "ERROR", f"[DEV] {e}")
                    raise

                healed.update(missing)
                features_retried = True
                LogOnce.once(
                    (self.queue, "healed", frozenset(missing)),
                    "WARNING",
                    f"[DEV] Update required: {self.queue} needs GQL_FEATURES {missing}, "
                    "set true for now. Update GQL_FEATURES in api.py to fix this permanently.",
                )
                continue
            except XClIdRetryExhaustedError as e:
                await self._xclid_failure(ctx, e, XCLID_FAIL_LOCK_SECONDS)
                continue
            except AbortReqError:
                # abort all queries; raise rather than return None, which callers
                # can't tell from "no data"
                raise
            except HandledError:
                # retry with new account
                continue
            except BlockedError as e:
                # rotating accounts does not help an edge-level limit, so wait it out first
                if await ctx.retry(FailKind.BLOCKED, delay=e.retry_after):
                    logger.info(f"Retrying after edge rate limit; delay={e.retry_after}")
                    continue

                logger.error(f"Edge rate limit persists; aborting {method} {url}")
                raise AbortReqError(str(e)) from e
            except XClIdAccountError as e:
                # A logged-out session is an account-level problem: persist it and
                # freeze the account for every queue instead of only this one.
                await self._xclid_failure(ctx, e, XCLID_ACCOUNT_FAIL_LOCK_SECONDS)
                continue
            except XClIdParseError as e:
                logger.error(
                    f"{self._format_ctx_error(ctx, e)}; "
                    "Report: https://github.com/vladkens/twscrape/issues"
                )
                await self._close_ctx()
                raise
            except (NetworkError, ConnectError) as e:
                # transport failed, retry same account with backoff, then cool it down and rotate
                if await ctx.retry(FailKind.TRANSPORT):
                    continue

                logger.warning(f"{self._format_ctx_error(ctx, e)}; cooling account for 60s")
                await self._close_ctx(utc.ts() + 60)
                continue
            except Exception as e:
                if ctx.fail(FailKind.UNKNOWN):
                    continue

                # Retry budget for this account is exhausted: persist the failure and
                # freeze (or deactivate) the account so every queue stops picking it.
                await self._unknown_failure(ctx, self._format_ctx_error(ctx, e))
                continue

    async def _reset_fail_counts(self, acc: Account):
        """Clear persisted consecutive-failure counters after a success."""
        if acc.xclid_fail_count:
            await self.pool.reset_fail_count(acc.username, "xclid")
            acc.xclid_fail_count = 0
        if acc.unknown_fail_count:
            await self.pool.reset_fail_count(acc.username, "unknown")
            acc.unknown_fail_count = 0
