import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Literal

from .http import HttpClient
from .http import make_client as _make_http_client
from .models import JSONTrait
from .utils import parse_proxy, utc

TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
LoginMethod = Literal["cookies", "password"]


def has_required_cookies(cookies: dict[str, str]) -> bool:
    return all(cookies.get(name) for name in ("auth_token", "ct0"))


def _parse_dt(value) -> datetime | None:
    """Parse a MySQL DATETIME column, which may come back as str, datetime or None."""
    if isinstance(value, str):
        return utc.from_iso(value)
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return None


@dataclass
class Account(JSONTrait):
    username: str
    password: str
    email: str
    email_password: str
    user_agent: str
    active: bool
    locks: dict[str, datetime] = field(default_factory=dict)  # queue: datetime
    stats: dict[str, int] = field(default_factory=dict)  # queue: requests
    headers: dict[str, str] = field(default_factory=dict)
    cookies: dict[str, str] = field(default_factory=dict)
    mfa_code: str | None = None
    proxy: str | None = None
    error_msg: str | None = None
    last_used: datetime | None = None
    _tx: str | None = None
    xclid_fail_count: int = 0  # consecutive x-client-transaction-id 404 retries exhausted
    unknown_fail_count: int = 0  # consecutive unknown request errors
    global_locked_until: datetime | None = None  # frozen for every queue until this time
    can_search: bool | None = None  # False = confirmed blocked on SearchTimeline; NULL = unprobed

    @property
    def has_session(self) -> bool:
        return has_required_cookies(self.cookies)

    @property
    def login_method(self) -> LoginMethod:
        return "cookies" if self.password == "_" else "password"

    @staticmethod
    def from_rs(rs) -> "Account":
        doc = dict(rs)
        for name in ("locks", "stats", "headers", "cookies"):
            value = doc.get(name) or {}
            doc[name] = json.loads(value) if isinstance(value, (str, bytes)) else value
        doc["locks"] = {k: _parse_dt(v) for k, v in doc["locks"].items()}
        doc["stats"] = {k: int(v) for k, v in doc["stats"].items() if isinstance(v, (int, float))}
        doc["active"] = bool(doc["active"])
        for name in ("last_used", "global_locked_until"):
            doc[name] = _parse_dt(doc.get(name))
        for name in ("xclid_fail_count", "unknown_fail_count"):
            doc[name] = int(doc.get(name) or 0)
        value = doc.get("can_search")
        doc["can_search"] = None if value is None else bool(value)
        return Account(**doc)

    def to_rs(self):
        rs = asdict(self)
        rs["locks"] = json.dumps(rs["locks"], default=lambda x: x.isoformat())
        rs["stats"] = json.dumps(rs["stats"])
        rs["headers"] = json.dumps(rs["headers"])
        rs["cookies"] = json.dumps(rs["cookies"])
        # MySQL DATETIME parameters are naive UTC, not ISO strings with offsets.
        for name in ("last_used", "global_locked_until"):
            value = getattr(self, name)
            rs[name] = value.astimezone(timezone.utc).replace(tzinfo=None) if value else None
        return rs

    def resolve_proxy(self, proxy: str | None = None) -> str | None:
        proxies = [proxy, os.getenv("TWS_PROXY"), os.getenv("DOVEXSCRAPE_PROXY"), self.proxy]
        proxies = [x for x in proxies if x is not None]
        return parse_proxy(proxies[0]) if proxies else None

    def make_client(self, proxy: str | None = None) -> HttpClient:
        proxy = self.resolve_proxy(proxy)
        headers = {**self.headers}
        headers["user-agent"] = self.user_agent
        headers["content-type"] = "application/json"
        headers["authorization"] = TOKEN
        headers["x-twitter-active-user"] = "yes"
        headers["x-twitter-client-language"] = "en"
        if "ct0" in self.cookies:
            headers["x-csrf-token"] = self.cookies["ct0"]

        seed = int(hashlib.sha256(self.username.encode()).hexdigest()[:8], 16)
        return _make_http_client(proxy=proxy, headers=headers, cookies=self.cookies, seed=seed)
