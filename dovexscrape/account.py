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


def _parse_dt(value: str | datetime | None) -> datetime | None:
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

    @property
    def has_session(self) -> bool:
        return has_required_cookies(self.cookies)

    @property
    def login_method(self) -> LoginMethod:
        return "cookies" if self.password == "_" else "password"

    @staticmethod
    def from_rs(rs: dict) -> "Account":
        doc = dict(rs)
        doc["locks"] = {k: utc.from_iso(v) for k, v in json.loads(doc["locks"] or "{}").items()}
        stats = json.loads(doc["stats"] or "{}")
        doc["stats"] = {k: int(v) for k, v in stats.items() if isinstance(v, (int, float))}
        doc["headers"] = json.loads(doc["headers"] or "{}")
        doc["cookies"] = json.loads(doc["cookies"] or "{}")
        doc["active"] = bool(doc["active"])
        doc["last_used"] = _parse_dt(doc.get("last_used"))
        doc["global_locked_until"] = _parse_dt(doc.get("global_locked_until"))
        doc["xclid_fail_count"] = int(doc.get("xclid_fail_count") or 0)
        doc["unknown_fail_count"] = int(doc.get("unknown_fail_count") or 0)
        return Account(**doc)

    def to_rs(self):
        rs = asdict(self)
        rs["locks"] = json.dumps(rs["locks"], default=lambda x: x.isoformat())
        rs["stats"] = json.dumps(rs["stats"])
        rs["headers"] = json.dumps(rs["headers"])
        rs["cookies"] = json.dumps(rs["cookies"])
        rs["last_used"] = rs["last_used"].isoformat() if rs["last_used"] else None
        locked_until = rs["global_locked_until"]
        rs["global_locked_until"] = locked_until.isoformat() if locked_until else None
        return rs

    def resolve_proxy(self, proxy: str | None = None) -> str | None:
        proxies = [proxy, os.getenv("DOVEXSCRAPE_PROXY"), self.proxy]
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
