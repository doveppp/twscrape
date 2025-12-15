import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from httpx import AsyncClient, AsyncHTTPTransport
from httpx._config import Limits

from .models import JSONTrait
from .utils import utc

TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
GLOBAL_TRANSPORT = AsyncHTTPTransport(
    retries=3,
    limits=Limits(max_connections=100, max_keepalive_connections=100, keepalive_expiry=30),
)


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

    @staticmethod
    def from_rs(rs: dict):
        doc = dict(rs)
        doc["locks"] = {k: utc.from_iso(v) for k, v in json.loads(doc["locks"]).items()}
        doc["stats"] = {k: v for k, v in json.loads(doc["stats"]).items() if isinstance(v, int)}
        doc["headers"] = json.loads(doc["headers"])
        doc["cookies"] = json.loads(doc["cookies"])
        doc["active"] = bool(doc["active"])

        last_used = doc["last_used"]
        if isinstance(last_used, str):
            doc["last_used"] = utc.from_iso(last_used)
        elif isinstance(last_used, datetime):
            if last_used.tzinfo is None:
                doc["last_used"] = last_used.replace(tzinfo=timezone.utc)
            else:
                doc["last_used"] = last_used

        return Account(**doc)

    def to_rs(self):
        rs = asdict(self)
        rs["locks"] = json.dumps(rs["locks"], default=lambda x: x.isoformat())
        rs["stats"] = json.dumps(rs["stats"])
        rs["headers"] = json.dumps(rs["headers"])
        rs["cookies"] = json.dumps(rs["cookies"])
        rs["last_used"] = rs["last_used"].isoformat() if rs["last_used"] else None
        return rs

    def make_client(self, proxy: str | None = None) -> AsyncClient:
        proxies = [proxy, os.getenv("TWS_PROXY"), self.proxy]
        proxies = [x for x in proxies if x is not None]
        proxy = proxies[0] if proxies else None

        client = AsyncClient(proxy=proxy, follow_redirects=True, transport=GLOBAL_TRANSPORT)

        # saved from previous usage
        client.cookies.update(self.cookies)
        client.headers.update(self.headers)

        # default settings
        client.headers["user-agent"] = self.user_agent
        client.headers["content-type"] = "application/json"
        client.headers["authorization"] = TOKEN
        client.headers["x-twitter-active-user"] = "yes"
        client.headers["x-twitter-client-language"] = "en"

        if "ct0" in client.cookies:
            client.headers["x-csrf-token"] = client.cookies["ct0"]

        return client
