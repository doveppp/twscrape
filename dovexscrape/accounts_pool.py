import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TypedDict

from .account import Account, _parse_dt, has_required_cookies
from .db import execute, fetchall, fetchone
from .http import HttpStatusError
from .logger import logger
from .login import LoginConfig, login
from .utils import get_env_bool, parse_cookies, utc


class NoAccountError(Exception):
    pass


class AccountInfo(TypedDict):
    username: str
    login_method: str
    logged_in: bool
    active: bool
    last_used: datetime | None
    total_req: int
    error_msg: str | None


# kind -> accounts column holding the consecutive failure count
_FAIL_COLUMNS = {"xclid": "xclid_fail_count", "unknown": "unknown_fail_count"}


def _read_lock_until(rs) -> datetime | None:
    """Read the `lock_until` value out of a MySQL row (dict or tuple)."""
    if not rs:
        return None
    val = rs["lock_until"] if isinstance(rs, dict) else rs[0]
    return _parse_dt(val)


def guess_delim(line: str):
    lp, rp = tuple([x.strip() for x in line.split("username")])
    return rp[0] if not lp else lp[-1]


class AccountsPool:
    # _order_by: str = "RAND()"
    _order_by: str = "username"

    def __init__(
        self,
        db_file="accounts.db",
        login_config: LoginConfig | None = None,
        raise_when_no_account=False,
        wait_timeout: float | None = None,
        wait_interval: float = 5.0,
    ):
        self._db_file = str(db_file)
        self._login_config = login_config or LoginConfig()
        self._raise_when_no_account = raise_when_no_account
        # When every active account is momentarily locked (in-use or rate-limited),
        # get_for_queue_or_wait polls for up to wait_timeout seconds (every
        # wait_interval) before giving up. This lets a brief in-use lock resolve into
        # a successful acquisition instead of an instant failure, while a real
        # rate-limit is not waited out indefinitely. None keeps the legacy behaviour
        # (raise immediately if raise_when_no_account, otherwise block forever).
        self._wait_timeout = wait_timeout
        self._wait_interval = wait_interval

    @staticmethod
    def _json_path(queue: str):
        queue = queue.replace('"', '\\"')
        return f'$."{queue}"'

    @staticmethod
    def _usernames_where(usernames: list[str]) -> tuple[str, dict[str, str]]:
        params = {f"username_{i}": x for i, x in enumerate(dict.fromkeys(usernames))}
        return ",".join(f"%({x})s" for x in params), params

    async def load_from_file(self, filepath: str, line_format: str):
        line_delim = guess_delim(line_format)
        tokens = line_format.split(line_delim)

        required = {"username", "password", "email", "email_password"}
        if not required.issubset(tokens):
            raise ValueError(f"Invalid line format: {line_format}")

        accounts = []
        with open(filepath) as f:
            lines = f.read().split("\n")
            lines = [x.strip() for x in lines if x.strip()]

            for line in lines:
                data = [x.strip() for x in line.split(line_delim)]
                if len(data) < len(tokens):
                    raise ValueError(f"Invalid line: {line}")

                data = data[: len(tokens)]
                vals = {k: v for k, v in zip(tokens, data) if k != "_"}
                accounts.append(vals)

        for x in accounts:
            await self.add_account(**x)

    async def add_account(
        self,
        username: str,
        password: str,
        email: str,
        email_password: str,
        user_agent: str | None = None,
        proxy: str | None = None,
        cookies: str | None = None,
        mfa_code: str | None = None,
    ):
        qs = "SELECT * FROM accounts WHERE username = %(username)s"
        rs = await fetchone(self._db_file, qs, {"username": username})
        if rs:
            logger.warning(f"Account {username} already exists")
            return

        account = Account(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            user_agent=user_agent or "@chrome",
            active=False,
            locks={},
            stats={},
            headers={},
            cookies=parse_cookies(cookies) if cookies else {},
            proxy=proxy,
            mfa_code=mfa_code,
        )

        if has_required_cookies(account.cookies):
            account.active = True

        await self.save(account)
        logger.info(f"Account {username} added successfully (active={account.active})")

    async def add_account_cookies(self, username: str, cookies: str):
        parsed = parse_cookies(cookies)
        if not has_required_cookies(parsed):
            raise ValueError("Cookies must include auth_token and ct0")

        qs = """
        INSERT INTO accounts (
            username, password, email, email_password, user_agent, active,
            locks, stats, headers, cookies
        )
        VALUES (
            %(username)s, '_', '_', '_', '@chrome', 1,
            '{}', '{}', '{}', %(cookies)s
        ) AS new
        ON DUPLICATE KEY UPDATE
            cookies = new.cookies,
            headers = '{}',
            active = 1,
            error_msg = NULL
        """
        await execute(self._db_file, qs, {"username": username, "cookies": json.dumps(parsed)})
        logger.info(f"Cookies for account {username} updated successfully")

    async def delete_accounts(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        usernames = list(dict.fromkeys(usernames))
        if not usernames:
            logger.warning("No usernames provided")
            return

        placeholders, params = self._usernames_where(usernames)
        qs = f"DELETE FROM accounts WHERE username IN ({placeholders})"
        await execute(self._db_file, qs, params)

    async def delete_inactive(self):
        qs = "DELETE FROM accounts WHERE active = 0"
        await execute(self._db_file, qs)

    async def get(self, username: str):
        qs = "SELECT * FROM accounts WHERE username = %(username)s"
        rs = await fetchone(self._db_file, qs, {"username": username})
        if not rs:
            raise ValueError(f"Account {username} not found")
        return Account.from_rs(rs)

    async def get_all(self):
        qs = "SELECT * FROM accounts"
        rs = await fetchall(self._db_file, qs)
        return [Account.from_rs(x) for x in rs]

    async def get_account(self, username: str):
        qs = "SELECT * FROM accounts WHERE username = %(username)s"
        rs = await fetchone(self._db_file, qs, {"username": username})
        if not rs:
            return None
        return Account.from_rs(rs)

    async def save(self, account: Account):
        data = account.to_rs()
        cols = list(data.keys())

        qs = f"""
        INSERT INTO accounts ({",".join(cols)}) VALUES ({",".join([f"%({x})s" for x in cols])}) AS new
        ON DUPLICATE KEY UPDATE {",".join([f"{x} = new.{x}" for x in cols])}
        """
        await execute(self._db_file, qs, data)

    async def login(self, account: Account):
        try:
            await login(account, cfg=self._login_config)
            logger.info(f"Logged in to {account.username} successfully")
            return True
        except HttpStatusError as e:
            rep = e.response
            logger.error(f"Failed to login '{account.username}': {rep.status_code} - {rep.text}")
            return False
        except Exception as e:
            logger.error(f"Failed to login '{account.username}': {e}")
            return False
        finally:
            await self.save(account)

    # Cookie accounts use password="_" and get their session through add_cookie.
    # Password accounts get their session through login/login_all.
    # Relogin resets only password accounts; cookie accounts wait for another add_cookie.
    async def login_all(self, usernames: list[str] | None = None):
        if usernames is not None and not usernames:
            return {"total": 0, "success": 0, "failed": 0}

        params = None
        if usernames is None:
            qs = "SELECT * FROM accounts WHERE active = 0 AND error_msg IS NULL"
        else:
            placeholders, params = self._usernames_where(usernames)
            qs = f"SELECT * FROM accounts WHERE username IN ({placeholders})"

        rs = await fetchall(self._db_file, qs, params)
        accounts = [Account.from_rs(x) for x in rs]
        accounts = [x for x in accounts if x.login_method == "password"]
        # await asyncio.gather(*[login(x) for x in self.accounts])

        counter = {"total": len(accounts), "success": 0, "failed": 0}
        for i, x in enumerate(accounts, start=1):
            logger.info(f"[{i}/{len(accounts)}] Logging in {x.username} - {x.email}")
            status = await self.login(x)
            counter["success" if status else "failed"] += 1
        return counter

    async def relogin(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        if not usernames:
            logger.warning("No usernames provided")
            return

        placeholders, params = self._usernames_where(usernames)
        qs = f"""
        UPDATE accounts SET
            active = 0,
            locks = json_object(),
            global_locked_until = NULL,
            _tx = NULL,
            last_used = NULL,
            error_msg = NULL,
            headers = json_object(),
            cookies = json_object(),
            user_agent = '@chrome',
            xclid_fail_count = 0,
            unknown_fail_count = 0
        WHERE username IN ({placeholders}) AND password != '_'
        """

        await execute(self._db_file, qs, params)
        await self.login_all(usernames)

    async def relogin_failed(self):
        qs = "SELECT username FROM accounts WHERE active = 0 AND error_msg IS NOT NULL"
        rs = await fetchall(self._db_file, qs)
        await self.relogin([x["username"] for x in rs])

    async def reset_locks(self):
        qs = """
        UPDATE accounts SET
            locks = json_object(),
            global_locked_until = NULL,
            _tx = NULL,
            xclid_fail_count = 0,
            unknown_fail_count = 0
        """
        await execute(self._db_file, qs)

    async def set_active(self, username: str, active: bool):
        qs = "UPDATE accounts SET active = %(active)s WHERE username = %(username)s"
        await execute(self._db_file, qs, {"username": username, "active": 1 if active else 0})

    async def incr_fail_count(self, username: str, kind: str) -> int:
        """Bump a consecutive-failure counter and return the new value.

        Deliberately not guarded by _tx: a failure must never be dropped.
        """
        col = _FAIL_COLUMNS[kind]
        qs = f"UPDATE accounts SET {col} = {col} + 1 WHERE username = %(username)s"
        await execute(self._db_file, qs, {"username": username})

        qs = f"SELECT {col} AS count FROM accounts WHERE username = %(username)s"
        rs = await fetchone(self._db_file, qs, {"username": username})
        return int(rs["count"]) if rs and rs["count"] is not None else 0

    async def reset_fail_count(self, username: str, kind: str):
        col = _FAIL_COLUMNS[kind]
        qs = f"UPDATE accounts SET {col} = 0 WHERE username = %(username)s AND {col} > 0"
        await execute(self._db_file, qs, {"username": username})

    async def lock_global(self, username: str, unlock_at: int, tx: str | None = None):
        """Freeze an account for every queue until unlock_at (unix ts)."""
        qs = """
        UPDATE accounts SET global_locked_until = FROM_UNIXTIME(%(unlock_at)s)
        WHERE username = %(username)s
        """
        params = {"username": username, "unlock_at": unlock_at}
        if tx is not None:
            qs += " AND _tx = %(tx)s"
            params["tx"] = tx
        await execute(self._db_file, qs, params)

    async def lock_until(
        self, username: str, queue: str, unlock_at: int, req_count=0, tx: str | None = None
    ):
        queue_path = self._json_path(queue)
        qs = """
        UPDATE accounts SET
            locks = JSON_SET(locks, %(queue_path)s, FROM_UNIXTIME(%(unlock_at)s)),
            stats = JSON_SET(stats, %(queue_path)s, COALESCE(JSON_EXTRACT(stats, %(queue_path)s), 0) + %(req_count)s),
            last_used = NOW()
        WHERE username = %(username)s
        """
        params = {
            "username": username,
            "queue_path": queue_path,
            "unlock_at": unlock_at,
            "req_count": req_count,
        }
        if tx is not None:
            qs += " AND _tx = %(tx)s"
            params["tx"] = tx
        await execute(self._db_file, qs, params)

    async def unlock(self, username: str, queue: str, req_count=0, tx: str | None = None):
        queue_path = self._json_path(queue)
        qs = """
        UPDATE accounts SET
            locks = JSON_REMOVE(locks, %(queue_path)s),
            stats = JSON_SET(stats, %(queue_path)s, COALESCE(JSON_EXTRACT(stats, %(queue_path)s), 0) + %(req_count)s),
            last_used = NOW(),
            _tx = NULL
        WHERE username = %(username)s
        """
        params = {"username": username, "queue_path": queue_path, "req_count": req_count}
        if tx is not None:
            qs += " AND _tx = %(tx)s"
            params["tx"] = tx
        await execute(self._db_file, qs, params)

    async def get_for_queue(self, queue: str) -> Account | None:
        queue_path = self._json_path(queue)
        tx = uuid.uuid4().hex

        # MySQL has no RETURNING, so the picked row is marked with _tx and read back.
        qs = f"""
        UPDATE accounts SET
            locks = JSON_SET(locks, %(queue_path)s, DATE_ADD(NOW(), INTERVAL 15 MINUTE)),
            last_used = NOW(),
            _tx = %(tx)s
        WHERE active = 1
          AND (locks IS NULL
               OR JSON_EXTRACT(locks, %(queue_path)s) IS NULL
               OR CAST(JSON_UNQUOTE(JSON_EXTRACT(locks, %(queue_path)s)) AS DATETIME) < NOW())
          AND (global_locked_until IS NULL OR global_locked_until < NOW())
        ORDER BY {self._order_by}
        LIMIT 1
        """
        await execute(self._db_file, qs, {"queue_path": queue_path, "tx": tx})

        qs = "SELECT * FROM accounts WHERE _tx = %(tx)s"
        rs = await fetchone(self._db_file, qs, {"tx": tx})
        return Account.from_rs(rs) if rs else None

    async def get_for_queue_or_wait(self, queue: str) -> Account | None:
        start = utc.now()
        msg_shown = False
        while True:
            account = await self.get_for_queue(queue)
            if account is not None:
                if msg_shown:
                    logger.info(f"Continuing with account {account.username} on queue {queue}")
                return account

            raise_no_account = self._raise_when_no_account or get_env_bool(
                "DOVEXSCRAPE_RAISE_WHEN_NO_ACCOUNT"
            )

            # next_available_at returns None only when no active account exists at all,
            # in which case waiting is futile. A timestamp means active accounts exist
            # but are all locked right now (in-use or rate-limited).
            nat = await self.next_available_at(queue)
            no_active = not nat

            # Give up (raise / stop) when: no active account exists, the caller opted
            # out of waiting (wait_timeout is None), or the wait budget is exhausted.
            elapsed = (utc.now() - start).total_seconds()
            give_up = no_active or self._wait_timeout is None or elapsed >= self._wait_timeout
            if give_up:
                if raise_no_account:
                    raise NoAccountError(f"No account available for queue {queue}")
                if no_active:
                    logger.warning("No active accounts. Stopping...")
                    return None
                if self._wait_timeout is not None:
                    return None
                # wait_timeout is None and not raising: fall through to the legacy
                # unbounded wait below.

            if not msg_shown:
                logger.info(f'No account available for queue "{queue}". Next available at {nat}')
                msg_shown = True

            await asyncio.sleep(self._wait_interval)

    async def next_available_at(self, queue: str):
        queue_path = self._json_path(queue)
        qs = """
        SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(locks, %(queue_path)s)) AS DATETIME) AS lock_until
        FROM accounts
        WHERE active = 1 AND JSON_EXTRACT(locks, %(queue_path)s) IS NOT NULL
        ORDER BY lock_until ASC
        LIMIT 1
        """
        rs = await fetchone(self._db_file, qs, {"queue_path": queue_path})
        queue_lock = _read_lock_until(rs)

        # an account can also be frozen for every queue at once (see QueueClient 404 handling)
        qs = """
        SELECT MIN(global_locked_until) AS lock_until
        FROM accounts
        WHERE active = 1 AND global_locked_until IS NOT NULL
        """
        rs = await fetchone(self._db_file, qs)
        global_lock = _read_lock_until(rs)

        upcoming = [x for x in (queue_lock, global_lock) if x is not None]
        if not upcoming:
            return None

        trg = min(upcoming)
        now = utc.now()
        if trg < now:
            return "now"

        at_local = datetime.now() + (trg - now)
        return at_local.strftime("%H:%M:%S")

    async def mark_inactive(self, username: str, error_msg: str | None, tx: str | None = None):
        qs = """
        UPDATE accounts SET active = 0, error_msg = %(error_msg)s, _tx = NULL,
            xclid_fail_count = 0, unknown_fail_count = 0
        WHERE username = %(username)s
        """
        params = {"username": username, "error_msg": error_msg}
        if tx is not None:
            qs += " AND _tx = %(tx)s"
            params["tx"] = tx
        await execute(self._db_file, qs, params)

    async def stats(self):
        rows = await fetchall(self._db_file, "SELECT locks FROM accounts")
        keys: set[str] = set()
        for r in rows:
            locks = r["locks"]
            if isinstance(locks, str):
                try:
                    locks = json.loads(locks)
                except json.JSONDecodeError:
                    locks = {}
            if isinstance(locks, dict):
                keys.update(locks.keys())

        config: list[tuple[str, str, dict]] = [
            ("total", "SELECT COUNT(*) FROM accounts", {}),
            ("active", "SELECT COUNT(*) FROM accounts WHERE active = 1", {}),
            ("inactive", "SELECT COUNT(*) FROM accounts WHERE active = 0", {}),
            (
                "globally_locked",
                "SELECT COUNT(*) FROM accounts WHERE active = 1 AND global_locked_until > NOW()",
                {},
            ),
        ]

        for i, queue in enumerate(sorted(keys)):
            p = f"queue_path_{i}"
            path = self._json_path(queue)
            config.append(
                (
                    f"locked_{queue.replace('`', '')}",
                    "SELECT COUNT(*) FROM accounts"
                    f" WHERE JSON_EXTRACT(locks, %({p})s) IS NOT NULL"
                    f" AND CAST(JSON_UNQUOTE(JSON_EXTRACT(locks, %({p})s)) AS DATETIME) > NOW()",
                    {p: path},
                )
            )

        parts, params = [], {}
        for key, q, qp in config:
            parts.append(f"({q}) AS `{key}`")
            params.update(qp)

        rs = await fetchone(self._db_file, f"SELECT {','.join(parts)}", params)
        return dict(rs) if rs else {}

    async def accounts_info(self):
        accounts = await self.get_all()

        items: list[AccountInfo] = []
        for x in accounts:
            item: AccountInfo = {
                "username": x.username,
                "logged_in": x.has_session,
                "login_method": x.login_method,
                "active": x.active,
                "last_used": x.last_used,
                "total_req": sum(x.stats.values()),
                "error_msg": str(x.error_msg)[0:60],
            }
            items.append(item)

        old_time = datetime(1970, 1, 1).replace(tzinfo=timezone.utc)
        items = sorted(items, key=lambda x: x["username"].lower())
        items = sorted(
            items,
            key=lambda x: x["last_used"] or old_time if x["total_req"] > 0 else old_time,
            reverse=True,
        )
        items = sorted(items, key=lambda x: x["active"], reverse=True)
        # items = sorted(items, key=lambda x: x["total_req"], reverse=True)
        return items
