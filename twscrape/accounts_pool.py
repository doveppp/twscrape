import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TypedDict

from .account import Account, _parse_dt, has_required_cookies
from .db import DB, default_db
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
    can_search: bool | None


# kind -> accounts column holding the consecutive failure count
_FAIL_COLUMNS = {"xclid": "xclid_fail_count", "unknown": "unknown_fail_count"}


def guess_delim(line: str):
    lp, rp = tuple([x.strip() for x in line.split("username")])
    return rp[0] if not lp else lp[-1]


class AccountsPool:
    # _order_by: str = "RAND()"
    _order_by: str = "username"

    def __init__(
        self,
        db_file: str | None = None,
        login_config: LoginConfig | None = None,
        raise_when_no_account=False,
        wait_timeout: float | None = None,
        wait_interval: float = 5.0,
    ):
        self._db_file = str(db_file or default_db())
        self._db = DB(self._db_file)
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

    async def __aenter__(self):
        await self._db.pool()
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def aclose(self):
        await self._db.aclose()

    @staticmethod
    def _json_path(queue: str):
        return "$." + json.dumps(queue, ensure_ascii=False)

    @staticmethod
    def _usernames_where(usernames: list[str]) -> tuple[str, dict[str, str]]:
        params = {f"username_{i}": x for i, x in enumerate(dict.fromkeys(usernames))}
        return ",".join(f"%({x})s" for x in params), params

    @staticmethod
    def _queue_filter(queue: str) -> str:
        """Fixed queue eligibility. Only SearchTimeline drops a confirmed False."""
        if queue == "SearchTimeline":
            return "AND (can_search IS NULL OR can_search = 1)"
        return ""

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
        if await self.get_account(username):
            logger.warning(f"Account {username} already exists")
            return

        parsed = parse_cookies(cookies) if cookies else {}
        account = Account(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            user_agent=user_agent or "@chrome",
            active=has_required_cookies(parsed),
            locks={},
            stats={},
            headers={},
            cookies=parsed,
            proxy=proxy,
            mfa_code=mfa_code,
        )

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
            error_msg = NULL,
            locks = '{}',
            global_locked_until = NULL,
            _tx = NULL,
            xclid_fail_count = 0,
            unknown_fail_count = 0
        """
        await self._db.execute(qs, {"username": username, "cookies": json.dumps(parsed)})
        logger.info(f"Cookies for account {username} updated successfully")

    async def delete_accounts(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        usernames = list(dict.fromkeys(usernames))
        if not usernames:
            logger.warning("No usernames provided")
            return

        placeholders, params = self._usernames_where(usernames)
        qs = f"DELETE FROM accounts WHERE username IN ({placeholders})"
        await self._db.execute(qs, params)

    async def delete_inactive(self):
        await self._db.execute("DELETE FROM accounts WHERE active = 0")

    async def get(self, username: str):
        account = await self.get_account(username)
        if account is None:
            raise ValueError(f"Account {username} not found")
        return account

    async def get_account(self, username: str):
        qs = "SELECT * FROM accounts WHERE username = %(username)s"
        rs = await self._db.fetchone(qs, {"username": username})
        return Account.from_rs(rs) if rs else None

    async def get_all(self):
        rs = await self._db.fetchall("SELECT * FROM accounts")
        return [Account.from_rs(x) for x in rs]

    async def save(self, account: Account):
        data = account.to_rs()
        cols = list(data.keys())

        qs = f"""
        INSERT INTO accounts ({",".join(cols)})
        VALUES ({",".join([f"%({x})s" for x in cols])}) AS new
        ON DUPLICATE KEY UPDATE {",".join([f"{x} = new.{x}" for x in cols])}
        """
        await self._db.execute(qs, data)

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

        rs = await self._db.fetchall(qs, params)
        accounts = [Account.from_rs(x) for x in rs]
        accounts = [x for x in accounts if x.login_method == "password"]

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

        await self._db.execute(qs, params)
        await self.login_all(usernames)

    async def relogin_failed(self):
        qs = "SELECT username FROM accounts WHERE active = 0 AND error_msg IS NOT NULL"
        rs = await self._db.fetchall(qs)
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
        await self._db.execute(qs)

    async def set_active(self, username: str, active: bool):
        qs = "UPDATE accounts SET active = %(active)s WHERE username = %(username)s"
        await self._db.execute(qs, {"username": username, "active": 1 if active else 0})

    async def set_can_search(self, username: str, can_search: bool | None):
        qs = "UPDATE accounts SET can_search = %(can_search)s WHERE username = %(username)s"
        value = None if can_search is None else (1 if can_search else 0)
        await self._db.execute(qs, {"username": username, "can_search": value})

    async def incr_fail_count(self, username: str, kind: str) -> int:
        """Bump a consecutive-failure counter and return the new value.

        Deliberately not guarded by _tx: a failure must never be dropped.
        """
        column = _FAIL_COLUMNS[kind]
        pool = await self._db.pool()
        async with pool.acquire() as conn, conn.cursor() as cur:
            # LAST_INSERT_ID(expr) returns this connection's exact increment, so two
            # workers hitting the same account cannot read each other's value.
            await cur.execute(
                f"UPDATE accounts SET {column} = LAST_INSERT_ID({column} + 1) "
                "WHERE username = %(username)s",
                {"username": username},
            )
            if not cur.rowcount:
                return 0
            await cur.execute("SELECT LAST_INSERT_ID() AS count")
            return int((await cur.fetchone())["count"])

    async def reset_fail_count(self, username: str, kind: str):
        column = _FAIL_COLUMNS[kind]
        qs = f"UPDATE accounts SET {column} = 0 WHERE username = %(username)s AND {column} > 0"
        await self._db.execute(qs, {"username": username})

    @staticmethod
    def _guard(sql: str, params: dict, tx: str | None):
        if tx is not None:
            sql += " AND _tx = %(tx)s"
            params["tx"] = tx
        return sql, params

    async def lock_global(self, username: str, unlock_at: int, tx: str | None = None):
        """Freeze an account for every queue until unlock_at (unix ts)."""
        qs = """
        UPDATE accounts SET global_locked_until = FROM_UNIXTIME(%(unlock_at)s)
        WHERE username = %(username)s
        """
        params = {"username": username, "unlock_at": unlock_at}
        await self._db.execute(*self._guard(qs, params, tx))

    async def lock_until(
        self,
        username: str,
        queue: str,
        unlock_at: int,
        req_count=0,
        tx: str | None = None,
        *,
        global_lock=False,
    ):
        path = self._json_path(queue)
        qs = """
        UPDATE accounts SET
            locks = JSON_SET(locks, %(path)s, FROM_UNIXTIME(%(unlock_at)s)),
            stats = JSON_SET(stats, %(path)s, COALESCE(JSON_EXTRACT(stats, %(path)s), 0)
                + %(req_count)s),
            last_used = NOW(),
            _tx = NULL,
            global_locked_until = IF(%(global)s, FROM_UNIXTIME(%(unlock_at)s),
                global_locked_until)
        WHERE username = %(username)s
        """
        params = {
            "username": username,
            "path": path,
            "unlock_at": unlock_at,
            "req_count": req_count,
            "global": global_lock,
        }
        await self._db.execute(*self._guard(qs, params, tx))

    async def unlock(self, username: str, queue: str, req_count=0, tx: str | None = None):
        path = self._json_path(queue)
        qs = """
        UPDATE accounts SET
            locks = JSON_REMOVE(locks, %(path)s),
            stats = JSON_SET(stats, %(path)s, COALESCE(JSON_EXTRACT(stats, %(path)s), 0)
                + %(req_count)s),
            last_used = NOW(),
            _tx = NULL
        WHERE username = %(username)s
        """
        params = {"username": username, "path": path, "req_count": req_count}
        await self._db.execute(*self._guard(qs, params, tx))

    async def get_for_queue(self, queue: str) -> Account | None:
        path = self._json_path(queue)
        queue_filter = self._queue_filter(queue)
        pool = await self._db.pool()

        async with pool.acquire() as conn, conn.cursor() as cur:
            # Read candidates without locks, then claim one with a single-row
            # compare-and-set UPDATE. Every statement commits on its own, so a
            # worker never holds a table scan or multiple row locks: this removes
            # the lock-order cycle that made concurrent acquisition deadlock-prone.
            await cur.execute(
                f"""
                SELECT username FROM accounts
                WHERE active = 1
                  AND (JSON_EXTRACT(locks, %(path)s) IS NULL
                       OR CAST(JSON_UNQUOTE(JSON_EXTRACT(locks, %(path)s))
                           AS DATETIME) < NOW())
                  AND (global_locked_until IS NULL OR global_locked_until < NOW())
                  AND (_tx IS NULL OR last_used < DATE_SUB(NOW(), INTERVAL 15 MINUTE))
                  {queue_filter}
                ORDER BY {self._order_by}
                LIMIT 50
                """,
                {"path": path},
            )
            candidates = await cur.fetchall()

            for candidate in candidates:
                tx = uuid.uuid4().hex
                await cur.execute(
                    f"""
                    UPDATE accounts SET
                        locks = JSON_SET(locks, %(path)s,
                            DATE_ADD(NOW(), INTERVAL 15 MINUTE)),
                        last_used = NOW(),
                        _tx = %(tx)s
                    WHERE username = %(username)s
                      AND active = 1
                      AND (JSON_EXTRACT(locks, %(path)s) IS NULL
                           OR CAST(JSON_UNQUOTE(JSON_EXTRACT(locks, %(path)s))
                               AS DATETIME) < NOW())
                      AND (global_locked_until IS NULL OR global_locked_until < NOW())
                      AND (_tx IS NULL OR last_used < DATE_SUB(NOW(), INTERVAL 15 MINUTE))
                      {queue_filter}
                    """,
                    {"path": path, "tx": tx, "username": candidate["username"]},
                )
                if not cur.rowcount:
                    continue

                await cur.execute(
                    "SELECT * FROM accounts WHERE username = %(username)s AND _tx = %(tx)s",
                    {"username": candidate["username"], "tx": tx},
                )
                rs = await cur.fetchone()
                return Account.from_rs(rs) if rs else None

        return None

    async def active_count(self) -> int:
        """Number of accounts with active=1, including currently locked ones."""
        rs = await self._db.fetchone("SELECT COUNT(*) AS count FROM accounts WHERE active = 1")
        return int(rs["count"]) if rs else 0

    async def get_for_queue_or_wait(self, queue: str) -> Account | None:
        start = utc.now()
        msg_shown = False
        while True:
            account = await self.get_for_queue(queue)
            if account is not None:
                if msg_shown:
                    logger.info(f"Continuing with account {account.username} on queue {queue}")
                return account

            raise_no_account = (
                self._raise_when_no_account
                or get_env_bool("TWS_RAISE_WHEN_NO_ACCOUNT")
                or get_env_bool("DOVEXSCRAPE_RAISE_WHEN_NO_ACCOUNT")
            )

            # next_available_at returns None when no account is eligible for this
            # queue (none active, or every active account is blocked for it, e.g.
            # can_search=0 on SearchTimeline), in which case waiting is futile. A
            # timestamp means eligible accounts exist but are all locked right now.
            nat = await self.next_available_at(queue)
            no_eligible = not nat

            # Give up (raise / stop) when: no eligible account exists, the caller
            # opted out of waiting (wait_timeout is None), or the budget is spent.
            elapsed = (utc.now() - start).total_seconds()
            give_up = no_eligible or self._wait_timeout is None or elapsed >= self._wait_timeout
            if give_up:
                if raise_no_account:
                    raise NoAccountError(f"No account available for queue {queue}")
                if no_eligible:
                    logger.warning("No accounts eligible for this queue. Stopping...")
                    return None
                if self._wait_timeout is not None:
                    return None
                # wait_timeout is None and not raising: fall through to the legacy
                # unbounded wait below.

            if not msg_shown:
                active = await self.active_count()
                logger.info(
                    f'No account available for queue "{queue}". '
                    f"Active accounts: {active}. Next available at {nat}"
                )
                msg_shown = True

            await asyncio.sleep(self._wait_interval)

    async def next_available_at(self, queue: str):
        path = self._json_path(queue)
        queue_filter = self._queue_filter(queue)
        qs = f"""
        SELECT MIN(GREATEST(
            COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(locks, %(path)s)) AS DATETIME), NOW()),
            COALESCE(global_locked_until, NOW()),
            IF(_tx IS NULL, NOW(), DATE_ADD(COALESCE(last_used, NOW()), INTERVAL 15 MINUTE))
        )) AS lock_until
        FROM accounts
        WHERE active = 1
          {queue_filter}
        """
        rs = await self._db.fetchone(qs, {"path": path})
        trg = _parse_dt(rs["lock_until"]) if rs else None
        if trg is None:
            return None

        now = utc.now()
        if trg < now:
            return "now"

        at_local = datetime.now() + (trg - now)
        return at_local.strftime("%H:%M:%S")

    async def mark_inactive(self, username: str, error_msg: str | None, tx: str | None = None):
        qs = """
        UPDATE accounts SET
            active = 0,
            error_msg = %(error_msg)s,
            _tx = NULL,
            xclid_fail_count = 0,
            unknown_fail_count = 0
        WHERE username = %(username)s
        """
        params = {"username": username, "error_msg": error_msg}
        await self._db.execute(*self._guard(qs, params, tx))

    async def reactivate(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        if not usernames:
            logger.warning("No usernames provided")
            return

        placeholders, params = self._usernames_where(usernames)
        qs = f"SELECT * FROM accounts WHERE username IN ({placeholders})"
        accounts = [Account.from_rs(x) for x in await self._db.fetchall(qs, params)]
        for x in sorted(set(usernames) - {a.username for a in accounts}):
            logger.warning(f"Account {x} not found")

        # an account without session cookies can't make requests until relogin or add_cookie
        usernames = []
        for x in accounts:
            if has_required_cookies(x.cookies):
                usernames.append(x.username)
            else:
                logger.warning(f"Account {x.username} has no session, use relogin or add_cookie")

        if not usernames:
            return

        placeholders, params = self._usernames_where(usernames)
        qs = f"""
        UPDATE accounts SET
            active = 1,
            error_msg = NULL,
            locks = '{{}}',
            global_locked_until = NULL,
            _tx = NULL,
            xclid_fail_count = 0,
            unknown_fail_count = 0
        WHERE username IN ({placeholders})
        """
        await self._db.execute(qs, params)
        logger.info(f"Reactivated accounts: {', '.join(usernames)}")

    async def stats(self):
        accounts = await self.get_all()
        now = utc.now()
        queues = {q for x in accounts for q in x.locks}

        res = {
            "total": len(accounts),
            "active": sum(x.active for x in accounts),
            "inactive": sum(not x.active for x in accounts),
            "globally_locked": sum(
                bool(x.active and x.global_locked_until and x.global_locked_until > now)
                for x in accounts
            ),
        }
        for queue in queues:
            res[f"locked_{queue}"] = sum(
                bool(x.locks.get(queue) and x.locks[queue] > now) for x in accounts
            )
        return res

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
                "can_search": x.can_search,
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
        return items
