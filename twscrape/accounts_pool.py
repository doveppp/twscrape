import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TypedDict

from fake_useragent import UserAgent
from httpx import HTTPStatusError

from .account import Account
from .db import execute, fetchall, fetchone
from .logger import logger
from .login import LoginConfig, login
from .utils import get_env_bool, parse_cookies, utc


class NoAccountError(Exception):
    pass


class AccountInfo(TypedDict):
    username: str
    logged_in: bool
    active: bool
    last_used: datetime | None
    total_req: int
    error_msg: str | None


def guess_delim(line: str):
    lp, rp = tuple([x.strip() for x in line.split("username")])
    return rp[0] if not lp else lp[-1]


class AccountsPool:
    _order_by: str = "RAND()"

    def __init__(
        self,
        db_file,
        login_config: LoginConfig | None = None,
        raise_when_no_account=False,
    ):
        self._db_file = db_file
        self._login_config = login_config or LoginConfig()
        self._raise_when_no_account = raise_when_no_account

    async def load_from_file(self, filepath: str, line_format: str):
        line_delim = guess_delim(line_format)
        tokens = line_format.split(line_delim)

        required = set(["username", "password", "email", "email_password"])
        if not required.issubset(tokens):
            raise ValueError(f"Invalid line format: {line_format}")

        accounts = []
        with open(filepath, "r") as f:
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
            user_agent=user_agent or UserAgent().safari,
            active=False,
            locks={},
            stats={},
            headers={},
            cookies=parse_cookies(cookies) if cookies else {},
            proxy=proxy,
            mfa_code=mfa_code,
        )

        if "ct0" in account.cookies:
            account.active = True

        await self.save(account)
        logger.info(f"Account {username} added successfully (active={account.active})")

    async def delete_accounts(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        usernames = list(set(usernames))
        if not usernames:
            logger.warning("No usernames provided")
            return

        # Use single quotes for compatibility with MySQL and standard SQL
        qs = f"""DELETE FROM accounts WHERE username IN ({",".join([f"'{x}'" for x in usernames])})"""
        await execute(self._db_file, qs)

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
        ON DUPLICATE KEY UPDATE {",".join([f"{x}=new.{x}" for x in cols])}
        """
        await execute(self._db_file, qs, data)

    async def login(self, account: Account):
        try:
            await login(account, cfg=self._login_config)
            logger.info(f"Logged in to {account.username} successfully")
            return True
        except HTTPStatusError as e:
            rep = e.response
            logger.error(f"Failed to login '{account.username}': {rep.status_code} - {rep.text}")
            return False
        except Exception as e:
            logger.error(f"Failed to login '{account.username}': {e}")
            return False
        finally:
            await self.save(account)

    async def login_all(self, usernames: list[str] | None = None):
        if usernames is None:
            qs = "SELECT * FROM accounts WHERE active = 0 AND error_msg IS NULL"
        else:
            us = ",".join([f"'{x}'" for x in usernames])
            qs = f"SELECT * FROM accounts WHERE username IN ({us})"

        rs = await fetchall(self._db_file, qs)
        accounts = [Account.from_rs(rs) for rs in rs]
        # await asyncio.gather(*[login(x) for x in self.accounts])

        counter = {"total": len(accounts), "success": 0, "failed": 0}
        for i, x in enumerate(accounts, start=1):
            logger.info(f"[{i}/{len(accounts)}] Logging in {x.username} - {x.email}")
            status = await self.login(x)
            counter["success" if status else "failed"] += 1
        return counter

    async def relogin(self, usernames: str | list[str]):
        usernames = usernames if isinstance(usernames, list) else [usernames]
        usernames = list(set(usernames))
        if not usernames:
            logger.warning("No usernames provided")
            return

        qs = f"""
        UPDATE accounts SET
            active = 0,
            locks = json_object(),
            last_used = NULL,
            error_msg = NULL,
            headers = json_object(),
            cookies = json_object(),
            user_agent = "{UserAgent().safari}"
        WHERE username IN ({",".join([f"'{x}'" for x in usernames])})
        """

        await execute(self._db_file, qs)
        await self.login_all(usernames)

    async def relogin_failed(self):
        qs = "SELECT username FROM accounts WHERE active = 0 AND error_msg IS NOT NULL"
        rs = await fetchall(self._db_file, qs)
        await self.relogin([x["username"] for x in rs])

    async def reset_locks(self):
        qs = "UPDATE accounts SET locks = json_object()"
        await execute(self._db_file, qs)

    async def set_active(self, username: str, active: bool):
        # MySQL boolean is tinyint(1)
        active_val = 1 if active else 0
        qs = "UPDATE accounts SET active = %(active)s WHERE username = %(username)s"
        await execute(self._db_file, qs, {"username": username, "active": active_val})

    async def lock_until(self, username: str, queue: str, unlock_at: int, req_count=0):
        qs = f"""
        UPDATE accounts SET
            locks = JSON_SET(locks, '$.{queue}', FROM_UNIXTIME({unlock_at})),
            stats = JSON_SET(stats, '$.{queue}', COALESCE(JSON_EXTRACT(stats, '$.{queue}'), 0) + {req_count}),
            last_used = FROM_UNIXTIME({utc.ts()})
        WHERE username = %(username)s
        """
        await execute(self._db_file, qs, {"username": username})

    async def unlock(self, username: str, queue: str, req_count=0):
        qs = f"""
        UPDATE accounts SET
            locks = JSON_REMOVE(locks, '$.{queue}'),
            stats = JSON_SET(stats, '$.{queue}', COALESCE(JSON_EXTRACT(stats, '$.{queue}'), 0) + {req_count}),
            last_used = FROM_UNIXTIME({utc.ts()})
        WHERE username = %(username)s
        """
        await execute(self._db_file, qs, {"username": username})

    async def _get_and_lock(self, queue: str, condition: str):
        # if space in condition, it's a subquery, otherwise it's username
        is_subquery = " " in condition

        if is_subquery:
            # Execute subquery directly to get username
            qs_select = condition
        else:
            # Simple username lookup
            qs_select = f"SELECT username FROM accounts WHERE username = '{condition}' LIMIT 1"

        res = await fetchone(self._db_file, qs_select)
        if not res:
            return None
        username = res["username"]

        # Lock the account
        qs_update = f"""
        UPDATE accounts SET
            locks = JSON_SET(locks, '$.{queue}', DATE_ADD(NOW(), INTERVAL 15 MINUTE)),
            last_used = NOW()
        WHERE username = '{username}'
        """
        await execute(self._db_file, qs_update)

        # Get full account data
        qs_get = f"SELECT * FROM accounts WHERE username = '{username}'"
        rs = await fetchone(self._db_file, qs_get)
        return Account.from_rs(rs) if rs else None

    async def get_for_queue(self, queue: str):
        # Build query to find available account
        qs = f"""
        SELECT username FROM accounts
        WHERE active = 1 
          AND (locks IS NULL 
               OR JSON_EXTRACT(locks, '$.{queue}') IS NULL 
               OR JSON_EXTRACT(locks, '$.{queue}') < NOW())
        ORDER BY {self._order_by}
        LIMIT 1
        """
        return await self._get_and_lock(queue, qs)

    async def get_for_queue_or_wait(self, queue: str) -> Account | None:
        msg_shown = False
        while True:
            account = await self.get_for_queue(queue)
            if not account:
                if self._raise_when_no_account or get_env_bool("TWS_RAISE_WHEN_NO_ACCOUNT"):
                    raise NoAccountError(f"No account available for queue {queue}")

                if not msg_shown:
                    nat = await self.next_available_at(queue)
                    if not nat:
                        logger.warning("No active accounts. Stopping...")
                        return None

                    msg = f'No account available for queue "{queue}". Next available at {nat}'
                    logger.info(msg)
                    msg_shown = True

                await asyncio.sleep(5)
                continue
            else:
                if msg_shown:
                    logger.info(f"Continuing with account {account.username} on queue {queue}")

            return account

    async def next_available_at(self, queue: str):
        qs = f"""
        SELECT JSON_EXTRACT(locks, '$."{queue}"') as lock_until
        FROM accounts
        WHERE active = 1 AND JSON_EXTRACT(locks, '$."{queue}"') IS NOT NULL
        ORDER BY lock_until ASC
        LIMIT 1
        """
        rs = await fetchone(self._db_file, qs)
        if rs:
            # For MySQL, rs[0] might be datetime object or string.
            val = rs["lock_until"] if isinstance(rs, dict) else rs[0]
            if val is None:
                return None

            # SQLite returns string "YYYY-MM-DD HH:MM:SS"
            # MySQL might return datetime object

            if isinstance(val, str):
                trg = utc.from_iso(val)
            elif isinstance(val, datetime):
                if val.tzinfo is None:
                    val = val.replace(tzinfo=timezone.utc)  # assume utc
                trg = val
            else:
                # Should not happen
                trg = utc.now()

            now = utc.now()
            if trg < now:
                return "now"

            at_local = datetime.now() + (trg - now)
            return at_local.strftime("%H:%M:%S")

        return None

    async def mark_inactive(self, username: str, error_msg: str | None):
        qs = """
        UPDATE accounts SET active = 0, error_msg = %(error_msg)s
        WHERE username = %(username)s
        """
        await execute(self._db_file, qs, {"username": username, "error_msg": error_msg})

    async def stats(self):
        qs = "SELECT locks FROM accounts WHERE locks IS NOT NULL"
        rows = await fetchall(self._db_file, qs)
        keys = set()
        for r in rows:
            locks = r["locks"]
            if isinstance(locks, str):
                try:
                    locks = json.loads(locks)
                except:
                    locks = {}
            elif isinstance(locks, dict):
                pass
            else:
                locks = {}
            keys.update(locks.keys())
        gql_ops = list(keys)

        def locks_count(queue):
            return f"""
            SELECT COUNT(*) FROM accounts
            WHERE JSON_EXTRACT(locks, '$.{queue}') IS NOT NULL
                AND JSON_EXTRACT(locks, '$.{queue}') > NOW()
            """

        config = [
            ("total", "SELECT COUNT(*) FROM accounts"),
            ("active", "SELECT COUNT(*) FROM accounts WHERE active = 1"),
            ("inactive", "SELECT COUNT(*) FROM accounts WHERE active = 0"),
            *[(f"locked_{x}", locks_count(x)) for x in gql_ops],
        ]

        qs = f"SELECT {','.join([f'({q}) as {k}' for k, q in config])}"
        rs = await fetchone(self._db_file, qs)
        return dict(rs) if rs else {}

    async def accounts_info(self):
        accounts = await self.get_all()

        items: list[AccountInfo] = []
        for x in accounts:
            stats = x.stats
            if isinstance(stats, str):
                try:
                    stats = json.loads(stats)
                except:
                    stats = {}

            item: AccountInfo = {
                "username": x.username,
                "logged_in": (x.headers or {}).get("authorization", "") != "",
                "active": x.active,
                "last_used": x.last_used,
                "total_req": sum(stats.values()) if stats else 0,
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
