import asyncio

import pytest

from twscrape.accounts_pool import AccountsPool, NoAccountError
from twscrape.api import API
from twscrape.utils import utc


async def test_add_accounts(pool_mock: AccountsPool):
    # should add account
    await pool_mock.add_account("user1", "pass1", "email1", "email_pass1")
    acc = await pool_mock.get("user1")
    assert acc.username == "user1"
    assert acc.password == "pass1"
    assert acc.email == "email1"
    assert acc.email_password == "email_pass1"
    acc.email_password = "_"
    assert acc.login_method == "password"

    # should not add account with same username
    await pool_mock.add_account("user1", "pass2", "email2", "email_pass2")
    acc = await pool_mock.get("user1")
    assert acc.username == "user1"
    assert acc.password == "pass1"
    assert acc.email == "email1"
    assert acc.email_password == "email_pass1"

    # should not add account with different username case
    await pool_mock.add_account("USER1", "pass2", "email2", "email_pass2")
    acc = await pool_mock.get("user1")
    assert acc.username == "user1"
    assert acc.password == "pass1"
    assert acc.email == "email1"
    assert acc.email_password == "email_pass1"

    # should add account with different username
    await pool_mock.add_account("user2", "pass2", "email2", "email_pass2")
    acc = await pool_mock.get("user2")
    assert acc.username == "user2"
    assert acc.password == "pass2"
    assert acc.email == "email2"
    assert acc.email_password == "email_pass2"


async def test_add_account_cookies(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=token; ct0=csrf")
    acc = await pool_mock.get("user1")

    assert acc.active is True
    assert acc.has_session is True
    assert acc.login_method == "cookies"


@pytest.mark.parametrize(
    "cookies",
    ["auth_token=token", "ct0=csrf", "other=value"],
)
async def test_add_account_cookies_requires_full_session(pool_mock: AccountsPool, cookies: str):
    with pytest.raises(ValueError):
        await pool_mock.add_account_cookies("user1", cookies)
    assert await pool_mock.get_account("user1") is None


async def test_add_account_cookies_existing(pool_mock: AccountsPool):
    await pool_mock.add_account(
        "user1",
        "pass",
        "email",
        "email-pass",
        proxy="http://proxy.test",
        mfa_code="mfa-secret",
        cookies="auth_token=token; ct0=csrf",
    )
    acc = await pool_mock.get("user1")
    acc.stats = {"SearchTimeline": 3}
    await pool_mock.save(acc)

    await pool_mock.add_account_cookies("user1", "auth_token=other; ct0=other-csrf")
    same = await pool_mock.get("user1")

    assert same.cookies == {"auth_token": "other", "ct0": "other-csrf"}
    assert same.active is True
    assert same.login_method == "password"
    assert same.password == "pass"
    assert same.proxy == acc.proxy
    assert same.stats == acc.stats


async def test_get_all(pool_mock: AccountsPool):
    # should return empty list
    accs = await pool_mock.get_all()
    assert len(accs) == 0

    # should return all accounts
    await pool_mock.add_account("user1", "pass1", "email1", "email_pass1")
    await pool_mock.add_account("user2", "pass2", "email2", "email_pass2")
    accs = await pool_mock.get_all()
    assert len(accs) == 2
    assert accs[0].username == "user1"
    assert accs[1].username == "user2"


async def test_save(pool_mock: AccountsPool):
    # should save account
    await pool_mock.add_account("user1", "pass1", "email1", "email_pass1")
    acc = await pool_mock.get("user1")
    acc.password = "pass2"
    await pool_mock.save(acc)
    acc = await pool_mock.get("user1")
    assert acc.password == "pass2"

    # should not save account
    acc = await pool_mock.get("user1")
    acc.username = "user2"
    await pool_mock.save(acc)
    acc = await pool_mock.get("user1")
    assert acc.username == "user1"


async def test_get_for_queue(pool_mock: AccountsPool):
    Q = "test_queue"

    # should return account
    await pool_mock.add_account("user1", "pass1", "email1", "email_pass1")
    await pool_mock.set_active("user1", True)
    acc = await pool_mock.get_for_queue(Q)
    assert acc is not None
    assert acc.username == "user1"
    assert acc.active is True
    assert acc.locks is not None
    assert Q in acc.locks
    assert acc.locks[Q] is not None

    # should return None
    acc = await pool_mock.get_for_queue(Q)
    assert acc is None


async def test_account_unlock(pool_mock: AccountsPool):
    Q = "test_queue"

    await pool_mock.add_account("user1", "pass1", "email1", "email_pass1")
    await pool_mock.set_active("user1", True)
    acc = await pool_mock.get_for_queue(Q)
    assert acc is not None
    assert acc.locks[Q] is not None

    # should unlock account and make available for queue
    await pool_mock.unlock(acc.username, Q)
    acc = await pool_mock.get_for_queue(Q)
    assert acc is not None
    assert acc.locks[Q] is not None

    # should update lock time
    end_time = utc.ts() + 60  # + 1 minute
    await pool_mock.lock_until(acc.username, Q, end_time)

    acc = await pool_mock.get(acc.username)
    assert int(acc.locks[Q].timestamp()) == end_time


async def test_get_stats(pool_mock: AccountsPool):
    Q = "SearchTimeline"

    # should return empty stats
    stats = await pool_mock.stats()
    for k, v in stats.items():
        assert v == 0, f"{k} should be 0"

    # should increate total
    await pool_mock.add_account("user1", "pass1", "email1", "email_pass1")
    stats = await pool_mock.stats()
    assert stats["total"] == 1
    assert stats["active"] == 0

    # should increate active
    await pool_mock.set_active("user1", True)
    stats = await pool_mock.stats()
    assert stats["total"] == 1
    assert stats["active"] == 1

    # should update queue stats
    acc = await pool_mock.get_for_queue(Q)
    assert acc is not None
    stats = await pool_mock.stats()
    assert stats["total"] == 1
    assert stats["active"] == 1
    assert stats[f"locked_{Q}"] == 1


async def test_delete_accounts(pool_mock: AccountsPool):
    await pool_mock.add_account("user1", "pass1", "email1", "ep1")
    await pool_mock.add_account("user2", "pass2", "email2", "ep2")

    await pool_mock.delete_accounts("user1")
    usernames = {x.username for x in await pool_mock.get_all()}
    assert "user1" not in usernames
    assert "user2" in usernames

    await pool_mock.delete_accounts(["user2"])
    assert len(await pool_mock.get_all()) == 0


async def test_username_lists_are_parameterized(pool_mock: AccountsPool, monkeypatch):
    username = 'user"quoted'
    await pool_mock.add_account(username, "pass", "email", "ep")

    async def fake_login(account):
        return True

    monkeypatch.setattr(pool_mock, "login", fake_login)
    assert await pool_mock.login_all([username]) == {"total": 1, "success": 1, "failed": 0}

    await pool_mock.relogin(username)
    assert (await pool_mock.get(username)).active is False

    await pool_mock.delete_accounts([username])
    assert await pool_mock.get_all() == []


async def test_login_all_empty_username_list_is_noop(pool_mock: AccountsPool):
    assert await pool_mock.login_all([]) == {"total": 0, "success": 0, "failed": 0}


async def test_relogin_skips_cookie_accounts(pool_mock: AccountsPool, monkeypatch):
    await pool_mock.add_account_cookies("user1", "auth_token=token; ct0=csrf")

    async def fail_if_called(account):
        pytest.fail("cookie account entered password login")

    monkeypatch.setattr(pool_mock, "login", fail_if_called)
    await pool_mock.relogin("user1")

    assert (await pool_mock.get("user1")).active is True


async def test_delete_inactive(pool_mock: AccountsPool):
    await pool_mock.add_account("user1", "pass1", "email1", "ep1")
    await pool_mock.add_account("user2", "pass2", "email2", "ep2")
    await pool_mock.set_active("user2", True)

    await pool_mock.delete_inactive()
    accs = await pool_mock.get_all()
    assert len(accs) == 1
    assert accs[0].username == "user2"


async def test_reset_locks(pool_mock: AccountsPool):
    Q = "TestQueue"
    await pool_mock.add_account("user1", "pass1", "email1", "ep1")
    await pool_mock.set_active("user1", True)

    await pool_mock.get_for_queue(Q)
    user = await pool_mock.get("user1")
    assert Q in user.locks

    await pool_mock.reset_locks()
    user = await pool_mock.get("user1")
    assert Q not in user.locks


async def test_mark_inactive(pool_mock: AccountsPool):
    await pool_mock.add_account("user1", "pass1", "email1", "ep1")
    await pool_mock.set_active("user1", True)

    await pool_mock.mark_inactive("user1", "banned by system")
    acc = await pool_mock.get("user1")
    assert acc.active is False
    assert acc.error_msg == "banned by system"


async def test_reactivate(pool_mock: AccountsPool, monkeypatch):
    await pool_mock.add_account("user1", "pass1", "email1", "ep1", cookies="auth_token=a; ct0=b")
    await pool_mock.add_account("user2", "pass2", "email2", "ep2")
    for x in ("user1", "user2"):
        await pool_mock.mark_inactive(x, "banned by system")

    warnings = []
    monkeypatch.setattr("twscrape.accounts_pool.logger.warning", warnings.append)
    await pool_mock.reactivate(["user1", "user2", "nope"])

    acc = await pool_mock.get("user1")
    assert acc.active is True
    assert acc.error_msg is None

    # without session cookies the account stays inactive, an unknown username is skipped
    acc = await pool_mock.get("user2")
    assert acc.active is False
    assert acc.error_msg == "banned by system"
    assert warnings == [
        "Account nope not found",
        "Account user2 has no session, use relogin or add_cookie",
    ]


async def test_reactivate_one_username(pool_mock: AccountsPool):
    await pool_mock.add_account("user1", "pass1", "email1", "ep1", cookies="auth_token=a; ct0=b")
    await pool_mock.mark_inactive("user1", "banned by system")

    await pool_mock.reactivate("user1")
    acc = await pool_mock.get("user1")
    assert acc.active is True


async def test_next_available_at_none_when_empty(pool_mock: AccountsPool):
    assert await pool_mock.next_available_at("TestQueue") is None


async def test_next_available_at_returns_future_time(pool_mock: AccountsPool):
    Q = "TestQueue"
    await pool_mock.add_account("user1", "pass1", "email1", "ep1")
    await pool_mock.set_active("user1", True)
    await pool_mock.lock_until("user1", Q, utc.ts() + 3600)

    result = await pool_mock.next_available_at(Q)
    assert result is not None
    assert result != "now"


async def test_get_for_queue_or_wait_raises_when_flag_set(pool_mock: AccountsPool):
    pool = AccountsPool(pool_mock._db_file, raise_when_no_account=True)
    with pytest.raises(NoAccountError):
        await pool.get_for_queue_or_wait("TestQueue")


async def test_get_for_queue_or_wait_raises_via_env(pool_mock: AccountsPool, monkeypatch):
    monkeypatch.setenv("TWS_RAISE_WHEN_NO_ACCOUNT", "1")
    with pytest.raises(NoAccountError):
        await pool_mock.get_for_queue_or_wait("TestQueue")


async def test_get_for_queue_or_wait_waits_for_locked_account(pool_mock: AccountsPool, monkeypatch):
    queue = "TestQueue"
    pool = AccountsPool(pool_mock._db_file, wait_timeout=1, wait_interval=0.1)
    await pool.add_account("user1", "pass1", "email1", "ep1")
    await pool.set_active("user1", True)
    await pool.get_for_queue(queue)

    intervals = []

    async def release_account(interval):
        intervals.append(interval)
        await pool.unlock("user1", queue)

    monkeypatch.setattr("twscrape.accounts_pool.asyncio.sleep", release_account)

    account = await pool.get_for_queue_or_wait(queue)

    assert account is not None
    assert account.username == "user1"
    assert intervals == [0.1]


async def test_get_for_queue_or_wait_returns_none_when_timeout_is_zero(pool_mock: AccountsPool):
    queue = "TestQueue"
    pool = AccountsPool(pool_mock._db_file, wait_timeout=0)
    await pool.add_account("user1", "pass1", "email1", "ep1")
    await pool.set_active("user1", True)
    await pool.get_for_queue(queue)

    assert await pool.get_for_queue_or_wait(queue) is None


async def test_get_for_queue_or_wait_does_not_wait_without_active_accounts(
    pool_mock: AccountsPool, monkeypatch
):
    pool = AccountsPool(pool_mock._db_file, wait_timeout=1)

    async def fail_if_called(_):
        pytest.fail("should not wait when no accounts are active")

    monkeypatch.setattr("twscrape.accounts_pool.asyncio.sleep", fail_if_called)

    assert await pool.get_for_queue_or_wait("TestQueue") is None


async def test_api_passes_wait_config_to_new_pool(db_dsn):
    api = API(db_dsn, wait_timeout=1, wait_interval=0.1)
    try:
        assert api.pool._wait_timeout == 1
        assert api.pool._wait_interval == 0.1
    finally:
        await api.pool.aclose()


async def test_accounts_info_active_first(pool_mock: AccountsPool):
    await pool_mock.add_account("user_b", "pass", "email", "ep")
    await pool_mock.add_account("user_a", "pass", "email", "ep")
    await pool_mock.set_active("user_a", True)

    items = await pool_mock.accounts_info()
    assert items[0]["username"] == "user_a"
    assert items[0]["active"] is True
    assert items[1]["username"] == "user_b"
    assert items[1]["active"] is False


async def test_load_from_file(pool_mock: AccountsPool, tmp_path):
    filepath = tmp_path / "accounts.txt"
    filepath.write_text("user1:pass1:email1:ep1\nuser2:pass2:email2:ep2\n")

    await pool_mock.load_from_file(str(filepath), "username:password:email:email_password")
    usernames = {x.username for x in await pool_mock.get_all()}
    assert "user1" in usernames
    assert "user2" in usernames


# --- MySQL integration: leases and persisted health ---


async def test_concurrent_acquisition_has_one_owner(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    accounts = await asyncio.gather(*(pool_mock.get_for_queue("SearchTimeline") for _ in range(12)))
    owners = [x for x in accounts if x is not None]
    assert len(owners) == 1
    assert owners[0]._tx

    # another queue must not steal the first queue's fencing token
    assert await pool_mock.get_for_queue("TweetDetail") is None
    assert await pool_mock.next_available_at("TweetDetail") is not None


async def test_stale_owner_cannot_change_account_state(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    first = await pool_mock.get_for_queue("SearchTimeline")
    await pool_mock.reset_locks()
    second = await pool_mock.get_for_queue("SearchTimeline")
    assert first is not None and second is not None
    assert first._tx != second._tx

    await pool_mock.unlock("user1", "SearchTimeline", 100, tx=first._tx)
    await pool_mock.lock_until("user1", "SearchTimeline", utc.ts() + 9999, tx=first._tx)
    await pool_mock.mark_inactive("user1", "stale", tx=first._tx)

    actual = await pool_mock.get("user1")
    assert actual._tx == second._tx
    assert actual.active is True
    assert actual.stats == {}

    await pool_mock.unlock("user1", "SearchTimeline", 2, tx=second._tx)
    actual = await pool_mock.get("user1")
    assert actual._tx is None
    assert actual.stats["SearchTimeline"] == 2


async def test_failure_counts_are_atomic_and_persisted(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    counts = await asyncio.gather(*(pool_mock.incr_fail_count("user1", "xclid") for _ in range(10)))
    assert sorted(counts) == list(range(1, 11))

    # a fresh pool (new connections) sees the persisted value
    other = AccountsPool(pool_mock._db_file)
    try:
        assert (await other.get("user1")).xclid_fail_count == 10
    finally:
        await other.aclose()

    await pool_mock.reset_fail_count("user1", "xclid")
    assert (await pool_mock.get("user1")).xclid_fail_count == 0
    with pytest.raises(KeyError):
        await pool_mock.incr_fail_count("user1", "sql-injection")


async def test_global_lock_blocks_every_queue(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    await pool_mock.lock_global("user1", utc.ts() + 3600)

    assert await pool_mock.get_for_queue("SearchTimeline") is None
    assert await pool_mock.get_for_queue("TweetDetail") is None
    assert await pool_mock.next_available_at("TweetDetail") not in (None, "now")

    await pool_mock.lock_global("user1", utc.ts() - 1)
    assert await pool_mock.get_for_queue("SearchTimeline") is not None


async def test_queue_name_is_parameterized(pool_mock: AccountsPool):
    queue = 'queue.\\"quoted'
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    account = await pool_mock.get_for_queue(queue)
    assert account is not None
    assert queue in account.locks

    await pool_mock.unlock("user1", queue, 1, tx=account._tx)
    assert (await pool_mock.get("user1")).stats == {queue: 1}


async def test_mark_inactive_resets_fail_counts(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    await pool_mock.incr_fail_count("user1", "xclid")
    await pool_mock.incr_fail_count("user1", "unknown")

    await pool_mock.mark_inactive("user1", "banned by system")

    account = await pool_mock.get("user1")
    assert account.active is False
    assert account.xclid_fail_count == 0
    assert account.unknown_fail_count == 0


async def test_reset_locks_clears_global_lock_and_health(pool_mock: AccountsPool):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    await pool_mock.get_for_queue("SearchTimeline")
    await pool_mock.lock_global("user1", utc.ts() + 3600)
    await pool_mock.incr_fail_count("user1", "xclid")

    await pool_mock.reset_locks()

    account = await pool_mock.get("user1")
    assert account.locks == {}
    assert account.global_locked_until is None
    assert account.xclid_fail_count == 0


async def test_reactivate_requires_session_and_clears_health(pool_mock: AccountsPool, monkeypatch):
    await pool_mock.add_account_cookies("user1", "auth_token=a; ct0=b")
    await pool_mock.add_account("user2", "pass2", "email2", "ep2")
    for x in ("user1", "user2"):
        await pool_mock.mark_inactive(x, "banned")
    await pool_mock.incr_fail_count("user1", "xclid")

    warnings = []
    monkeypatch.setattr("twscrape.accounts_pool.logger.warning", warnings.append)
    await pool_mock.reactivate(["user1", "user2", "nope"])

    user1 = await pool_mock.get("user1")
    assert user1.active is True
    assert user1.xclid_fail_count == 0
    user2 = await pool_mock.get("user2")
    assert user2.active is False
    assert warnings == [
        "Account nope not found",
        "Account user2 has no session, use relogin or add_cookie",
    ]


async def test_migration_adds_health_columns_without_losing_rows(db_dsn):
    pool = AccountsPool(db_dsn)
    try:
        await pool.add_account_cookies("user1", "auth_token=t; ct0=c")
        for column in ("xclid_fail_count", "unknown_fail_count", "global_locked_until", "mfa_code"):
            await pool._db.execute(f"ALTER TABLE accounts DROP COLUMN {column}")
        await pool._db.execute("UPDATE _meta SET `value` = '1' WHERE `key` = 'user_version'")
    finally:
        await pool.aclose()

    migrated = AccountsPool(db_dsn)
    try:
        account = await migrated.get("user1")
        assert account.cookies == {"auth_token": "t", "ct0": "c"}
        assert account.xclid_fail_count == 0
        assert account.unknown_fail_count == 0
    finally:
        await migrated.aclose()


async def test_concurrent_acquire_release_does_not_deadlock(pool_mock: AccountsPool):
    for index in range(16):
        await pool_mock.add_account_cookies(
            f"user{index:02d}", f"auth_token=t{index}; ct0=c{index}"
        )

    async def worker(seed):
        for _ in range(20):
            account = await pool_mock.get_for_queue("SearchTimeline")
            if account is not None:
                await pool_mock.unlock(account.username, "SearchTimeline", 1, tx=account._tx)
            await asyncio.sleep(0)

    results = await asyncio.gather(*(worker(seed) for seed in range(32)), return_exceptions=True)
    failures = [x for x in results if isinstance(x, BaseException)]
    assert failures == []


def test_default_db_is_resolved_at_construction(monkeypatch):
    monkeypatch.setenv("TWS_DB", "mysql://user:pass@example.test/custom")
    pool = AccountsPool()
    assert pool._db_file == "mysql://user:pass@example.test/custom"


async def test_no_account_log_includes_active_count(pool_mock: AccountsPool, monkeypatch):
    await pool_mock.add_account_cookies("user1", "auth_token=t; ct0=c")
    await pool_mock.get_for_queue("SearchTimeline")  # lease the only account

    pool = AccountsPool(pool_mock._db_file)
    messages = []

    async def stop(_):
        raise RuntimeError("stop-after-log")

    from twscrape.logger import logger

    monkeypatch.setattr(logger, "info", messages.append)
    monkeypatch.setattr("twscrape.accounts_pool.asyncio.sleep", stop)
    try:
        with pytest.raises(RuntimeError, match="stop-after-log"):
            await pool.get_for_queue_or_wait("SearchTimeline")
    finally:
        await pool.aclose()

    assert len(messages) == 1
    assert messages[0].startswith(
        'No account available for queue "SearchTimeline". Active accounts: 1.'
    )
