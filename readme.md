# dovexscrape

<div align="center">

[<img src="https://badges.ws/pypi/v/dovexscrape" alt="version" />](https://pypi.org/project/dovexscrape)
[<img src="https://badges.ws/pypi/python/dovexscrape" alt="py versions" />](https://pypi.org/project/dovexscrape)
[<img src="https://badges.ws/pypi/dm/dovexscrape" alt="downloads" />](https://pypi.org/project/dovexscrape)
[<img src="https://badges.ws/github/license/doveppp/dovexscrape" alt="license" />](https://github.com/doveppp/dovexscrape/blob/main/LICENSE)
[<img src="https://badges.ws/badge/-/buy%20me%20a%20coffee/ff813f?icon=buymeacoffee&label" alt="donate" />](https://buymeacoffee.com/vladkens)

</div>

dovexscrape is an async Python library and CLI for X/Twitter Search and GraphQL endpoints. It runs on your own account pool, keeps sessions in MySQL, rotates accounts when an endpoint is rate-limited, and returns either parsed SNScrape-style models or raw API responses.

<div align="center">
  <img src=".github/example.png" alt="example of cli usage" height="400px">
</div>

## Install

```bash
pip install dovexscrape
```

`httpx` is the default HTTP backend. For browser-like TLS fingerprinting, install the optional `curl-cffi` backend:

```bash
pip install "dovexscrape[curl]"

DOVEXSCRAPE_HTTP_BACKEND=curl dovexscrape user_by_login xdevelopers
```

## Database

Accounts, sessions, locks and rate-limit counters live in MySQL (8.0.19+), so several processes or
machines can share a single account pool:

```bash
export DOVEXSCRAPE_DB="mysql://user:pass@host:3306/dovexscrape"
dovexscrape accounts
```

Use `--db` to point a single run at another database. The schema (`accounts`, `_meta`) is created on
first connect; tables created by older SQLite-based releases are not migrated, so start from a fresh
database.

Each connection pins the session time zone to UTC, so locks and rate-limit windows stay comparable
even when the server runs with a different `time_zone`. MySQL 8's default `caching_sha2_password`
authentication works out of the box (`cryptography` is installed as a dependency).

## Rate limit

All accounts and queues in a process share one request ceiling, evenly spaced rather than
bursting, which keeps the API and its edge (Cloudflare) calm:

```bash
DOVEXSCRAPE_RATE_LIMIT=2 dovexscrape search "python lang:en" --limit=100
```

The default is `5` requests per second; use `off` (or `0`) to disable it. The same value can be
set per client with `API(rate_limit="2/s")`. The ceiling covers GraphQL calls, including retries;
the login flow and the `x-client-transaction-id` page fetches are not limited.

## Features

- Search and GraphQL X/Twitter API methods
- Async/await API for running multiple scrapers concurrently
- Login flow with optional email verification code retrieval
- Cookie-based account setup
- Saved account sessions and per-account proxies
- Raw Twitter API responses and parsed SNScrape-compatible models
- Automatic account switching across rate-limited operations

## Sponsor

<p align="center">
  <a href="https://www.rapidproxy.io/?ref=dovexscrape">
    <img src=".github/rapidproxy.jpg" alt="RapidProxy logo" width="460">
  </a>
</p>

<p align="center">
  <a href="https://www.rapidproxy.io/?ref=dovexscrape"><strong>RapidProxy</strong></a> is a residential proxy platform with 90M+ real IPs across 200+ countries. It supports rotation, geo-targeting, and high concurrency to improve scraping success and reduce bans. Start your free trial today!
</p>

<p align="center">Discount Code: <code>RAPID10</code> to get 10% off.</p>

## Start With Cookies

dovexscrape requires authorized X/Twitter accounts. The most stable setup is to add an account from browser cookies containing `auth_token` and `ct0`. The recommended way to export them from your current browser profile is [unjar](https://github.com/vladkens/unjar):

```bash
unjar x.com -f header | dovexscrape add_cookie my_account
dovexscrape accounts
dovexscrape search "from:xdevelopers lang:en" --limit=20
```

`my_account` is a local identifier; dovexscrape does not verify that it matches the X username stored in the cookies. Run the same command again to replace its saved session while preserving credentials, statistics, locks, and proxy settings.

Alternatively, let the CLI prompt securely for cookies copied from x.com -> DevTools (F12) -> Application -> Cookies:

```bash
dovexscrape add_cookie my_account
```

Cookie accounts that include `auth_token` and `ct0` are activated immediately; no `login_accounts` step is needed.

Ready-to-use cookie accounts are available from [this provider](https://kutt.to/ueeM5f). Proxy users can bring their own proxies or use [this provider](https://kutt.to/eb3rXk). These are referral links.

X/Twitter's Terms of Service discourage using multiple accounts. Use this project responsibly and at your own discretion.

## Python API

```python
import asyncio
from dovexscrape import API, gather


async def main():
    api = API()  # or API("mysql://user:pass@host:3306/db")

    # Add once; the session is stored in the account database.
    await api.pool.add_account_cookies("my_account", "auth_token=xxx; ct0=yyy")

    user = await api.user_by_login("xdevelopers")
    print(user.id, user.username, user.followersCount)

    tweets = await gather(api.search("from:xdevelopers lang:en", limit=20))
    for tweet in tweets:
        print(tweet.id, tweet.user.username, tweet.rawContent)


if __name__ == "__main__":
    asyncio.run(main())
```

`gather()` is a convenience helper. You can stream results directly:

```python
async for tweet in api.search("open source lang:en", limit=100):
    print(tweet.id, tweet.rawContent)
```

Configure what happens when no account is immediately available:

```python
api = API(raise_when_no_account=True, wait_timeout=30, wait_interval=1)
```

`wait_timeout` limits how long to wait for a locked account, `wait_interval` controls how often the pool checks again, and `raise_when_no_account` raises `NoAccountError` instead of ending the operation. By default, dovexscrape waits indefinitely while active accounts are locked.

Search defaults to the Latest tab. Pass `kv={"product": "Top"}` or `kv={"product": "Media"}` to use another search product:

```python
tweets = await gather(api.search("python", limit=20, kv={"product": "Top"}))
```

Every parsed method has a `_raw` version for the original response wrapper:

```python
async for rep in api.search_raw("from:xdevelopers", limit=20):
    print(rep.status_code, rep.json())
```

When breaking out of an async generator early, close it with `contextlib.aclosing` so the account lock is released promptly:

```python
from contextlib import aclosing

async with aclosing(api.search("elon musk")) as gen:
    async for tweet in gen:
        if tweet.id < 200:
            break
```

## API Surface

Search:

```python
await gather(api.search("elon musk", limit=20))  # list[Tweet]
await gather(api.search("elon musk", limit=20, kv={"product": "Top"}))  # Top tab
await gather(api.search_user("openai", limit=20))  # list[User]
await gather(api.search_trend("python", limit=20))  # list[Trend]
```

Tweets:

```python
tweet_id = 20

await api.tweet_details(tweet_id)  # Tweet
await gather(api.tweet_replies(tweet_id, limit=20))  # list[Tweet]
await gather(api.tweet_thread(tweet_id, limit=20))  # list[Tweet]
await gather(api.retweeters(tweet_id, limit=20))  # list[User]
await gather(api.bookmarks(limit=20))  # list[Tweet]
```

Users and timelines:

```python
user_login = "xdevelopers"
user_id = 2244994945

await api.user_by_id(user_id)  # User
await api.user_by_login(user_login)  # User
await api.user_about(user_login)  # AccountAbout
await gather(api.following(user_id, limit=20))  # list[User]
await gather(api.followers(user_id, limit=20))  # list[User]
await gather(api.verified_followers(user_id, limit=20))  # list[User]
await gather(api.subscriptions(user_id, limit=20))  # list[User]
await gather(api.user_tweets(user_id, limit=20))  # list[Tweet]
await gather(api.user_tweets_and_replies(user_id, limit=20))  # list[Tweet]
await gather(api.user_media(user_id, limit=20))  # list[Tweet]
```

Lists:

```python
list_id = 123456789

await gather(api.list_timeline(list_id, limit=20))  # list[Tweet]
await gather(api.list_members(list_id, limit=20))  # list[User]
```

Communities:

```python
community_id = 1501272736215322629

await api.community_info(community_id)  # Community
await gather(api.community_members(community_id, limit=20))  # list[User]
await gather(api.community_moderators(community_id, limit=20))  # list[User]
await gather(api.community_tweets(community_id, limit=20))  # list[Tweet]
```

Trends:

```python
await gather(api.trends("news"))  # list[Trend]
await gather(api.trends("sport"))  # list[Trend]
await gather(api.trends("entertainment"))  # list[Trend]
await gather(api.trends("VGltZWxpbmU6DAC2CwABAAAACHRyZW5kaW5nAAA"))  # list[Trend]
```

Parsed `Tweet`, `User`, `Community`, and trend objects can be converted with `.dict()` or `.json()`.

## CLI

```bash
dovexscrape
dovexscrape search --help
```

Commands:

```bash
dovexscrape search "QUERY" --limit=20
dovexscrape tweet_details TWEET_ID
dovexscrape tweet_replies TWEET_ID --limit=20
dovexscrape tweet_thread TWEET_ID --limit=20
dovexscrape retweeters TWEET_ID --limit=20
dovexscrape user_by_id USER_ID
dovexscrape user_by_login USERNAME
dovexscrape user_about USERNAME
dovexscrape user_media USER_ID --limit=20
dovexscrape following USER_ID --limit=20
dovexscrape followers USER_ID --limit=20
dovexscrape verified_followers USER_ID --limit=20
dovexscrape subscriptions USER_ID --limit=20
dovexscrape user_tweets USER_ID --limit=20
dovexscrape user_tweets_and_replies USER_ID --limit=20
dovexscrape list_timeline LIST_ID --limit=20
dovexscrape list_members LIST_ID --limit=20
dovexscrape community_info COMMUNITY_ID
dovexscrape community_members COMMUNITY_ID --limit=20
dovexscrape community_moderators COMMUNITY_ID --limit=20
dovexscrape community_tweets COMMUNITY_ID --limit=20
dovexscrape trends sport
```

CLI output is JSON Lines: one document per line.

```bash
dovexscrape search "elon musk lang:es" --limit=20 > tweets.jsonl
dovexscrape search "elon musk lang:es" --limit=20 --raw
```

Use a separate account database when you need isolated account pools:

```bash
dovexscrape --db "mysql://user:pass@host:3306/research" search "python lang:en" --limit=100
```

## Accounts

Add username/password accounts from a file:

```bash
dovexscrape add_accounts ./accounts.txt username:password:email:email_password
dovexscrape login_accounts
```

`dovexscrape login_accounts` starts the login flow for each inactive account. If X asks for email verification and `email_password` is available, dovexscrape tries to read the code through IMAP and saves the resulting cookies for later use.

`line_format` describes how each line is split. Supported tokens:

- `username` - required
- `password` - required
- `email` - required
- `email_password` - used to fetch email verification codes through IMAP
- `cookies` - cookie string, JSON, base64, or another format accepted by the parser
- `_` - skip column

Example account file:

```text
username:password:email:email password:user_agent:cookies
```

Command:

```bash
dovexscrape add_accounts ./accounts.txt username:password:email:email_password:_:cookies
```

If IMAP is unavailable, enter verification codes manually:

```bash
dovexscrape login_accounts --manual
dovexscrape relogin user1 user2 --manual
dovexscrape relogin_failed --manual
```

Inspect and maintain the pool:

```bash
dovexscrape accounts
dovexscrape stats
dovexscrape relogin user1 user2
dovexscrape relogin_failed
dovexscrape reset_locks
dovexscrape delete_inactive
dovexscrape del_accounts user1 user2
```

`dovexscrape accounts` prints the current account state:

```text
username  logged_in  active  last_used            total_req  error_msg
user1     True       True    2023-05-20 03:20:40  100        None
user2     True       True    2023-05-20 03:25:45  120        None
user3     False      False   None                 120        Login error
```

## Limits

`limit` is the target number of parsed objects, not a page size. X/Twitter controls page size per endpoint, so a call can return fewer or more objects than requested.

Rate limits are tracked per account and per endpoint. When an account is limited for one operation, dovexscrape locks it for that operation until the reset time and tries another active account.

`user_tweets` and `user_tweets_and_replies` are limited by X/Twitter to about 3200 tweets.

## Proxy

Use a proxy per account, per API instance, or for CLI commands:

```python
await api.pool.add_account(
    "user1",
    "pass1",
    "user1@example.com",
    "email_pass1",
    proxy="http://login:pass@example.com:8080",
)

api = API(proxy="http://login:pass@example.com:8080")
doc = await api.user_by_login("xdevelopers")
```

```bash
DOVEXSCRAPE_PROXY=socks5://user:pass@127.0.0.1:1080 dovexscrape user_by_login xdevelopers
```

Proxy priority:

1. `api.proxy`
2. `DOVEXSCRAPE_PROXY`
3. account proxy

Do not set `api.proxy` or `DOVEXSCRAPE_PROXY` when you want per-account proxies to be used.

## Environment

- `DOVEXSCRAPE_DB` - default MySQL url for accounts, eg. `mysql://user:pass@host:3306/db`
- `DOVEXSCRAPE_RATE_LIMIT` - process-wide request ceiling, default `5` (req/s); accepts `5`, `5/s`, or `off`/`0` to disable
- `DOVEXSCRAPE_MAX_EMPTY_PAGES` - empty pages tolerated before pagination stops, default `3`; `0` or `1` stop on the first empty page
- `DOVEXSCRAPE_PROXY` - global proxy for all accounts
- `DOVEXSCRAPE_WAIT_EMAIL_CODE` - email verification timeout in seconds, default `30`
- `DOVEXSCRAPE_RAISE_WHEN_NO_ACCOUNT` - raise `NoAccountError` instead of waiting; accepts `false`, `0`, `true`, `1`
- `DOVEXSCRAPE_HTTP_BACKEND` - `httpx` or `curl`
- `DOVEXSCRAPE_LOG_LEVEL` - logger level, default `INFO`
- `DOVEXSCRAPE_TELEMETRY=0` - disable anonymous telemetry

## Telemetry

dovexscrape collects anonymous, aggregated telemetry about used GraphQL operation names and the selected HTTP backend. It does not collect usernames, cookies, proxies, queries, request URLs, or response bodies. Disable it with `DOVEXSCRAPE_TELEMETRY=0`.

## See Also

- [twitter-advanced-search](https://github.com/igorbrigadir/twitter-advanced-search) - guide on search filters
- [TweeterPy](https://github.com/iSarabjitDhiman/TweeterPy) - another X client
- [twitter-api-client](https://github.com/trevorhobenshield/twitter-api-client) - implementation of Twitter's v1, v2, and GraphQL APIs
