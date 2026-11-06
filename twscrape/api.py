import asyncio
from collections import deque
from contextlib import aclosing
from typing import Literal
from urllib.parse import urlparse

from .account import Account, has_required_cookies
from .accounts_pool import AccountsPool
from .http import HttpClient, Response
from .logger import logger, set_log_level
from .models import (
    AccountAbout,
    Community,
    Tweet,
    User,
    parse_about,
    parse_community,
    parse_trends,
    parse_tweet,
    parse_tweets,
    parse_user,
    parse_users,
)
from .queue_client import QueueClient, XClIdGenStore, has_data
from .utils import (
    configure_rate_limit,
    encode_params,
    find_obj,
    get_by_path,
    get_env_int,
    rate_limiter,
)

# GraphQL operation IDs used by this module.
# If you add a new endpoint, add it here manually.
# Update this block with: `uv run scripts/update-gql-ops.py`
# This script rewrites the whole block automatically.

# Consecutive pages carrying a cursor but nothing to parse are tolerated this many
# times before pagination gives up; 0 and 1 both mean "stop on the first empty page".
DEFAULT_MAX_EMPTY_PAGES = 3
ENV_MAX_EMPTY_PAGES = "TWS_MAX_EMPTY_PAGES"

SEARCH_CHECK_TIMEOUT = 30.0
SEARCH_CHECK_QUERY = "hello"

# GQL_OPS_CODEGEN
OP_AboutAccountQuery = "TzOG2twZEfhr9KmClvVVqA/AboutAccountQuery"
OP_BlueVerifiedFollowers = "u3PkPbg--arppBcwNbF1ig/BlueVerifiedFollowers"
OP_Bookmarks = "iblrFnKr6PZUR-dWpfXG6g/Bookmarks"
OP_CommunityQuery = "-ElI1vg3dYbttVMhBhGdLw/CommunityQuery"
OP_CommunityTweetsTimeline = "EwftYyqQemkckQ0wzGM6uw/CommunityTweetsTimeline"
OP_Followers = "JNyQdTISpzCkj_1fqxDvFg/Followers"
OP_Following = "qGZZDF3mp91q7X22s3HxpA/Following"
OP_GenericTimelineById = "ee4dBLWL8a8qg6n19m1htQ/GenericTimelineById"
OP_ListLatestTweetsTimeline = "1LE3u14FJjPZUHKFGzos2g/ListLatestTweetsTimeline"
OP_ListMembers = "8rYmkvWQe9jRRZdy_-vkGA/ListMembers"
OP_Retweeters = "ROjiuYueotTnWoI8m2YaiQ/Retweeters"
OP_SearchTimeline = "hyPfJYJ_XAtDYoslQc-Rgg/SearchTimeline"
OP_TweetDetail = "XMOz5h24KAZ86qKffKTLdQ/TweetDetail"
OP_UserByRestId = "xvmVfRLmnr1alc5f2dib0Q/UserByRestId"
OP_UserByScreenName = "Gb-d6r0vxPOADdG62OEBpQ/UserByScreenName"
OP_UserCreatorSubscriptions = "Qxe_gd-ZvdofnzSL8Ngzpw/UserCreatorSubscriptions"
OP_UserMedia = "VyudDWQnr9vJNw7GasFz2g/UserMedia"
OP_UserTweets = "SXVCYB8XHSS25nzIljNtZA/UserTweets"
OP_UserTweetsAndReplies = "qUpkZU6eN8MbtQb7rC_pYg/UserTweetsAndReplies"
OP_membersSliceTimeline_Query = "woAp_YdzAdqnWDrqLTNpAw/membersSliceTimeline_Query"
OP_moderatorsSliceTimeline_Query = "0oYT9GRiWUhrz5xoqFE9uw/moderatorsSliceTimeline_Query"
# GQL_OPS_CODEGEN

GQL_URL = "https://x.com/i/api/graphql"
ARTICLE_FIELD_TOGGLES = {
    "withArticleRichContentState": True,
    "withArticlePlainText": False,
    "withArticleSummaryText": True,
    "withArticleVoiceOver": True,
    "withGrokAnalyze": False,
    "withDisallowedReplyControls": False,
}

GQL_FEATURES = {  # search values here (view source) https://x.com/
    "articles_preview_enabled": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "communities_web_enable_tweet_community_results_fetch": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": False,
    "responsive_web_media_download_video_enabled": False,
    "responsive_web_profile_redirect_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_awards_web_tipping_enabled": False,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "tweet_with_visibility_results_prefer_gql_media_interstitial_enabled": False,
    "tweetypie_unmention_optimization_enabled": True,
    "verified_phone_label_enabled": False,
    "view_counts_everywhere_api_enabled": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "premium_content_api_read_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": False,
    "responsive_web_grok_share_attachment_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": False,
    "responsive_web_grok_image_annotation_enabled": False,
    "responsive_web_grok_analysis_button_from_backend": False,
    "responsive_web_jetfuel_frame": False,
    "rweb_video_screen_enabled": True,
    "responsive_web_grok_show_grok_translated_post": True,
}

KV = dict | None
TrendId = Literal["trending", "news", "sport", "entertainment"] | str
SearchCheck = tuple[Literal["positive", "negative", "inconclusive"], str]


def _search_error_messages(res: dict) -> list[str]:
    errors = res.get("errors")
    if not errors:
        return []
    if isinstance(errors, dict) or not isinstance(errors, list):
        errors = [errors]
    return [str(x.get("message", "")) if isinstance(x, dict) else str(x) for x in errors]


def _has_tweet_content(rep: Response) -> bool:
    # Parsed timeline structure, not entryId/cursor markers: only real tweet content counts.
    try:
        return next(parse_tweets(rep), None) is not None
    except Exception:
        return False


async def _parsed_pages(raw_pages, parser, limit: int, accept=None):
    # Keep parser context for each page, then deduplicate accepted items across pages.
    # Repeats come from adjacent pages and from the pinned tweet on the first page, so
    # only those ids are kept: a set of every id grows without bound on long crawls.
    first_ids: set[int | str | None] = set()
    recent_ids: deque[set[int | str | None]] = deque(maxlen=10)
    count, duplicate_pages = 0, 0
    async with aclosing(raw_pages) as gen:
        async for rep in gen:
            seen_ids = first_ids.union(*recent_ids)
            page_ids = set()
            for item in parser(rep, seen_ids=seen_ids, accept=accept):
                page_ids.add(item.id)
                count += 1
                yield item
                if limit > 0 and count >= limit:
                    return

            if first_ids:
                recent_ids.append(page_ids)
            else:
                first_ids = page_ids

            duplicate_pages = 0 if page_ids else duplicate_pages + 1
            if duplicate_pages >= 3:
                logger.warning("Stopping after 3 pages without new parsed items")
                return


class API:
    # Note: kv is variables, ft is features from original GQL request
    pool: AccountsPool

    def __init__(
        self,
        pool: AccountsPool | str | None = None,
        debug=False,
        proxy: str | None = None,
        raise_when_no_account=False,
        wait_timeout: float | None = None,
        wait_interval: float = 5.0,
        rate_limit: float | str | None = None,
        max_empty_pages: int | None = None,
    ):
        # process-wide: applies to every API/QueueClient in this process, eg. 5 or "5/s";
        # None keeps whatever the environment or the default installed, 0/"off" disables
        configure_rate_limit(rate_limit)
        rate_limiter.log_config()  # startup log; lazy env read, no import-time side effect

        # per-instance: 0/1 stop on the first empty page, N tolerates N consecutive ones
        if max_empty_pages is None:
            max_empty_pages = get_env_int(
                ENV_MAX_EMPTY_PAGES,
                DEFAULT_MAX_EMPTY_PAGES,
                fallback_key="DOVEXSCRAPE_MAX_EMPTY_PAGES",
            )
        self.max_empty_pages = max(0, max_empty_pages)

        if isinstance(pool, AccountsPool):
            self.pool = pool
        elif isinstance(pool, str):
            self.pool = AccountsPool(
                db_file=pool,
                raise_when_no_account=raise_when_no_account,
                wait_timeout=wait_timeout,
                wait_interval=wait_interval,
            )
        else:
            self.pool = AccountsPool(
                raise_when_no_account=raise_when_no_account,
                wait_timeout=wait_timeout,
                wait_interval=wait_interval,
            )

        self.proxy = proxy
        self.debug = debug
        if self.debug:
            set_log_level("DEBUG")

    # general helpers

    def _is_end(self, rep: Response, q: str, res: list, cur: str | None, cnt: int, lim: int):
        new_count = len(res)
        new_total = cnt + new_count

        is_res = new_count > 0
        is_cur = cur is not None
        is_lim = lim > 0 and new_total >= lim

        return rep if is_res else None, new_total, is_cur and not is_lim

    def _is_stalled(self, q: str, res: list, cur: str | None, seen: set[tuple[str, ...]]):
        keys: list[tuple[str, ...]] = [("cursor", cur)] if cur is not None else []
        if q == "SearchTimeline":
            entry_ids = tuple(
                str(x.get("entryId"))
                for x in res
                if isinstance(x, dict) and x.get("entryId") is not None
            )
            if entry_ids:
                keys.append(("entries", *entry_ids))

        if any(x in seen for x in keys):
            return True

        # Not sure whether A → B → A should count as a stall, so only compare the last page.
        seen.clear()  # Comment this out to detect repeats across all pages.
        seen.update(keys)
        return False

    def _get_cursor(self, obj: dict, cursor_type="Bottom") -> str | None:
        # standard timeline cursor: {cursorType: "Bottom", value: "..."}
        # fallback: community endpoints use slice_info.next_cursor (plain string)
        cur = find_obj(obj, lambda x: x.get("cursorType") == cursor_type)
        if cur is None and cursor_type == "ShowMoreThreads":
            # TweetDetail currently ships only the Bottom cursor, even when the
            # caller asks for ShowMoreThreads: without this fallback the replies
            # timeline stops after the first page (large threads are cut short).
            cur = find_obj(obj, lambda x: x.get("cursorType") == "Bottom")
        value = cur.get("value") if cur else get_by_path(obj, "next_cursor")
        return value if isinstance(value, str) else None

    @staticmethod
    def _is_empty_module(entry) -> bool:
        # module entries whose items are empty carry nothing to parse
        if not isinstance(entry, dict):
            return False
        content = entry.get("content") or {}
        if content.get("__typename") != "TimelineTimelineModule":
            return False
        return not (content.get("items") or [])

    def _gql_entries(self, obj: dict) -> list:
        # standard timelines put items in "entries"; community endpoints use "items_results"
        els = get_by_path(obj, "entries") or get_by_path(obj, "items_results") or []
        if els and "entryId" in (els[0] or {}):
            # filter out pagination cursors and non-content module entries
            els = [
                x
                for x in els
                if not x["entryId"].startswith(
                    ("cursor-", "messageprompt-", "module-", "who-to-follow-")
                )
            ]
            # collapsed conversation threads (eg. conversationthread-* in the
            # LowQuality section) come with empty items and only allTweetIds as
            # metadata. They parse to nothing but still count as page content,
            # so _is_end keeps the empty-page counter at zero and pagination
            # walks page after page of nothing. Drop them to keep that guard alive.
            els = [x for x in els if not self._is_empty_module(x)]
        return els

    # gql helpers

    async def _gql_items(
        self,
        op: str,
        kv: dict,
        ft: dict | None = None,
        limit=-1,
        cursor_type="Bottom",
        toggles: dict | None = None,
    ):
        queue, cur, cnt, active = op.split("/")[-1], None, 0, True
        kv, ft = {**kv}, {**GQL_FEATURES, **(ft or {})}
        empty_pages = 0
        seen: set[tuple[str, ...]] = set()

        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            while active:
                params = {"variables": kv, "features": ft}
                if cur is not None:
                    params["variables"]["cursor"] = cur
                if toggles:
                    params["fieldToggles"] = toggles

                rep = await client.get(f"{GQL_URL}/{op}", params=encode_params(params))
                if rep is None:
                    return

                obj = rep.json()
                els = self._gql_entries(obj)
                cur = self._get_cursor(obj, cursor_type)

                if self._is_stalled(queue, els, cur, seen):
                    logger.warning(f"{queue} pagination stalled, stopping")
                    return

                rep, cnt, active = self._is_end(rep, queue, els, cur, cnt, limit)
                if rep is None:
                    # cursor exists → data may follow after empty/filtered pages (e.g. promo)
                    if cur is not None:
                        empty_pages += 1
                        if empty_pages >= self.max_empty_pages:
                            logger.debug(f"{queue} – {empty_pages} empty pages in a row, stopping")
                            return
                        continue
                    return

                empty_pages = 0
                yield rep

    async def _gql_item(
        self, op: str, kv: dict, ft: dict | None = None, toggles: dict | None = None
    ):
        ft = ft or {}
        queue = op.split("/")[-1]
        async with QueueClient(self.pool, queue, self.debug, proxy=self.proxy) as client:
            params = {"variables": {**kv}, "features": {**GQL_FEATURES, **ft}}
            if toggles:
                params["fieldToggles"] = toggles
            return await client.get(f"{GQL_URL}/{op}", params=encode_params(params))

    # search

    async def search_raw(self, q: str, limit=-1, kv: KV = None):
        op = OP_SearchTimeline
        kv = {
            "rawQuery": q,
            "count": 20,
            "product": "Latest",
            "querySource": "typed_query",
            **(kv or {}),
        }
        toggles = {"withArticleRichContentState": False}
        async with aclosing(self._gql_items(op, kv, limit=limit, toggles=toggles)) as gen:
            async for x in gen:
                yield x

    async def search(self, q: str, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.search_raw(q, limit=-1, kv=kv), parse_tweets, limit)
        ) as gen:
            async for x in gen:
                yield x

    async def search_user(self, q: str, limit=-1, kv: KV = None):
        kv = {"product": "People", **(kv or {})}
        async with aclosing(
            _parsed_pages(self.search_raw(q, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for x in gen:
                yield x

    async def _search_check_once(
        self, acc: Account, clt: HttpClient, proxy: str | None, fresh: bool
    ) -> SearchCheck:
        try:
            gen = await XClIdGenStore.get(
                acc.username, proxy=proxy, cookies=acc.cookies, fresh=fresh
            )
            url = f"{GQL_URL}/{OP_SearchTimeline}"
            params = {
                "variables": {
                    "rawQuery": SEARCH_CHECK_QUERY,
                    "count": 20,
                    "product": "Latest",
                    "querySource": "typed_query",
                },
                "features": GQL_FEATURES,
                "fieldToggles": {"withArticleRichContentState": False},
            }
            headers = {"x-client-transaction-id": gen.calc("GET", urlparse(url).path or "/")}
            await rate_limiter.acquire()
            rep = await clt.request("GET", url, params=encode_params(params), headers=headers)
        except Exception as e:
            return "inconclusive", type(e).__name__

        return self._classify_search(rep)

    @staticmethod
    def _classify_search(rep: Response) -> SearchCheck:
        if rep.status_code == 404 and not rep.text.strip():
            return "negative", "search_404"
        if rep.status_code != 200:
            return "inconclusive", f"http_{rep.status_code}"

        try:
            res = rep.json()
        except Exception as e:
            return "inconclusive", type(e).__name__
        if not isinstance(res, dict):
            return "inconclusive", "invalid_response"

        errors = _search_error_messages(res)
        if errors:
            if not has_data(rep, res) and any("internal server error" in x.lower() for x in errors):
                # shortcut: route failures may be endpoint-wide, recheck after service recovery.
                return "negative", "search_internal_error"
            return "inconclusive", "http_200_errors"
        if _has_tweet_content(rep):
            return "positive", "ok"
        return "inconclusive", "empty_search"

    async def _search_check_probe(self, acc: Account) -> SearchCheck:
        clt = acc.make_client(proxy=self.proxy)
        try:
            proxy = acc.resolve_proxy(self.proxy)
            kind, reason = await self._search_check_once(acc, clt, proxy, fresh=False)
            if kind != "negative":
                return kind, reason

            kind, second_reason = await self._search_check_once(acc, clt, proxy, fresh=True)
            if kind == "negative" and second_reason == reason:
                return "negative", reason
            if kind == "negative":
                return "inconclusive", f"unconfirmed_{second_reason}"
            return kind, second_reason
        finally:
            await clt.aclose()

    async def check_search(self, username: str) -> dict[str, str | bool | None]:
        """Probe SearchTimeline for one account and persist the result.

        Reads the named account directly, including inactive and already marked
        accounts, without taking a pool slot or touching locks/stats/failure
        counters. A decision is stored only after two identical known failures
        (empty HTTP 404 or HTTP 200 "Internal server error" with no data) or a
        valid page with tweets. Repeated failures describe availability now and
        can still reflect an endpoint-wide issue, so recheck later; False is not
        a permanent suspension. Every other answer leaves can_search unchanged.
        """
        acc = await self.pool.get_account(username)
        if acc is None:
            return {
                "username": username,
                "can_search": None,
                "updated": False,
                "reason": "account_not_found",
            }
        if not has_required_cookies(acc.cookies):
            return {
                "username": username,
                "can_search": None,
                "updated": False,
                "reason": "missing_session",
            }

        try:
            kind, reason = await asyncio.wait_for(
                self._search_check_probe(acc), timeout=SEARCH_CHECK_TIMEOUT
            )
        except Exception as e:
            return {
                "username": username,
                "can_search": None,
                "updated": False,
                "reason": type(e).__name__,
            }

        if kind == "inconclusive":
            return {
                "username": username,
                "can_search": None,
                "updated": False,
                "reason": reason,
            }

        can_search = kind == "positive"
        await self.pool.set_can_search(username, can_search)
        return {
            "username": username,
            "can_search": can_search,
            "updated": True,
            "reason": reason,
        }

    # user_by_id

    async def user_by_id_raw(self, uid: int, kv: KV = None):
        op = OP_UserByRestId
        kv = {"userId": str(uid), "withSafetyModeUserFields": True, **(kv or {})}
        ft = {
            "highlights_tweets_tab_ui_enabled": True,
            "hidden_profile_likes_enabled": True,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "hidden_profile_subscriptions_enabled": True,
            "subscriptions_verification_info_verified_since_enabled": True,
            "subscriptions_verification_info_is_identity_verified_enabled": False,
            "responsive_web_twitter_article_notes_tab_enabled": False,
            "subscriptions_feature_can_gift_premium": False,
            "profile_label_improvements_pcf_label_in_post_enabled": False,
        }
        return await self._gql_item(op, kv, ft)

    async def user_by_id(self, uid: int, kv: KV = None) -> User | None:
        rep = await self.user_by_id_raw(uid, kv=kv)
        return parse_user(rep) if rep else None

    # user_by_login

    async def user_by_login_raw(self, login: str, kv: KV = None):
        op = OP_UserByScreenName
        kv = {"screen_name": login, "withSafetyModeUserFields": True, **(kv or {})}
        ft = {
            "highlights_tweets_tab_ui_enabled": True,
            "hidden_profile_likes_enabled": True,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "hidden_profile_subscriptions_enabled": True,
            "subscriptions_verification_info_verified_since_enabled": True,
            "subscriptions_verification_info_is_identity_verified_enabled": False,
            "responsive_web_twitter_article_notes_tab_enabled": False,
            "subscriptions_feature_can_gift_premium": False,
            "profile_label_improvements_pcf_label_in_post_enabled": False,
        }
        return await self._gql_item(op, kv, ft)

    async def user_by_login(self, login: str, kv: KV = None) -> User | None:
        rep = await self.user_by_login_raw(login, kv=kv)
        return parse_user(rep) if rep else None

    async def user_about_raw(self, username: str, kv: KV = None):
        op = OP_AboutAccountQuery
        kv = {"screenName": username, **(kv or {})}
        ft = {"responsive_web_graphql_timeline_navigation_enabled": True}
        return await self._gql_item(op, kv, ft)

    async def user_about(self, username: str, kv: KV = None) -> AccountAbout | None:
        rep = await self.user_about_raw(username, kv=kv)
        return parse_about(rep) if rep else None

    # tweet_details

    async def tweet_details_raw(self, twid: int, kv: KV = None):
        op = OP_TweetDetail
        kv = {
            "focalTweetId": str(twid),
            "with_rux_injections": True,
            "includePromotedContent": True,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        return await self._gql_item(op, kv, toggles=ARTICLE_FIELD_TOGGLES)

    async def tweet_details(self, twid: int, kv: KV = None) -> Tweet | None:
        rep = await self.tweet_details_raw(twid, kv=kv)
        return parse_tweet(rep, twid) if rep else None

    # tweet_replies
    # note: uses same op as tweet_details, see: https://github.com/vladkens/twscrape/issues/104

    async def tweet_replies_raw(self, twid: int, limit=-1, kv: KV = None):
        op = OP_TweetDetail
        kv = {
            "focalTweetId": str(twid),
            "referrer": "tweet",
            "with_rux_injections": True,
            "includePromotedContent": True,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        async with aclosing(
            self._gql_items(op, kv, limit=limit, cursor_type="ShowMoreThreads")
        ) as gen:
            async for x in gen:
                yield x

    async def tweet_replies(self, twid: int, limit=-1, kv: KV = None):
        raw = self.tweet_replies_raw(twid, limit=-1, kv=kv)
        async with aclosing(
            _parsed_pages(raw, parse_tweets, limit, lambda x: x.inReplyToTweetId == twid)
        ) as gen:
            async for x in gen:
                yield x

    # tweet_thread
    # Same TweetDetail family as tweet_replies, but configured to return the
    # full thread timeline and then filtered by conversationId instead of
    # only direct replies via inReplyToTweetId.

    async def tweet_thread_raw(self, twid: int, limit=-1, kv: KV = None):
        op = OP_TweetDetail
        kv = {
            "focalTweetId": str(twid),
            "referrer": "profile",
            "with_rux_injections": False,
            "includePromotedContent": True,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        ft = {
            "rweb_video_screen_enabled": False,
            "profile_label_improvements_pcf_label_in_post_enabled": True,
            "rweb_tipjar_consumption_enabled": True,
            "verified_phone_label_enabled": False,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "responsive_web_graphql_timeline_navigation_enabled": True,
            "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
            "premium_content_api_read_enabled": False,
            "communities_web_enable_tweet_community_results_fetch": True,
            "c9s_tweet_anatomy_moderator_badge_enabled": True,
            "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
            "responsive_web_grok_analyze_post_followups_enabled": True,
            "responsive_web_jetfuel_frame": False,
            "responsive_web_grok_share_attachment_enabled": True,
            "articles_preview_enabled": True,
            "responsive_web_edit_tweet_api_enabled": True,
            "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
            "view_counts_everywhere_api_enabled": True,
            "longform_notetweets_consumption_enabled": True,
            "responsive_web_twitter_article_tweet_consumption_enabled": True,
            "tweet_awards_web_tipping_enabled": False,
            "responsive_web_grok_show_grok_translated_post": False,
            "responsive_web_grok_analysis_button_from_backend": True,
            "creator_subscriptions_quote_tweet_preview_enabled": False,
            "freedom_of_speech_not_reach_fetch_enabled": True,
            "standardized_nudges_misinfo": True,
            "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
            "longform_notetweets_rich_text_read_enabled": True,
            "longform_notetweets_inline_media_enabled": True,
            "responsive_web_grok_image_annotation_enabled": True,
            "responsive_web_enhance_cards_enabled": False,
        }
        async with aclosing(self._gql_items(op, kv, ft=ft, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def tweet_thread(self, twid: int, limit=-1, kv: KV = None):
        raw = self.tweet_thread_raw(twid, limit=-1, kv=kv)
        async with aclosing(
            _parsed_pages(raw, parse_tweets, limit, lambda x: x.conversationId == twid)
        ) as gen:
            async for x in gen:
                yield x

    # followers

    async def followers_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_Followers
        kv = {"userId": str(uid), "count": 20, "includePromotedContent": False, **(kv or {})}
        ft = {"responsive_web_twitter_article_notes_tab_enabled": False}
        async with aclosing(self._gql_items(op, kv, limit=limit, ft=ft)) as gen:
            async for x in gen:
                yield x

    async def followers(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.followers_raw(uid, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for x in gen:
                yield x

    # verified_followers

    async def verified_followers_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_BlueVerifiedFollowers
        kv = {"userId": str(uid), "count": 20, "includePromotedContent": False, **(kv or {})}
        ft = {
            "responsive_web_twitter_article_notes_tab_enabled": True,
        }
        async with aclosing(self._gql_items(op, kv, limit=limit, ft=ft)) as gen:
            async for x in gen:
                yield x

    async def verified_followers(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.verified_followers_raw(uid, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for x in gen:
                yield x

    # following

    async def following_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_Following
        kv = {"userId": str(uid), "count": 20, "includePromotedContent": False, **(kv or {})}
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def following(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.following_raw(uid, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for x in gen:
                yield x

    # subscriptions

    async def subscriptions_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserCreatorSubscriptions
        kv = {"userId": str(uid), "count": 20, "includePromotedContent": False, **(kv or {})}
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def subscriptions(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.subscriptions_raw(uid, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for x in gen:
                yield x

    # retweeters

    async def retweeters_raw(self, twid: int, limit=-1, kv: KV = None):
        op = OP_Retweeters
        kv = {"tweetId": str(twid), "count": 20, "includePromotedContent": True, **(kv or {})}
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def retweeters(self, twid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.retweeters_raw(twid, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for x in gen:
                yield x

    # user_tweets

    async def user_tweets_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserTweets
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def user_tweets(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.user_tweets_raw(uid, limit=-1, kv=kv), parse_tweets, limit)
        ) as gen:
            async for x in gen:
                yield x

    # user_tweets_and_replies

    async def user_tweets_and_replies_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserTweetsAndReplies
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": True,
            "withCommunity": True,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def user_tweets_and_replies(self, uid: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(
                self.user_tweets_and_replies_raw(uid, limit=-1, kv=kv), parse_tweets, limit
            )
        ) as gen:
            async for x in gen:
                yield x

    # user_media

    async def user_media_raw(self, uid: int, limit=-1, kv: KV = None):
        op = OP_UserMedia
        kv = {
            "userId": str(uid),
            "count": 40,
            "includePromotedContent": False,
            "withClientEventToken": False,
            "withBirdwatchNotes": False,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        toggles = {"withArticlePlainText": False}
        async with aclosing(self._gql_items(op, kv, limit=limit, toggles=toggles)) as gen:
            async for x in gen:
                yield x

    async def user_media(self, uid: int, limit=-1, kv: KV = None):
        # Some UserMedia entries have no attached media.
        raw = self.user_media_raw(uid, limit=-1, kv=kv)

        def accept(x: Tweet) -> bool:
            return bool(x.media and (x.media.photos or x.media.videos or x.media.animated))

        async with aclosing(_parsed_pages(raw, parse_tweets, limit, accept)) as gen:
            async for x in gen:
                yield x

    # list_timeline

    async def list_timeline_raw(self, list_id: int, limit=-1, kv: KV = None):
        op = OP_ListLatestTweetsTimeline
        kv = {"listId": str(list_id), "count": 20, **(kv or {})}
        toggles = {"withArticleRichContentState": False}
        async with aclosing(self._gql_items(op, kv, limit=limit, toggles=toggles)) as gen:
            async for x in gen:
                yield x

    async def list_timeline(self, list_id: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.list_timeline_raw(list_id, limit=-1, kv=kv), parse_tweets, limit)
        ) as gen:
            async for x in gen:
                yield x

    # trends

    async def trends_raw(self, trend_id: TrendId, limit=-1, kv: KV = None):
        map = {
            "trending": "VGltZWxpbmU6DAC2CwABAAAACHRyZW5kaW5nAAA",
            "news": "VGltZWxpbmU6DAC2CwABAAAABG5ld3MAAA",
            "sport": "VGltZWxpbmU6DAC2CwABAAAABnNwb3J0cwAA",
            "entertainment": "VGltZWxpbmU6DAC2CwABAAAADWVudGVydGFpbm1lbnQAAA",
        }
        trend_id = map.get(trend_id, trend_id)

        op = OP_GenericTimelineById
        kv = {
            "timelineId": trend_id,
            "count": 20,
            "withQuickPromoteEligibilityTweetFields": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def trends(self, trend_id: TrendId, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.trends_raw(trend_id, limit=-1, kv=kv), parse_trends, limit)
        ) as gen:
            async for x in gen:
                yield x

    async def search_trend(self, q: str, limit=-1, kv: KV = None):
        kv = {
            "querySource": "trend_click",
            **(kv or {}),
        }
        async with aclosing(
            _parsed_pages(self.search_raw(q, limit=-1, kv=kv), parse_tweets, limit)
        ) as gen:
            async for x in gen:
                yield x

    # Get current user bookmarks

    async def bookmarks_raw(self, limit=-1, kv: KV = None):
        op = OP_Bookmarks
        kv = {
            "count": 20,
            "includePromotedContent": False,
            "withClientEventToken": False,
            "withBirdwatchNotes": False,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        ft = {
            "graphql_timeline_v2_bookmark_timeline": True,
        }
        async with aclosing(self._gql_items(op, kv, ft, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def bookmarks(self, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.bookmarks_raw(limit=-1, kv=kv), parse_tweets, limit)
        ) as gen:
            async for x in gen:
                yield x

    # list members of a List

    async def list_members_raw(self, list_id: int, limit: int = -1, kv: KV = None):
        op = OP_ListMembers
        kv = {"listId": str(list_id), "count": 20, **(kv or {})}
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for page in gen:
                yield page

    async def list_members(self, list_id: int, limit: int = -1, kv: KV = None):
        async with aclosing(
            _parsed_pages(self.list_members_raw(list_id, limit=-1, kv=kv), parse_users, limit)
        ) as gen:
            async for user in gen:
                yield user

    # Community members

    async def community_members_raw(self, community_id: int, limit=-1, kv: KV = None):
        op = OP_membersSliceTimeline_Query
        kv = {
            "communityId": str(community_id),
            "count": 20,
            "includePromotedContent": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def community_members(self, community_id: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(
                self.community_members_raw(community_id, limit=-1, kv=kv), parse_users, limit
            )
        ) as gen:
            async for x in gen:
                yield x

    # Community moderators

    async def community_moderators_raw(self, community_id: int, limit=-1, kv: KV = None):
        op = OP_moderatorsSliceTimeline_Query
        kv = {
            "communityId": str(community_id),
            "count": 20,
            "includePromotedContent": False,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def community_moderators(self, community_id: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(
                self.community_moderators_raw(community_id, limit=-1, kv=kv), parse_users, limit
            )
        ) as gen:
            async for x in gen:
                yield x

    # Community tweets timeline

    async def community_tweets_raw(self, community_id: int, limit=-1, kv: KV = None):
        op = OP_CommunityTweetsTimeline
        kv = {
            "communityId": str(community_id),
            "count": 40,
            "displayLocation": "Community",
            "rankingMode": "Relevance",
            "withCommunity": True,
            "includePromotedContent": False,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True,
            **(kv or {}),
        }
        async with aclosing(self._gql_items(op, kv, limit=limit)) as gen:
            async for x in gen:
                yield x

    async def community_tweets(self, community_id: int, limit=-1, kv: KV = None):
        async with aclosing(
            _parsed_pages(
                self.community_tweets_raw(community_id, limit=-1, kv=kv), parse_tweets, limit
            )
        ) as gen:
            async for x in gen:
                yield x

    async def community_info_raw(self, community_id: int, kv: KV = None):
        op = OP_CommunityQuery
        kv = {"communityId": str(community_id), **(kv or {})}
        ft = {
            "c9s_list_members_action_api_enabled": False,
            "c9s_superc9s_indication_enabled": False,
        }
        return await self._gql_item(op, kv, ft)

    async def community_info(self, community_id: int, kv: KV = None) -> Community | None:
        rep = await self.community_info_raw(community_id, kv=kv)
        return parse_community(rep) if rep else None
