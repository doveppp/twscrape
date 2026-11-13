import asyncio
from unittest.mock import MagicMock

import pytest

import twscrape.xclid as xclid
from twscrape.http import CurlClient, HttpMethod, HttpxClient, NetworkError, Response

from .mock_http import MockClient, _raw


class AssetClient(MockClient):
    """MockClient that serves asset bodies by URL and records fetched URLs."""

    def __init__(self, pages: dict[str, str]):
        super().__init__()
        self.pages = pages
        self.requested: list[str] = []

    async def request(self, method: HttpMethod, url: str, **kwargs) -> Response:
        self.requested.append(url)
        text = self.pages.get(url)
        return Response(_raw(status_code=200 if text is not None else 404, text=text or ""))


class BlockingAssetClient(AssetClient):
    def __init__(self):
        super().__init__({})
        self.started = asyncio.Event()
        self.cancelled = 0

    async def request(self, method: HttpMethod, url: str, **kwargs) -> Response:
        self.requested.append(url)
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        raise AssertionError("fetch should never complete")


class FakeClient:
    def __init__(self):
        self.closed = False
        self.cookies = MagicMock()

    async def aclose(self):
        self.closed = True


async def test_xclid_create_passes_proxy_and_cookies_to_client(monkeypatch):
    seen = {}
    fake_client = FakeClient()

    def fake_make_client(*, headers=None, proxy=None, cookies=None):
        seen["headers"] = headers
        seen["proxy"] = proxy
        seen["cookies"] = cookies
        return fake_client

    async def fake_get_tw_page_text(url, clt):
        seen["url"] = url
        seen["client"] = clt
        return "<html></html>"

    async def fake_load_keys(soup, clt):
        seen["load_client"] = clt
        return [1, 2, 3], "anim-key"

    monkeypatch.setattr(xclid, "_make_http_client", fake_make_client)
    monkeypatch.setattr(xclid, "get_tw_page_text", fake_get_tw_page_text)
    monkeypatch.setattr(xclid, "load_keys", fake_load_keys)

    proxy = "http://127.0.0.1:7897"
    cookies = {"auth_token": "abc", "ct0": "def"}
    gen = await xclid.XClIdGen.create(proxy=proxy, cookies=cookies)

    assert gen.vk_bytes == [1, 2, 3]
    assert gen.anim_key == "anim-key"
    assert seen["headers"] == {"user-agent": "@chrome"}
    assert seen["proxy"] == proxy
    assert seen["cookies"] == cookies
    assert seen["url"] == "https://x.com/tesla"
    assert seen["client"] is fake_client
    assert seen["load_client"] is fake_client
    assert fake_client.closed is True


async def test_xclid_create_without_proxy_or_cookies(monkeypatch):
    seen = {}
    fake_client = FakeClient()

    def fake_make_client(*, headers=None, proxy=None, cookies=None):
        seen["proxy"] = proxy
        seen["cookies"] = cookies
        return fake_client

    async def fake_get_tw_page_text(url, clt):
        return "<html></html>"

    async def fake_load_keys(soup, clt):
        return [1, 2, 3], "anim-key"

    monkeypatch.setattr(xclid, "_make_http_client", fake_make_client)
    monkeypatch.setattr(xclid, "get_tw_page_text", fake_get_tw_page_text)
    monkeypatch.setattr(xclid, "load_keys", fake_load_keys)

    await xclid.XClIdGen.create()
    assert seen == {"proxy": None, "cookies": None}


async def test_xclid_client_does_not_send_cookies_to_asset_hosts(monkeypatch):
    monkeypatch.setenv("TWS_HTTP_BACKEND", "httpx")
    client = xclid._make_client(cookies={"auth_token": "abc", "ct0": "def"})
    assert isinstance(client, HttpxClient)
    try:
        x_request = client._client.build_request("GET", "https://x.com/tesla")
        asset_request = client._client.build_request(
            "GET", "https://abs.twimg.com/x-web/client-web/main.js"
        )
        assert x_request.headers["cookie"] == "auth_token=abc; ct0=def"
        assert "cookie" not in asset_request.headers
    finally:
        await client.aclose()


async def test_xclid_default_client_scopes_cookies_to_x(monkeypatch):
    monkeypatch.delenv("TWS_HTTP_BACKEND", raising=False)
    client = xclid._make_client(cookies={"auth_token": "abc"})
    assert isinstance(client, CurlClient)
    try:
        cookies = list(client.cookies.jar)
        assert [(x.name, x.domain) for x in cookies] == [("auth_token", ".x.com")]
    finally:
        await client.aclose()


async def test_logged_out_entry_is_account_error():
    html = (
        '<script src="https://abs.twimg.com/x-web/client-web/'
        'entry-client-logged-out-a1b2c3.js"></script>'
    )

    with pytest.raises(xclid.XClIdAccountError, match="Logged-out X web app"):
        await xclid.parse_anim_idx(html, MockClient())


def test_cloudflare_challenge_shell_raises_precise_error():
    # https://github.com/vladkens/twscrape/issues/330 - X sometimes serves a
    # Cloudflare JS-detection shell (only /cdn-cgi/challenge-platform script,
    # no app bundles) instead of the web app HTML.
    html = '<script src="/cdn-cgi/challenge-platform/scripts/jsd/api.js?onload=jsdOnload"></script>'

    with pytest.raises(
        xclid.XClIdParseError, match="Cloudflare challenge page served instead of X web app"
    ):
        xclid.get_scripts_list(html)


def test_challenge_script_alongside_real_scripts_is_ignored():
    # X itself loads a challenge-platform script on normal pages; when real app
    # scripts are present the challenge marker must not break parsing.
    html = (
        '<script src="/cdn-cgi/challenge-platform/scripts/jsd/api.js"></script>'
        '<script src="https://abs.twimg.com/x-web/x-web/app-a1b2c3.js"></script>'
    )

    urls = xclid.get_scripts_list(html)
    assert urls == ["https://abs.twimg.com/x-web/x-web/app-a1b2c3.js"]


def test_script_list_combines_direct_and_reconstructed_urls():
    html = (
        '<script src="https://abs.twimg.com/x-web/x-web/app-a1b2c3.js"></script>'
        '<script src="https://abs.twimg.com/responsive-web/client-web/vendor.1234567a.js"></script>'
        '<script src="/responsive-web/client-web/main.15e48250ae23af9ea.js"></script>'
        '{100:"shared~feature"}+{100:"00c0ffee00c0ffee"}'
    )

    urls = xclid.get_scripts_list(html)

    assert urls == [
        "https://abs.twimg.com/x-web/x-web/app-a1b2c3.js",
        "https://abs.twimg.com/responsive-web/client-web/vendor.1234567a.js",
        "https://abs.twimg.com/responsive-web/client-web/main.15e48250ae23af9ea.js",
        "https://abs.twimg.com/responsive-web/client-web/shared~feature.00c0ffee00c0ffeea.js",
    ]


# In the legacy webpack fixtures the name map comes BEFORE the hash map: hash values
# must be excluded from the name map by format alone, or they overwrite the names.
def test_legacy_webpack_build_with_7_hex_hashes():
    html = '{100:"main",200:"shared~feature"}+{100:"a1b2c3d",200:"0badca4"}'

    urls = xclid.get_scripts_list(html)

    assert urls == [
        "https://abs.twimg.com/responsive-web/client-web/main.a1b2c3da.js",
        "https://abs.twimg.com/responsive-web/client-web/shared~feature.0badca4a.js",
    ]


def test_legacy_webpack_build_with_16_hex_hashes():
    # Hash format served since 2026-08-24, e.g. main.15e48250ae23af9ea.js
    # https://github.com/vladkens/twscrape/issues/327
    html = '{100:"main",200:"shared~feature"}+{100:"15e48250ae23af9e",200:"00c0ffee00c0ffee"}'

    urls = xclid.get_scripts_list(html)

    assert urls == [
        "https://abs.twimg.com/responsive-web/client-web/main.15e48250ae23af9ea.js",
        "https://abs.twimg.com/responsive-web/client-web/shared~feature.00c0ffee00c0ffeea.js",
    ]


async def test_find_indices_url_complete_scan_is_parse_error():
    client = MockClient()
    client.add_response(text="no reference")
    client.add_response(text="still no reference")

    with pytest.raises(
        xclid.XClIdParseError,
        match=r"Signing script not found \(assets: 2 loaded, 0 failed\)",
    ):
        await xclid._find_indices_url(["https://x.test/a.js", "https://x.test/b.js"], client)


async def test_find_indices_url_summarizes_transport_error(monkeypatch):
    client = MockClient()
    error = NetworkError("asset timeout")
    client.add_exception(error)
    client.add_response(text="no reference")
    messages = []
    monkeypatch.setattr(xclid.logger, "trace", messages.append)

    with pytest.raises(
        xclid.XClIdParseError,
        match=r"Signing script not found \(assets: 1 loaded, 1 failed\)",
    ):
        await xclid._find_indices_url(["https://x.test/a.js", "https://x.test/b.js"], client)

    assert messages == ["XClId asset failed: NetworkError - https://x.test/a.js"]


async def test_find_indices_url_summarizes_http_error(monkeypatch):
    client = MockClient()
    client.add_response(status_code=503)
    client.add_response(text="no reference")
    messages = []
    monkeypatch.setattr(xclid.logger, "trace", messages.append)

    with pytest.raises(
        xclid.XClIdParseError,
        match=r"Signing script not found \(assets: 1 loaded, 1 failed\)",
    ):
        await xclid._find_indices_url(["https://x.test/a.js", "https://x.test/b.js"], client)

    assert messages == ["XClId asset failed: HttpStatusError 503 - https://x.test/a.js"]


async def test_find_indices_url_returns_reference_despite_unrelated_failure():
    client = MockClient()
    client.add_exception(NetworkError("asset timeout"))
    client.add_response(text='import("./sign.o-abc123.js")')

    url = await xclid._find_indices_url(
        ["https://x.test/assets/a.js", "https://x.test/assets/b.js"], client
    )

    assert url == "https://x.test/assets/sign.o-abc123.js"


# MARK: Vite / botox chunk discovery (offline structural fixtures)

ENTRY = "https://abs.twimg.com/x-web/x-web/entry-client-CQLfyhKh.js"
BOTOX_CHUNK = "https://abs.twimg.com/x-web/x-web/assets/C_7CIfHL.js"
SIGNER = "https://abs.twimg.com/x-web/x-web/assets/f-hq5jg0.js"


async def test_find_indices_url_discovers_hashed_vite_signer_two_hops():
    # Current build: page links the entry client, which statically imports the
    # botox chunk; the botox chunk template-imports the hashed signer.
    client = AssetClient(
        {
            ENTRY: (
                'import{a as t}from"./assets/B644W4is.js";'
                'import"./assets/C_7CIfHL.js";'
                'import{x}from"./assets/CI64jODp.js"'
            ),
            BOTOX_CHUNK: (
                "const l=()=>import(`./f-hq5jg0.js`).then(e=>e.default());"
                'const m="x-botox-client-transaction-id"'
            ),
        }
    )

    url = await xclid._find_indices_url([ENTRY], client)

    assert url == SIGNER
    # Two unrelated static imports are 404s; they must not stop discovery.
    assert set(client.requested) == {
        ENTRY,
        BOTOX_CHUNK,
        "https://abs.twimg.com/x-web/x-web/assets/B644W4is.js",
        "https://abs.twimg.com/x-web/x-web/assets/CI64jODp.js",
    }


@pytest.mark.parametrize("quote", ['"', "'", "`"])
async def test_find_indices_url_botox_import_supports_literal_quotes_and_spacing(quote):
    chunk = "https://abs.twimg.com/x-web/x-web/assets/C_7CIfHL.js"
    body = (
        f"const p=()=>import( {quote}./f-hq5jg0.js{quote} )"
        f".then( v => v.default() );const m=`x-botox-client-transaction-id`"
    )
    client = AssetClient({chunk: body})

    url = await xclid._find_indices_url([chunk], client)

    assert url == SIGNER
    assert client.requested == [chunk]


async def test_find_indices_url_keeps_old_named_x_web_signer_reference():
    client = AssetClient({ENTRY: 'const a=()=>import("./assets/sign.o-abc123.js")'})

    url = await xclid._find_indices_url([ENTRY], client)

    assert url == "https://abs.twimg.com/x-web/x-web/assets/sign.o-abc123.js"


async def test_find_indices_url_does_not_follow_unmarked_default_import():
    # A generic lazy default import is not the botox signer; without the marker
    # it must stay unfetched.
    client = AssetClient({ENTRY: 'const a=()=>import("./assets/decoy.js").then(e=>e.default());'})

    with pytest.raises(xclid.XClIdParseError, match=r"assets: 1 loaded, 0 failed"):
        await xclid._find_indices_url([ENTRY], client)

    assert client.requested == [ENTRY]


async def test_find_indices_url_dedups_cycles_and_stops_after_static_expansion():
    a = "https://abs.twimg.com/x-web/x-web/assets/a.js"
    b = "https://abs.twimg.com/x-web/x-web/assets/b.js"
    c = "https://abs.twimg.com/x-web/x-web/assets/c.js"
    client = AssetClient(
        {
            ENTRY: 'import"./assets/a.js";import"./assets/b.js"',
            a: 'import"./b.js";import"../entry-client-CQLfyhKh.js"',
            b: 'import"./a.js";import"./c.js"',
            c: "import(`./f-hq5jg0.js`).then(e=>e.default());x-botox-client-transaction-id",
        }
    )

    with pytest.raises(xclid.XClIdParseError, match=r"assets: 3 loaded, 0 failed"):
        await xclid._find_indices_url([ENTRY], client)

    # c is one static level too deep; cycles and re-imports are fetched once.
    assert set(client.requested) == {ENTRY, a, b}
    assert len(client.requested) == len(set(client.requested)) == 3


async def test_find_indices_url_does_not_fetch_untrusted_embedded_urls():
    client = AssetClient(
        {
            ENTRY: (
                'import"https://evil.test/assets/x.js";'
                'import"../../../../evil/y.js";'
                'import"./assets/ok.js"'
            ),
        }
    )

    with pytest.raises(xclid.XClIdParseError):
        await xclid._find_indices_url([ENTRY], client)

    assert set(client.requested) == {
        ENTRY,
        "https://abs.twimg.com/x-web/x-web/assets/ok.js",
    }
    assert not any("evil" in url for url in client.requested)


async def test_parse_anim_idx_uses_direct_legacy_ondemand_signer(monkeypatch):
    html = (
        '<script src="https://abs.twimg.com/responsive-web/client-web/'
        'ondemand.s-abc123.js"></script>'
    )
    seen = {}

    async def fake_get_tw_page_text(url, clt):
        seen["url"] = url
        return "(x[43],16),(x[2],16)"

    monkeypatch.setattr(xclid, "get_tw_page_text", fake_get_tw_page_text)

    assert await xclid.parse_anim_idx(html, MockClient()) == [43, 2]
    assert seen["url"] == "https://abs.twimg.com/responsive-web/client-web/ondemand.s-abc123.js"


async def test_find_indices_url_cancels_inflight_fetches():
    client = BlockingAssetClient()
    task = asyncio.create_task(xclid._find_indices_url([ENTRY], client))
    await client.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.cancelled == 1


async def test_parse_anim_idx_reads_hashed_signer():
    client = AssetClient(
        {
            ENTRY: 'import"./assets/C_7CIfHL.js"',
            BOTOX_CHUNK: (
                "import(`./f-hq5jg0.js`).then(e=>e.default());"
                'const header="x-botox-client-transaction-id"'
            ),
            SIGNER: "(x[43],16),(x[2],16),(x[43],16),(x[47],16)",
        }
    )

    assert await xclid.parse_anim_idx(f'<script src="{ENTRY}"></script>', client) == [43, 2, 43, 47]


async def test_find_indices_url_caps_static_asset_requests(monkeypatch):
    monkeypatch.setattr(xclid, "_CRAWL_MAX_ASSETS", 3)
    body = "".join(f'import"./assets/chunk-{i}.js";' for i in range(10))
    client = AssetClient({ENTRY: body})

    with pytest.raises(xclid.XClIdParseError, match=r"assets: 1 loaded, 2 failed"):
        await xclid._find_indices_url([ENTRY], client)

    assert len(client.requested) == 3
