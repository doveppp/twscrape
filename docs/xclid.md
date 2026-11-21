# X client transaction ID resource discovery

`twscrape/xclid.py` derives transaction IDs from the verification key and loading
animation in the account's X page. X serves different web builds to different
accounts, so resource discovery supports both legacy webpack bundles and Vite
modules.

- Legacy bundles reference `ondemand.s` or `sign.o` by name.
- Newer Vite entries import shared static chunks. The chunk containing
  `x-botox-client-transaction-id` lazily imports a content-hashed signing module;
  its filename is not stable. Discovery follows bounded static dependencies and
  resolves that lazy import rather than downloading every dynamic feature.

An unrelated asset failure, such as an unavailable chat manifest, does not stop
discovery when another resource identifies the signing module. If discovery
fails during a queued request, `XClIdParseError` counts toward the account's
transaction-ID failure limit. The queue rotates accounts after a 15-minute
global cooldown is recorded for the failed account. The third consecutive
failure deactivates the account. Search probes use their own confirmation rules.

Enable TRACE on the logging sink to see individual asset failures. The final
error includes discovery counters; those counters describe scanned resources,
not search API responses.

Offline regression tests run without account credentials or network access:

```bash
uv run --frozen pytest -q tests/test_xclid.py
```
