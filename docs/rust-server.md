# The Rust server

This branch integrates [daviddesmet's Rust server](https://github.com/daviddesmet/pairdrop-rs) with the web client from our PairDrop fork. The Node implementation in `server/` is available for comparison and existing tests.

## The service worker cache

`public/service-worker.js` is cache-first with no revalidation, and its `activate` handler only
deletes caches whose name differs from the current one. Upstream hardcodes that name as the
release version:

```js
const cacheVersion = 'v1.11.2';
```

So a cache populated from stale HTTP responses keeps serving them forever: the name still
matches, nothing is deleted, nothing is re-fetched. A stale `index.html` beside a current
`main.js` makes every deferred script load twice, raising `Uncaught SyntaxError: Identifier
'Events' has already been declared`. `network.js` aborts, the WebSocket never opens, and the
display name never arrives. A hard reload bypasses the service worker and looks fine; the next normal load is
broken again. Only clearing site data, by hand, on every device, gets out of it.

`src/sw.rs` serves the worker with the version replaced by a digest of everything else in
`public/`:

```
const cacheVersion = 'sha-5234c7fae57e99f6';
```

Change any asset and the name changes, so `activate` drops the old cache and refills it. The skew
cannot outlive a deploy, and the first visit after cutover clears whatever a device was holding.

Static responses also carry `Cache-Control: no-cache`, because `cache.addAll()` fetches through
the ordinary HTTP cache. Without it the service worker can bake a heuristically cached copy into
its own cache, which is how the cache gets poisoned in the first place. ETags still answer `304`,
so revalidation stays cheap.

## Healthcheck

The runtime image is `scratch`, so there is no `wget`, `curl` or shell for `HEALTHCHECK` to call.
The binary answers for itself:

```
pairdrop-rs --healthcheck
```

It opens one TCP connection to `127.0.0.1:$PORT`, sends `GET /config`, and exits `0` only on
`HTTP/1.1 200`.

**It must not half-close the socket.** An earlier version called `shutdown(Write)` after writing
the request, which is ordinarily polite; hyper reads that as an aborted connection and sends back
nothing at all, so the probe reported `unhealthy` against a perfectly healthy server. Write the
request, then read. No shutdown.

## Configuration

Set `TRUSTED_PROXIES` for deployments behind a reverse proxy. Use `PEER_ID_SECRET` to keep reconnecting clients' identities stable after a Rust server restart. See the [hosting guide](host-your-own.md#environment-variables) for the settings.

## Running it

```sh
cargo run
# Or build and run the container
docker compose up -d --build
```
