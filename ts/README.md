# @lug/client

Zero-dependency Node.js 22 client for lug. It supports framed Unix sockets and bare-JSON HTTP calls with SSE subscriptions.

```ts
import { Client, Follower } from "@lug/client";

const client = await Client.connect({
  transport: "unix",
  path: "/run/user/1000/lug/lug.sock",
  timeoutMs: 5_000,
});

const reply = await client.call({
  t: "append",
  log: "settings",
  patches: [{ Create: { theme: "dark" } }],
});

const follower = await Follower.follow(client, {
  log: "settings",
  onChange: (value, version) => console.log(version, value),
});

console.log(reply.t, follower.value);
await follower.close();
await client.close();
```

For HTTP, select the other transport. The bearer token is sent only in the `Authorization` header.

```ts
const client = await Client.connect({
  transport: "http",
  baseUrl: "http://127.0.0.1:8080",
  token: process.env.LUG_TOKEN!,
  timeoutMs: 5_000,
});
```

`call` accepts append, read, list, create, and ping requests. Request IDs are
allocated by the client. `subscribe` returns an `AsyncIterable<Response>` of
what the stream carries: `view`, `records`, `gap`, `end`. The frames that only
open or steer a stream never reach the consumer. The daemon answers `Subscribe`
with `Ok` on a socket and with `Welcome` over SSE, and answers every `Credit`
with `Ok`; the transport absorbs all three, so an acknowledgement can never be
mistaken for data.

Credit is granted one record at a time, when the consumer pulls. A consumer
that stops pulling stops the daemon pushing. A reducible subscription delivers
its view before any credit is granted, because the view is not a record.

A `gap` says versions were reclaimed before this subscriber reached them. It is
part of the stream rather than an error beside it, so a consumer can see what it
lost. `Follower` cannot fold across one and fails with `LugGapError` instead of
jumping versions silently.

`Response.View.value` is the bare document. The version is the frame's own
field, so there is nothing to unwrap.

Protocol `u64` values use JavaScript numbers. A version or id the client cannot
hold exactly is refused: `at` and `from` throw before anything is sent, and a
frame carrying one is rejected rather than rounded. Numbers inside a document
are ordinary JSON and are parsed as doubles, so a value above
`Number.MAX_SAFE_INTEGER` stored by another client reads back rounded here.

## Tests

```sh
npm test
npx tsc --noEmit
```

`test/interop.test.ts` is a conformance suite against the real daemon: it
starts `target/release/lug-server` with a generated config in a temp dir and
drives it over both transports, including the raw frames the client normally
consumes on the consumer's behalf. Build the workspace first with
`cargo build --release`; without those binaries the suite skips and says so.
