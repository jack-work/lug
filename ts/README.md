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

`call` accepts append, read, list, create, and ping requests. Request IDs are allocated by the client. `subscribe` returns an `AsyncIterable<Response>` and grants one record of credit when the consumer pulls. A reducible subscription delivers its view before granting record credit.

Protocol `u64` values use JavaScript numbers. Values above `Number.MAX_SAFE_INTEGER` cannot be represented exactly and should not be used with this client.
