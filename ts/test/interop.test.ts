// Cross-language conformance: the TypeScript client against a real lug-server.
//
// Everything here drives the actual daemon over a real socket or real HTTP.
// The only assertions made without it are the ones about frames the daemon
// must never send, which is exactly where a mock is the honest instrument.

import assert from "node:assert/strict";
import { connect as connectSocket, type Socket } from "node:net";
import { after, before, describe, it } from "node:test";
import {
  Client,
  encodeFrame,
  Follower,
  FrameDecoder,
  LugGapError,
} from "../src/index.js";
import type { JsonValue, Request, Response } from "../src/index.js";
import { isResponse } from "../src/wire.js";
import { missingBinaries, startDaemon, type Daemon } from "./daemon.js";

const missing = missingBinaries();

// Every response tag the daemon actually emitted at us, collected across the
// whole file. The last test refuses to pass until all of them showed up, so a
// variant that quietly stopped being exercised is caught rather than assumed.
const decoded = new Set<string>();

function note<T extends Response>(response: T): T {
  decoded.add(response.t);
  return response;
}

describe("the response union", () => {
  it("accepts every variant lug-proto declares", () => {
    const variants: Response[] = [
      { t: "welcome", id: 1, version: 1, max_frame: 16 * 1024 * 1024 },
      { t: "ack", id: 1, versions: [1, 2], synced: 2 },
      { t: "records", id: 1, records: [{ version: 1, patch: { Create: {} } }] },
      { t: "view", id: 1, version: 4, value: { a: 1 } },
      { t: "gap", id: 1, from: 0, to: 60 },
      { t: "logs", id: 1, logs: [] },
      { t: "ok", id: 1 },
      { t: "end", id: 1 },
      { t: "pong", id: 1 },
      { t: "error", id: 1, code: "no_such_log", message: "no log x" },
    ];
    for (const variant of variants) {
      assert.equal(isResponse(variant), true, `${variant.t} must decode`);
    }
  });

  it("refuses a tag it does not know rather than passing it through", () => {
    assert.equal(isResponse({ t: "aria", id: 1 }), false);
    assert.equal(isResponse({ t: "ok" }), false);
    assert.equal(isResponse({ id: 1 }), false);
    assert.equal(isResponse({ t: "error", id: 1, code: "oops", message: "" }), false);
  });

  it("refuses a u64 it cannot hold exactly", () => {
    const beyond = Number.MAX_SAFE_INTEGER + 2;
    assert.equal(isResponse({ t: "pong", id: beyond }), false);
    assert.equal(isResponse({ t: "view", id: 1, version: beyond, value: null }), false);
    assert.equal(isResponse({ t: "ack", id: 1, versions: [beyond], synced: 1 }), false);
    assert.equal(isResponse({ t: "gap", id: 1, from: 0, to: beyond }), false);
    assert.equal(isResponse({ t: "pong", id: Number.MAX_SAFE_INTEGER }), true);
  });
});

for (const transport of ["unix", "http"] as const) {
  describe(`conformance over ${transport}`, { skip: missing }, () => {
    let daemon: Daemon;
    let client: Client;

    before(async () => {
      daemon = await startDaemon();
      client = await open(daemon, transport);
    });

    after(async () => {
      await client?.close();
      await daemon?.stop();
    });

    it("answers a ping, which means the hello handshake was accepted", async () => {
      const pong = note(await client.call({ t: "ping" }));
      assert.equal(pong.t, "pong");
    });

    it("creates a log, reports it as it stands, and is idempotent", async () => {
      const created = note(await client.call({ t: "create", log: "notes", reducible: true }));
      assert.equal(created.t, "logs");
      assert.deepEqual(created.logs, [
        { name: "notes", reducible: true, version: 0, oldest: 0, subscribers: 0 },
      ]);

      const again = note(await client.call({ t: "create", log: "notes", reducible: true }));
      assert.equal(again.t, "logs");

      const conflict = note(await client.call({ t: "create", log: "notes", reducible: false }));
      assert.equal(conflict.t, "error");
      assert.equal(conflict.code, "log_exists");
    });

    it("acknowledges one version per patch that changed state", async () => {
      await client.call({ t: "create", log: "acks", reducible: true });
      const ack = note(
        await client.call({
          t: "append",
          log: "acks",
          patches: [{ Create: { count: 0 } }, { Update: { count: 1 } }],
        }),
      );
      assert.equal(ack.t, "ack");
      assert.deepEqual(ack.versions, [1, 2]);
      assert.equal(Number.isSafeInteger(ack.synced), true);

      // A patch that changes nothing mints no version, so versions is shorter
      // than the batch that produced it.
      const idle = note(
        await client.call({
          t: "append",
          log: "acks",
          patches: [{ Update: { count: 1 } }, { Update: { count: 2 } }],
          durability: "durable",
        }),
      );
      assert.equal(idle.t, "ack");
      assert.deepEqual(idle.versions, [3]);
    });

    it("reads the bare document, not a view wrapper", async () => {
      await client.call({ t: "create", log: "bare", reducible: true });
      await client.call({
        t: "append",
        log: "bare",
        patches: [{ Create: { theme: "dark", nested: { depth: 1 } } }],
      });
      const view = note(await client.call({ t: "read", log: "bare" }));
      assert.equal(view.t, "view");
      assert.equal(view.version, 1);
      assert.deepEqual(view.value, { theme: "dark", nested: { depth: 1 } });

      // The version lives in the frame. A client that still unwrapped a
      // pointer would find these keys; there is nothing to unwrap.
      const value = view.value as Record<string, JsonValue>;
      for (const wrapper of ["root", "state", "version", "snapshot"]) {
        assert.equal(Object.hasOwn(value, wrapper), false, `value carries ${wrapper}`);
      }
    });

    it("reads a past version and refuses one it never had", async () => {
      await client.call({ t: "create", log: "history", reducible: true });
      await client.call({
        t: "append",
        log: "history",
        patches: [{ Create: { step: 1 } }, { Update: { step: 2 } }],
      });
      const first = note(await client.call({ t: "read", log: "history", at: 1 }));
      assert.equal(first.t, "view");
      assert.deepEqual(first.value, { step: 1 });
      assert.equal(first.version, 1);

      const ahead = note(await client.call({ t: "read", log: "history", at: 99 }));
      assert.equal(ahead.t, "error");
      assert.equal(ahead.code, "out_of_range");
    });

    it("reports the errors the contract names", async () => {
      const absent = note(await client.call({ t: "read", log: "nowhere" }));
      assert.equal(absent.t, "error");
      assert.equal(absent.code, "no_such_log");

      await client.call({ t: "create", log: "plain", reducible: false });
      const plain = note(await client.call({ t: "read", log: "plain" }));
      assert.equal(plain.t, "error");
      assert.equal(plain.code, "not_reducible");

      await client.call({ t: "create", log: "rejects", reducible: true });
      await client.call({ t: "append", log: "rejects", patches: [{ Create: { tree: { leaf: 1 } } }] });
      // The same patch applyCavlcPatch refuses: a leaf cannot replace an
      // object without deleting it first.
      const rejected = note(
        await client.call({
          t: "append",
          log: "rejects",
          patches: [{ Update: { tree: 7 } }],
        }),
      );
      assert.equal(rejected.t, "error");
      assert.equal(rejected.code, "rejected");
    });

    it("lists every log with its version and retention floor", async () => {
      const logs = note(await client.call({ t: "list" }));
      assert.equal(logs.t, "logs");
      const names = logs.logs.map((log) => log.name);
      assert.equal(names.includes("notes"), true);
      for (const info of logs.logs) {
        assert.equal(Number.isSafeInteger(info.version), true);
        assert.equal(Number.isSafeInteger(info.oldest), true);
      }
    });

    it("pushes records only, one credit at a time, and releases the stream on cancel", async () => {
      await client.call({ t: "create", log: "tail", reducible: true });
      await client.call({
        t: "append",
        log: "tail",
        patches: [{ Create: { a: 1 } }, { Create: { b: 2 } }, { Create: { c: 3 } }],
      });

      const iterator = client.subscribe({ log: "tail", from: 0 })[Symbol.asyncIterator]();
      const versions: number[] = [];
      for (let pulled = 0; pulled < 3; pulled += 1) {
        const next = await iterator.next();
        assert.equal(next.done, false);
        const frame = note(next.value as Response);
        // An Ok opens the stream and answers every grant. If the transport let
        // one through it would arrive here, pretending to be data.
        assert.equal(frame.t, "records");
        if (frame.t === "records") {
          assert.equal(frame.records.length, 1, "one credit buys one record");
          for (const record of frame.records) {
            versions.push(record.version);
          }
        }
      }
      assert.deepEqual(versions, [1, 2, 3]);

      await iterator.return?.();
      await waitFor(async () => (await subscribers(client, "tail")) === 0, "stream to close");
    });

    it("opens a reducible subscription on its view", async () => {
      await client.call({ t: "create", log: "reducible", reducible: true });
      await client.call({ t: "append", log: "reducible", patches: [{ Create: { seen: 1 } }] });

      const iterator = client
        .subscribe({ log: "reducible", mode: "reducible" })
        [Symbol.asyncIterator]();
      const first = await iterator.next();
      const view = note(first.value as Response);
      assert.equal(view.t, "view");
      if (view.t === "view") {
        assert.deepEqual(view.value, { seen: 1 });
        assert.equal(view.version, 1);
      }

      const pushed = client.call({
        t: "append",
        log: "reducible",
        patches: [{ Update: { seen: 2 } }],
      });
      const next = await iterator.next();
      const records = note(next.value as Response);
      assert.equal(records.t, "records");
      if (records.t === "records") {
        assert.deepEqual(records.records.map((record) => record.version), [2]);
      }
      await pushed;
      await iterator.return?.();
    });

    it("refuses to follow a log that keeps no view", async () => {
      await client.call({ t: "create", log: "noview", reducible: false });
      await assert.rejects(
        Follower.follow(client, { log: "noview" }),
        /not_reducible/,
      );
    });

    it("converges on the document the lug CLI prints", async () => {
      const log = "converge";
      await client.call({ t: "create", log, reducible: true });
      await client.call({ t: "append", log, patches: seed });

      const versions: number[] = [];
      const follower = await Follower.follow(client, {
        log,
        onChange: (_value, version) => versions.push(version),
      });
      assert.equal(follower.version, seed.length);

      // The rest arrive as pushed records, which is the part that tests
      // whether the TypeScript fold agrees with cavlc rather than with itself.
      await client.call({ t: "append", log, patches: rest });
      const target = seed.length + rest.length;
      await waitFor(async () => follower.version === target, `follower to reach ${target}`);

      const printed = daemon.cli(["read", log, "--json"]);
      assert.equal(printed.status, 0, printed.stderr);
      const authoritative = JSON.parse(printed.stdout) as {
        version: number;
        value: JsonValue;
      };
      assert.equal(follower.version, authoritative.version);
      // Canonical rather than literal bytes: cavlc stores an object as a
      // sorted AVL map and the fold here keeps insertion order, so the two
      // print the same document with keys in different places.
      assert.equal(canonical(follower.value), canonical(authoritative.value));

      const view = note(await client.call({ t: "read", log }));
      assert.equal(view.t, "view");
      if (view.t === "view") {
        assert.equal(canonical(view.value), canonical(authoritative.value));
      }
      for (const version of versions) {
        assert.equal(Number.isSafeInteger(version), true);
      }
      await follower.close();
      await follower.finished;
    });

    it("refuses a version it cannot hold exactly, before it reaches the wire", () => {
      const beyond = Number.MAX_SAFE_INTEGER + 2;
      assert.throws(() => client.subscribe({ log: "notes", from: beyond }), TypeError);
      assert.throws(
        () => void client.call({ t: "read", log: "notes", at: beyond }),
        TypeError,
      );
      assert.throws(() => client.subscribe({ log: "notes", from: -1 }), TypeError);
    });
  });
}

describe("a bad bearer token", { skip: missing }, () => {
  let daemon: Daemon;

  before(async () => {
    daemon = await startDaemon();
  });

  after(async () => {
    await daemon?.stop();
  });

  it("is answered with an unauthorized frame, not a bare status", async () => {
    const client = await Client.connect({
      transport: "http",
      baseUrl: daemon.baseUrl,
      token: "not-the-token",
      timeoutMs: 5_000,
    });
    try {
      const refused = note(await client.call({ t: "ping" }));
      assert.equal(refused.t, "error");
      assert.equal(refused.code, "unauthorized");
    } finally {
      await client.close();
    }
  });
});

describe("credit against the daemon itself", { skip: missing }, () => {
  let daemon: Daemon;

  before(async () => {
    daemon = await startDaemon();
    const client = await Client.connect({
      transport: "unix",
      path: daemon.socket,
      timeoutMs: 5_000,
    });
    await client.call({ t: "create", log: "flow", reducible: true });
    await client.call({
      t: "append",
      log: "flow",
      patches: Array.from({ length: 10 }, (_value, index) => ({
        Create: { [`k${index}`]: index },
      })),
    });
    await client.close();
  });

  after(async () => {
    await daemon?.stop();
  });

  it("stops pushing at the grant and resumes when more arrives, on a socket", async () => {
    const wire = await RawSocket.connect(daemon.socket);
    try {
      const welcome = note(await wire.call({ t: "hello", id: 1, version: 1 }));
      assert.equal(welcome.t, "welcome");
      if (welcome.t === "welcome") {
        assert.equal(welcome.version, 1);
        assert.equal(welcome.session, undefined, "a socket correlates itself");
      }

      wire.send({ t: "subscribe", id: 2, log: "flow", from: 0, mode: "records", credit: 3 });
      const opened = note(await wire.next());
      assert.equal(opened.t, "ok", "a subscription is acknowledged before it pushes");

      const first = await wire.harvest(300);
      assert.deepEqual(records(first, note), [1, 2, 3], "the grant is a ceiling");

      wire.send({ t: "credit", id: 2, grant: 4 });
      const second = await wire.harvest(300);
      assert.equal(
        second.filter((response) => response.t === "ok").length,
        1,
        "a grant is answered on the stream's own id",
      );
      assert.deepEqual(records(second, note), [4, 5, 6, 7]);

      wire.send({ t: "cancel", id: 2 });
      const ended = note(await wire.until((response) => response.t === "end"));
      assert.equal(ended.t, "end");

      // A stream that was never opened is an error, not silence.
      wire.send({ t: "cancel", id: 99 });
      const bad = note(await wire.next());
      assert.equal(bad.t, "error");
      if (bad.t === "error") {
        assert.equal(bad.code, "bad_id");
      }
    } finally {
      wire.close();
    }
  });

  it("stops pushing at the grant and resumes when more arrives, over SSE", async () => {
    const sse = await RawSse.open(daemon, {
      id: "5",
      log: "flow",
      from: "0",
      mode: "records",
      credit: "2",
    });
    try {
      const welcome = note(await sse.next());
      assert.equal(welcome.t, "welcome");
      const session = welcome.t === "welcome" ? welcome.session : undefined;
      assert.notEqual(session, undefined, "SSE carries the session to steer with");

      assert.deepEqual(records(await sse.harvest(400), note), [1, 2]);

      const granted = note(await post(daemon, { t: "credit", id: 5, grant: 3 }, session));
      assert.equal(granted.t, "ok");
      assert.deepEqual(records(await sse.harvest(400), note), [3, 4, 5]);

      const cancelled = note(await post(daemon, { t: "cancel", id: 5 }, session));
      assert.equal(cancelled.t, "end");
    } finally {
      await sse.close();
    }
  });
});

describe("a log that outran its retention", { skip: missing }, () => {
  let daemon: Daemon;
  let reclaimed = 0;

  before(async () => {
    // A tiny ring, the smallest segment the daemon accepts, and a checkpoint
    // after every version, which is what lets segments be deleted.
    daemon = await startDaemon({ ring: 2, segment: 4096, checkpointEvery: 1 });
    const client = await Client.connect({
      transport: "unix",
      path: daemon.socket,
      timeoutMs: 10_000,
    });
    await client.call({ t: "create", log: "churn", reducible: true });
    const padding = "x".repeat(64);
    for (let batch = 0; batch < 4; batch += 1) {
      await client.call({
        t: "append",
        log: "churn",
        patches: Array.from({ length: 50 }, (_value, index) => ({
          Create: { [`k${batch}_${index}`]: padding },
        })),
      });
    }
    const logs = await client.call({ t: "list" });
    if (logs.t === "logs") {
      reclaimed = logs.logs.find((log) => log.name === "churn")?.oldest ?? 0;
    }
    await client.close();
  });

  after(async () => {
    await daemon?.stop();
  });

  it("reclaimed something to leave a hole in", () => {
    assert.equal(reclaimed > 1, true, `oldest is ${reclaimed}, nothing was reclaimed`);
  });

  for (const transport of ["unix", "http"] as const) {
    it(`reports the hole as a gap over ${transport}, not as a jump in versions`, async () => {
      const client = await open(daemon, transport);
      try {
        const iterator = client.subscribe({ log: "churn", from: 0 })[Symbol.asyncIterator]();
        const first = note((await iterator.next()).value as Response);
        assert.equal(first.t, "gap");
        if (first.t !== "gap") {
          return;
        }
        assert.equal(first.from, 0);
        assert.equal(first.to, reclaimed - 1);

        const resumed = note((await iterator.next()).value as Response);
        assert.equal(resumed.t, "records");
        if (resumed.t === "records") {
          assert.equal(resumed.records[0]?.version, first.to + 1);
        }
        await iterator.return?.();
      } finally {
        await client.close();
      }
    });
  }

  it("fails a follower loudly when its versions are gone", async () => {
    const client = await Client.connect({
      transport: "unix",
      path: daemon.socket,
      timeoutMs: 5_000,
    });
    try {
      // Reducible mode adopts the current view, so the only way to put a
      // follower behind retention is to hand it the raw stream a follower
      // folds. A gap there is a LugGapError and not a silent resync.
      const iterator = client.subscribe({ log: "churn", from: 0 })[Symbol.asyncIterator]();
      const frame = (await iterator.next()).value as Response;
      assert.equal(frame.t, "gap");
      if (frame.t === "gap") {
        const error = new LugGapError(frame.from, frame.to);
        assert.equal(error.from, frame.from);
        assert.equal(error.to, frame.to);
        assert.match(error.message, /reclaimed/);
      }
      await iterator.return?.();
    } finally {
      await client.close();
    }
  });
});

describe("integers wider than a double", { skip: missing }, () => {
  let daemon: Daemon;

  before(async () => {
    daemon = await startDaemon();
  });

  after(async () => {
    await daemon?.stop();
  });

  it("guards protocol versions and ids, and rounds document values", async () => {
    const client = await Client.connect({
      transport: "unix",
      path: daemon.socket,
      timeoutMs: 5_000,
    });
    try {
      await client.call({ t: "create", log: "wide", reducible: true });
      // Written as text through the other client, because JSON.stringify here
      // would round the digits off before they ever reached the daemon.
      const appended = daemon.cli(
        ["append", "wide", "-"],
        '{"Create":{"big":9007199254740993}}\n',
      );
      assert.equal(appended.status, 0, appended.stderr);

      const printed = daemon.cli(["read", "wide", "--json"]);
      assert.match(printed.stdout, /9007199254740993/, "the daemon holds the exact u64");

      // The client's guard covers ids and versions, which are protocol fields
      // it compares and counts with. A number inside a document is JSON.parse
      // territory and comes back rounded. Nothing in the client can hide that,
      // so the limitation is asserted rather than implied.
      const view = note(await client.call({ t: "read", log: "wide" }));
      assert.equal(view.t, "view");
      if (view.t === "view") {
        const value = view.value as { big: number };
        assert.equal(value.big, 9007199254740992);
        assert.equal(Number.isSafeInteger(value.big), false);
        assert.equal(Number.isSafeInteger(view.version), true);
      }
    } finally {
      await client.close();
    }
  });
});

describe("the variants actually seen", { skip: missing }, () => {
  it("covers every response lug-proto can put on the wire", () => {
    const required = [
      "welcome",
      "ok",
      "ack",
      "records",
      "view",
      "gap",
      "logs",
      "end",
      "pong",
      "error",
    ];
    const absent = required.filter((variant) => !decoded.has(variant));
    assert.deepEqual(absent, [], `never decoded: ${absent.join(", ")}`);
  });
});

const seed: JsonValue[] = [
  {
    Create: {
      app: {
        name: "lug",
        tags: ["log", "daemon"],
        limits: { inbox: 4096, batch: 1024 },
      },
      count: 0,
    },
  },
  { Update: { count: 1 } },
];

const rest: JsonValue[] = [
  { Update: { app: { Update: { name: "lug-server" } } } },
  { Update: { app: { Create: { owner: { uid: 1000, groups: [10, 20] } } } } },
  { Update: { app: { Update: { limits: { Update: { inbox: 8192 } } } } } },
  // An array is an opaque leaf, replaced whole rather than descended into.
  { Update: { app: { Update: { tags: ["log", "daemon", "mvcc"] } } } },
  { Update: { app: { Delete: ["limits"] } } },
  { Create: { pending: null } },
  { Update: { pending: true } },
  { Update: { app: { Update: { owner: { Update: { uid: 1001 } } } } } },
  { Update: { app: { Update: { owner: { Delete: ["groups"] } } } } },
  { Delete: ["count"] },
];

async function open(daemon: Daemon, transport: "unix" | "http"): Promise<Client> {
  return transport === "unix"
    ? Client.connect({ transport: "unix", path: daemon.socket, timeoutMs: 10_000 })
    : Client.connect({
        transport: "http",
        baseUrl: daemon.baseUrl,
        token: daemon.token,
        timeoutMs: 10_000,
      });
}

async function subscribers(client: Client, log: string): Promise<number> {
  const logs = await client.call({ t: "list" });
  if (logs.t !== "logs") {
    throw new Error(`list answered ${logs.t}`);
  }
  return logs.logs.find((info) => info.name === log)?.subscribers ?? 0;
}

function records(responses: Response[], seen: (response: Response) => unknown): number[] {
  const versions: number[] = [];
  for (const response of responses) {
    seen(response);
    if (response.t === "records") {
      for (const record of response.records) {
        versions.push(record.version);
      }
    }
  }
  return versions;
}

function canonical(value: JsonValue): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: JsonValue): JsonValue {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (typeof value === "object" && value !== null) {
    const ordered: { [key: string]: JsonValue } = {};
    for (const key of Object.keys(value).sort()) {
      ordered[key] = sortKeys(value[key] as JsonValue);
    }
    return ordered;
  }
  return value;
}

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  what: string,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await condition()) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await sleep(10);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// The framed socket without the client in the way, for the frames the client
// consumes on the consumer's behalf.
class RawSocket {
  readonly #socket: Socket;
  readonly #decoder = new FrameDecoder<unknown>();
  readonly #queue: Response[] = [];
  #waiting: ((response: Response) => void) | undefined;
  #failure: unknown;

  private constructor(socket: Socket) {
    this.#socket = socket;
    socket.on("data", (chunk: Buffer) => {
      try {
        for (const frame of this.#decoder.push(chunk)) {
          if (!isResponse(frame)) {
            throw new Error(`daemon sent a frame the union rejects: ${JSON.stringify(frame)}`);
          }
          const waiter = this.#waiting;
          this.#waiting = undefined;
          if (waiter === undefined) {
            this.#queue.push(frame);
          } else {
            waiter(frame);
          }
        }
      } catch (error) {
        this.#failure = error;
      }
    });
  }

  public static async connect(path: string): Promise<RawSocket> {
    const socket = connectSocket(path);
    await new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    return new RawSocket(socket);
  }

  public send(request: Request): void {
    this.#socket.write(encodeFrame(request));
  }

  public async call(request: Request): Promise<Response> {
    this.send(request);
    return this.next();
  }

  public async next(timeoutMs = 5_000): Promise<Response> {
    this.#throwIfFailed();
    const queued = this.#queue.shift();
    if (queued !== undefined) {
      return queued;
    }
    return new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#waiting = undefined;
        reject(new Error("no frame arrived in time"));
      }, timeoutMs);
      this.#waiting = (response) => {
        clearTimeout(timer);
        resolve(response);
      };
    });
  }

  public async until(matches: (response: Response) => boolean): Promise<Response> {
    for (;;) {
      const response = await this.next();
      if (matches(response)) {
        return response;
      }
    }
  }

  // Everything the daemon sends within the window. Silence is the assertion:
  // a server that pushed past its grant shows up as extra frames here.
  public async harvest(ms: number): Promise<Response[]> {
    await sleep(ms);
    this.#throwIfFailed();
    return this.#queue.splice(0);
  }

  public close(): void {
    this.#socket.destroy();
  }

  #throwIfFailed(): void {
    if (this.#failure !== undefined) {
      throw this.#failure;
    }
  }
}

// The SSE half of the same idea: read the raw event stream and steer it with
// calls, the way a browser would have to.
class RawSse {
  readonly #controller: AbortController;
  readonly #queue: Response[] = [];
  #text = "";
  #failure: unknown;
  #done = false;

  private constructor(
    controller: AbortController,
    body: ReadableStream<Uint8Array>,
  ) {
    this.#controller = controller;
    void this.#drain(body.getReader());
  }

  public static async open(
    daemon: Daemon,
    query: Record<string, string>,
  ): Promise<RawSse> {
    const url = new URL("/v1/stream", daemon.baseUrl);
    for (const [key, value] of Object.entries(query)) {
      url.searchParams.set(key, value);
    }
    const controller = new AbortController();
    const response = await fetch(url, {
      headers: {
        accept: "text/event-stream",
        authorization: `Bearer ${daemon.token}`,
      },
      signal: controller.signal,
    });
    if (!response.ok || response.body === null) {
      throw new Error(`stream refused with ${response.status}`);
    }
    return new RawSse(controller, response.body);
  }

  public async next(timeoutMs = 5_000): Promise<Response> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      this.#throwIfFailed();
      const queued = this.#queue.shift();
      if (queued !== undefined) {
        return queued;
      }
      if (Date.now() > deadline || this.#done) {
        throw new Error("no SSE event arrived in time");
      }
      await sleep(10);
    }
  }

  // Everything pushed within the window. Silence here is the assertion.
  public async harvest(ms: number): Promise<Response[]> {
    await sleep(ms);
    this.#throwIfFailed();
    return this.#queue.splice(0);
  }

  public async close(): Promise<void> {
    this.#controller.abort();
    await sleep(0);
  }

  async #drain(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          this.#done = true;
          return;
        }
        this.#text += new TextDecoder().decode(value);
        this.#events();
      }
    } catch (error) {
      this.#done = true;
      if (!this.#controller.signal.aborted) {
        this.#failure = error;
      }
    }
  }

  #events(): void {
    for (;;) {
      const boundary = this.#text.indexOf("\n\n");
      if (boundary === -1) {
        return;
      }
      const event = this.#text.slice(0, boundary);
      this.#text = this.#text.slice(boundary + 2);
      for (const line of event.split("\n")) {
        if (!line.startsWith("data:")) {
          continue;
        }
        const parsed: unknown = JSON.parse(line.slice(5).trim());
        if (!isResponse(parsed)) {
          this.#failure = new Error(`daemon sent an event the union rejects: ${line}`);
          return;
        }
        this.#queue.push(parsed);
      }
    }
  }

  #throwIfFailed(): void {
    if (this.#failure !== undefined) {
      throw this.#failure;
    }
  }
}

async function post(
  daemon: Daemon,
  request: Request,
  session: string | undefined,
): Promise<Response> {
  const headers: Record<string, string> = {
    authorization: `Bearer ${daemon.token}`,
    "content-type": "application/json",
  };
  if (session !== undefined) {
    headers["x-lug-session"] = session;
  }
  const response = await fetch(new URL("/v1/call", daemon.baseUrl), {
    method: "POST",
    headers,
    body: JSON.stringify(request),
  });
  const parsed: unknown = await response.json();
  if (!isResponse(parsed)) {
    throw new Error(`daemon answered with something the union rejects: ${JSON.stringify(parsed)}`);
  }
  return parsed;
}
