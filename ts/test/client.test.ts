import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  Client,
  Follower,
  FrameDecoder,
  LugConnectionError,
  LugGapError,
  LugTimeoutError,
  encodeFrame,
} from "../src/index.js";
import type { JsonValue, Request, Response } from "../src/index.js";

interface UnixFixture {
  path: string;
  close(): Promise<void>;
}

async function unixFixture(
  onRequest: (request: Request, socket: Socket) => void,
): Promise<UnixFixture> {
  const directory = await mkdtemp(join(tmpdir(), "lug-ts-"));
  const path = join(directory, "lug.sock");
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    const decoder = new FrameDecoder<Request>();
    socket.on("data", (chunk) => {
      for (const request of decoder.push(chunk)) {
        if (request.t === "hello") {
          const welcome: Response = {
            t: "welcome",
            id: request.id,
            version: 1,
            max_frame: 16 * 1024 * 1024,
          };
          const frame = encodeFrame(welcome);
          socket.write(frame.subarray(0, 3));
          socket.write(frame.subarray(3));
        } else {
          onRequest(request, socket);
        }
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(path, resolve);
  });
  return {
    path,
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(directory, { recursive: true, force: true });
    },
  };
}

test("unix client multiplexes replies delivered together and out of order", async () => {
  const waiting: Extract<Request, { t: "ping" }>[] = [];
  const fixture = await unixFixture((request, socket) => {
    assert.equal(request.t, "ping");
    waiting.push(request);
    if (waiting.length === 40) {
      const frames = waiting
        .reverse()
        .map((ping) => encodeFrame({ t: "pong", id: ping.id } satisfies Response));
      socket.write(concat(...frames));
    }
  });
  const client = await Client.connect({
    transport: "unix",
    path: fixture.path,
    timeoutMs: 1_000,
  });
  try {
    const replies = await Promise.all(
      Array.from({ length: 40 }, () => client.call({ t: "ping" })),
    );
    assert.equal(new Set(replies.map((reply) => reply.id)).size, 40);
    assert.ok(replies.every((reply) => reply.t === "pong"));
  } finally {
    await client.close();
    await fixture.close();
  }
});

test("unix client times out calls and rejects all pending calls on disconnect", async () => {
  let pendingBeforeClose = 0;
  const fixture = await unixFixture((request, socket) => {
    if (request.t === "read") {
      return;
    }
    pendingBeforeClose += 1;
    if (pendingBeforeClose === 2) {
      socket.destroy();
    }
  });
  const client = await Client.connect({
    transport: "unix",
    path: fixture.path,
    timeoutMs: 500,
  });
  try {
    await assert.rejects(
      client.call({ t: "read", log: "quiet" }, 20),
      LugTimeoutError,
    );
    const one = client.call({ t: "list" });
    const two = client.call({ t: "create", log: "x", reducible: false });
    await assert.rejects(one, LugConnectionError);
    await assert.rejects(two, LugConnectionError);
  } finally {
    await client.close();
    await fixture.close();
  }
});

test("a response tag the union does not know kills the connection", async () => {
  const fixture = await unixFixture((request, socket) => {
    // A variant from some future protocol. Passing it through as data would
    // be worse than refusing it, so the client is expected to be loud.
    socket.write(encodeFrame({ t: "aria", id: request.id }));
  });
  const client = await Client.connect({
    transport: "unix",
    path: fixture.path,
    timeoutMs: 500,
  });
  try {
    await assert.rejects(client.call({ t: "ping" }), (error: unknown) => {
      assert.ok(error instanceof LugConnectionError);
      assert.match(error.message, /without a valid id or tag/);
      return true;
    });
  } finally {
    await client.close();
    await fixture.close();
  }
});

test("a follower refuses to fold across a gap", async () => {
  const fixture = await unixFixture((request, socket) => {
    if (request.t === "subscribe") {
      socket.write(encodeFrame({ t: "ok", id: request.id } satisfies Response));
      socket.write(
        encodeFrame({
          t: "view",
          id: request.id,
          version: 7,
          value: { count: 7 },
        } satisfies Response),
      );
    } else if (request.t === "credit") {
      socket.write(encodeFrame({ t: "ok", id: request.id } satisfies Response));
      socket.write(
        encodeFrame({ t: "gap", id: request.id, from: 7, to: 40 } satisfies Response),
      );
    }
  });
  const client = await Client.connect({
    transport: "unix",
    path: fixture.path,
    timeoutMs: 500,
  });
  try {
    const follower = await Follower.follow(client, { log: "state" });
    assert.equal(follower.version, 7);
    await assert.rejects(follower.finished, (error: unknown) => {
      assert.ok(error instanceof LugGapError);
      assert.equal(error.from, 7);
      assert.equal(error.to, 40);
      return true;
    });
  } finally {
    await client.close();
    await fixture.close();
  }
});

test("unix subscription grants one credit for each record pull", async () => {
  let credit = 0;
  let resolveCancel!: () => void;
  const cancelled = new Promise<void>((resolve) => {
    resolveCancel = resolve;
  });
  const fixture = await unixFixture((request, socket) => {
    if (request.t === "subscribe") {
      assert.equal(request.credit, 0);
      // The daemon opens a subscription with Ok and only then pushes.
      socket.write(encodeFrame({ t: "ok", id: request.id } satisfies Response));
      socket.write(
        encodeFrame({
          t: "view",
          id: request.id,
          version: 4,
          value: { count: 4 },
        } satisfies Response),
      );
    } else if (request.t === "credit") {
      credit += request.grant;
      socket.write(encodeFrame({ t: "ok", id: request.id } satisfies Response));
      socket.write(
        encodeFrame({
          t: "records",
          id: request.id,
          records: [{ version: 5, patch: { Update: { count: 5 } } }],
        } satisfies Response),
      );
    } else if (request.t === "cancel") {
      resolveCancel();
    }
  });
  const client = await Client.connect({
    transport: "unix",
    path: fixture.path,
    timeoutMs: 1_000,
  });
  try {
    const iterator = client
      .subscribe({ log: "state", mode: "reducible" })
      [Symbol.asyncIterator]();
    const view = await iterator.next();
    assert.equal(view.value?.t, "view");
    assert.equal(credit, 0);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(credit, 0);
    const records = await iterator.next();
    assert.equal(records.value?.t, "records");
    assert.equal(credit, 1);
    await iterator.return?.();
    await cancelled;
  } finally {
    await client.close();
    await fixture.close();
  }
});

test("follower takes the view preamble and applies streamed cavlc patches", async () => {
  const patches: JsonValue[] = [
    { Create: { profile: { name: "Gluck" }, count: 0 } },
    { Update: { count: 2, profile: { Update: { name: "Figaro" } } } },
    { Update: { profile: { Create: { city: "Atlanta" } } } },
    { Delete: ["profile"] },
  ];
  let nextPatch = 0;
  const fixture = await unixFixture((request, socket) => {
    if (request.t === "subscribe") {
      socket.write(encodeFrame({ t: "ok", id: request.id } satisfies Response));
      socket.write(
        encodeFrame({
          t: "view",
          id: request.id,
          version: 0,
          value: {},
        } satisfies Response),
      );
    } else if (request.t === "credit" && nextPatch < patches.length) {
      const index = nextPatch;
      nextPatch += 1;
      socket.write(encodeFrame({ t: "ok", id: request.id } satisfies Response));
      socket.write(
        encodeFrame({
          t: "records",
          id: request.id,
          records: [{ version: index + 1, patch: patches[index]! }],
        } satisfies Response),
      );
    }
  });
  const client = await Client.connect({
    transport: "unix",
    path: fixture.path,
    timeoutMs: 1_000,
  });
  let reachedFour!: () => void;
  const atFour = new Promise<void>((resolve) => {
    reachedFour = resolve;
  });
  const changes: number[] = [];
  try {
    const follower = await Follower.follow(client, {
      log: "state",
      onChange: (_value, version) => {
        changes.push(version);
        if (version === 4) {
          reachedFour();
        }
      },
    });
    await atFour;
    assert.equal(follower.version, 4);
    assert.deepEqual(follower.value, { count: 2 });
    assert.deepEqual(changes, [0, 1, 2, 3, 4]);
    const exposed = follower.value;
    exposed.count = 99;
    assert.deepEqual(follower.value, { count: 2 });
    await follower.close();
  } finally {
    await client.close();
    await fixture.close();
  }
});

test("HTTP calls use bare JSON and can run concurrently", async () => {
  const requests: Request[] = [];
  const { baseUrl, close } = await httpFixture(async (request, response) => {
    assert.equal(request.headers.authorization, "Bearer test-token");
    const body = await readBody(request);
    assert.equal(body[0], "{");
    const parsed = JSON.parse(body) as Request;
    requests.push(parsed);
    setTimeout(() => {
      json(response, { t: "pong", id: parsed.id });
    }, parsed.id % 2 === 0 ? 2 : 10);
  });
  const client = await Client.connect({
    transport: "http",
    baseUrl,
    token: "test-token",
    timeoutMs: 1_000,
  });
  try {
    const replies = await Promise.all(
      Array.from({ length: 20 }, () => client.call({ t: "ping" })),
    );
    assert.equal(requests.length, 20);
    assert.equal(new Set(replies.map((reply) => reply.id)).size, 20);
  } finally {
    await client.close();
    await close();
  }
});

test("HTTP subscription learns its session and sends credit only on pulls", async () => {
  let streamResponse: ServerResponse | undefined;
  let streamId = 0;
  const controls: Array<{ request: Request; session: string | undefined }> = [];
  const { baseUrl, close } = await httpFixture(async (request, response) => {
    const url = new URL(request.url ?? "", baseUrl);
    if (request.method === "GET") {
      assert.equal(url.pathname, "/v1/stream");
      assert.equal(url.searchParams.get("log"), "events");
      assert.equal(url.searchParams.get("from"), "3");
      assert.equal(url.searchParams.get("mode"), "records");
      assert.equal(url.searchParams.get("credit"), "0");
      streamId = Number(url.searchParams.get("id"));
      streamResponse = response;
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.write(": lug\n\n");
      const welcome = JSON.stringify({
        t: "welcome",
        id: streamId,
        version: 1,
        max_frame: 16 * 1024 * 1024,
        session: "issued-session",
      } satisfies Response);
      response.write(`data: ${welcome.slice(0, 12)}`);
      response.write(`${welcome.slice(12)}\n\n`);
      return;
    }

    const parsed = JSON.parse(await readBody(request)) as Request;
    controls.push({
      request: parsed,
      session: header(request.headers["x-lug-session"]),
    });
    if (parsed.t === "credit") {
      json(response, { t: "ok", id: parsed.id });
      streamResponse?.write(
        `data: ${JSON.stringify({
          t: "records",
          id: parsed.id,
          records: [{ version: 4, patch: "next" }],
        } satisfies Response)}\n\n`,
      );
    } else if (parsed.t === "cancel") {
      json(response, { t: "end", id: parsed.id });
      streamResponse?.end(
        `data: ${JSON.stringify({ t: "end", id: parsed.id } satisfies Response)}\n\n`,
      );
    }
  });
  const client = await Client.connect({
    transport: "http",
    baseUrl,
    token: "test-token",
    timeoutMs: 1_000,
  });
  try {
    const iterator = client
      .subscribe({ log: "events", from: 3 })
      [Symbol.asyncIterator]();
    // The Welcome opens the stream and carries the session. It is not data, so
    // the first thing a consumer can pull is the first record.
    const record = await iterator.next();
    assert.equal(record.value?.t, "records");
    assert.equal(controls[0]?.request.t, "credit");
    assert.equal(controls[0]?.session, "issued-session");
    await iterator.return?.();
    assert.equal(controls[1]?.request.t, "cancel");
    assert.equal(controls[1]?.session, "issued-session");
  } finally {
    await client.close();
    await close();
  }
});

async function httpFixture(
  handler: (request: IncomingMessage, response: ServerResponse) => void,
): Promise<{ baseUrl: string; close(): Promise<void> }> {
  const server = createHttpServer(handler);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("HTTP test server has no TCP address");
  }
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function json(response: ServerResponse, value: Response): void {
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}

function header(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const length = parts.reduce((total, part) => total + part.byteLength, 0);
  const joined = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    joined.set(part, offset);
    offset += part.byteLength;
  }
  return joined;
}
