import { LugProtocolError } from "./errors.js";
import { HttpTransport } from "./http.js";
import type { Transport } from "./transport.js";
import type {
  CallRequest,
  Id,
  Request,
  Response,
  SubscriptionOptions,
} from "./types.js";
import { UnixTransport } from "./unix.js";

export interface UnixClientOptions {
  transport: "unix";
  path: string;
  timeoutMs: number;
}

export interface HttpClientOptions {
  transport: "http";
  baseUrl: string;
  token: string;
  timeoutMs: number;
}

export type ClientOptions = UnixClientOptions | HttpClientOptions;

export class Client {
  readonly #transport: Transport;
  readonly #timeoutMs: number;
  #nextId: Id;
  #closed = false;

  private constructor(transport: Transport, timeoutMs: number, firstId: Id) {
    this.#transport = transport;
    this.#timeoutMs = timeoutMs;
    this.#nextId = firstId;
  }

  public static async connect(options: ClientOptions): Promise<Client> {
    checkTimeout(options.timeoutMs);
    if (options.transport === "unix") {
      if (options.path.length === 0) {
        throw new TypeError("unix socket path must not be empty");
      }
      const transport = await UnixTransport.connect(options.path, options.timeoutMs);
      return new Client(transport, options.timeoutMs, 2);
    }
    const transport = new HttpTransport(options.baseUrl, options.token);
    return new Client(transport, options.timeoutMs, 1);
  }

  public call(request: CallRequest, timeoutMs = this.#timeoutMs): Promise<Response> {
    this.#ensureOpen();
    checkTimeout(timeoutMs);
    if (request.t === "read" && request.at !== undefined) {
      checkVersion(request.at, "at");
    }
    const id = this.#allocateId();
    return this.#transport.call(withId(request, id), timeoutMs);
  }

  public subscribe(options: SubscriptionOptions): AsyncIterable<Response> {
    this.#ensureOpen();
    if (options.from !== undefined) {
      checkVersion(options.from, "from");
    }
    const request: Extract<Request, { t: "subscribe" }> = {
      t: "subscribe",
      id: this.#allocateId(),
      log: options.log,
      from: options.from ?? 0,
      mode: options.mode ?? "records",
      credit: 0,
    };
    return this.#transport.subscribe(request, this.#timeoutMs);
  }

  public async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    await this.#transport.close();
  }

  #ensureOpen(): void {
    if (this.#closed) {
      throw new LugProtocolError("client is closed");
    }
  }

  #allocateId(): Id {
    if (!Number.isSafeInteger(this.#nextId)) {
      throw new LugProtocolError("client exhausted safe integer request ids");
    }
    const id = this.#nextId;
    this.#nextId += 1;
    return id;
  }
}

function withId(request: CallRequest, id: Id): Request {
  switch (request.t) {
    case "append":
      return {
        t: "append",
        id,
        log: request.log,
        patches: request.patches,
        durability: request.durability ?? "written",
      };
    case "read":
      return request.at === undefined
        ? { t: "read", id, log: request.log }
        : { t: "read", id, log: request.log, at: request.at };
    case "list":
      return { t: "list", id };
    case "create":
      return {
        t: "create",
        id,
        log: request.log,
        reducible: request.reducible,
      };
    case "ping":
      return { t: "ping", id };
    default:
      return assertNever(request);
  }
}

function checkTimeout(timeoutMs: number): void {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new TypeError(`timeoutMs must be positive, received ${timeoutMs}`);
  }
}

// A version is a u64 on the wire and a double here. Sending one the client
// cannot hold exactly would ask the server about a version nobody named, so
// it is refused where the caller can still see it.
function checkVersion(version: number, field: string): void {
  if (!Number.isSafeInteger(version) || version < 0) {
    throw new TypeError(
      `${field} must be a version below Number.MAX_SAFE_INTEGER, received ${version}`,
    );
  }
}

function assertNever(value: never): never {
  throw new LugProtocolError(`unknown request: ${JSON.stringify(value)}`);
}
