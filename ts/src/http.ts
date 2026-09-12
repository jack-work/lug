import { encodeFrame, MAX_FRAME } from "./codec.js";
import {
  LugConnectionError,
  LugProtocolError,
  LugTimeoutError,
} from "./errors.js";
import {
  ControlledSubscription,
  ResponseInbox,
  type SubscriptionDriver,
} from "./subscription.js";
import type { Transport } from "./transport.js";
import type { Request, Response } from "./types.js";
import { isResponse } from "./wire.js";

const CALL_PATH = "/v1/call";
const STREAM_PATH = "/v1/stream";
const SESSION_HEADER = "x-lug-session";

interface HttpStream {
  controller: AbortController;
  inbox: ResponseInbox;
  session: Promise<string>;
  resolveSession: (session: string) => void;
  rejectSession: (error: unknown) => void;
  sessionValue?: string;
}

export class HttpTransport implements Transport {
  readonly #baseUrl: URL;
  readonly #token: string;
  readonly #controllers = new Set<AbortController>();
  readonly #streams = new Map<number, HttpStream>();
  #closed = false;

  public constructor(baseUrl: string, token: string) {
    this.#baseUrl = new URL(baseUrl);
    this.#token = token;
  }

  public async call(request: Request, timeoutMs: number): Promise<Response> {
    const response = await this.#post(request, timeoutMs);
    if (response === undefined) {
      throw new LugProtocolError("HTTP call returned an empty body");
    }
    return response;
  }

  public subscribe(
    request: Extract<Request, { t: "subscribe" }>,
    timeoutMs: number,
  ): AsyncIterable<Response> {
    if (this.#closed) {
      throw new LugConnectionError("HTTP client is closed");
    }
    if (this.#streams.has(request.id)) {
      throw new LugProtocolError(`id ${request.id} is already in flight`);
    }

    const inbox = new ResponseInbox();
    const controller = new AbortController();
    const deferred = makeDeferred<string>();
    const stream: HttpStream = {
      controller,
      inbox,
      session: deferred.promise,
      resolveSession: deferred.resolve,
      rejectSession: deferred.reject,
    };
    const driver: SubscriptionDriver = {
      start: async () => {
        this.#streams.set(request.id, stream);
        try {
          await this.#startStream(request, stream, timeoutMs);
        } catch (error) {
          this.#streams.delete(request.id);
          stream.rejectSession(error);
          inbox.fail(error);
          throw error;
        }
      },
      grant: async () => {
        const session = await withTimeout(
          stream.session,
          timeoutMs,
          `stream ${request.id} welcome`,
        );
        const response = await this.#post(
          { t: "credit", id: request.id, grant: 1 },
          timeoutMs,
          session,
          true,
        );
        if (response?.t === "error") {
          this.#acceptStreamResponse(request.id, stream, response);
        }
      },
      cancel: async () => {
        this.#streams.delete(request.id);
        try {
          if (!this.#closed && stream.sessionValue !== undefined) {
            await this.#post(
              { t: "cancel", id: request.id },
              timeoutMs,
              stream.sessionValue,
              true,
            );
          }
        } finally {
          controller.abort();
          this.#controllers.delete(controller);
        }
      },
    };
    const preambles = request.mode === "reducible" ? 2 : 1;
    return new ControlledSubscription(inbox, preambles, driver);
  }

  public async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    const error = new LugConnectionError("HTTP client closed");
    for (const stream of this.#streams.values()) {
      stream.rejectSession(error);
      stream.inbox.fail(error);
    }
    this.#streams.clear();
    for (const controller of this.#controllers) {
      controller.abort(error);
    }
    this.#controllers.clear();
  }

  async #post(
    request: Request,
    timeoutMs: number,
    session?: string,
    allowEmpty = false,
  ): Promise<Response | undefined> {
    if (this.#closed) {
      throw new LugConnectionError("HTTP client is closed");
    }
    const controller = new AbortController();
    this.#controllers.add(controller);
    const timer = setTimeout(() => {
      controller.abort(new LugTimeoutError(`request ${request.id}`, timeoutMs));
    }, timeoutMs);
    try {
      const headers: Record<string, string> = {
        authorization: `Bearer ${this.#token}`,
        "content-type": "application/json",
      };
      if (session !== undefined) {
        headers[SESSION_HEADER] = session;
      }
      const response = await fetch(new URL(CALL_PATH, this.#baseUrl), {
        method: "POST",
        headers,
        body: encodeJson(request),
        signal: controller.signal,
      });
      const bytes = await readLimited(response.body);
      if (allowEmpty && bytes.byteLength === 0 && response.ok) {
        return undefined;
      }
      const decoded = decodeJsonResponse(bytes);
      if (!response.ok && decoded.t !== "error") {
        throw new LugConnectionError(
          `HTTP call failed with status ${response.status}`,
        );
      }
      return decoded;
    } catch (error) {
      if (controller.signal.reason instanceof LugTimeoutError) {
        throw controller.signal.reason;
      }
      if (error instanceof LugProtocolError || error instanceof LugConnectionError) {
        throw error;
      }
      throw new LugConnectionError(`HTTP call failed: ${errorMessage(error)}`, {
        cause: error,
      });
    } finally {
      clearTimeout(timer);
      this.#controllers.delete(controller);
    }
  }

  async #startStream(
    request: Extract<Request, { t: "subscribe" }>,
    stream: HttpStream,
    timeoutMs: number,
  ): Promise<void> {
    const url = new URL(STREAM_PATH, this.#baseUrl);
    url.searchParams.set("id", String(request.id));
    url.searchParams.set("log", request.log);
    url.searchParams.set("from", String(request.from));
    url.searchParams.set("mode", request.mode);
    url.searchParams.set("credit", String(request.credit));
    this.#controllers.add(stream.controller);
    const timer = setTimeout(() => {
      stream.controller.abort(new LugTimeoutError(`subscribe ${request.id}`, timeoutMs));
    }, timeoutMs);

    let response: globalThis.Response;
    try {
      response = await fetch(url, {
        headers: {
          accept: "text/event-stream",
          authorization: `Bearer ${this.#token}`,
        },
        signal: stream.controller.signal,
      });
    } catch (error) {
      clearTimeout(timer);
      this.#controllers.delete(stream.controller);
      if (stream.controller.signal.reason instanceof LugTimeoutError) {
        throw stream.controller.signal.reason;
      }
      throw new LugConnectionError(`HTTP stream failed: ${errorMessage(error)}`, {
        cause: error,
      });
    }
    clearTimeout(timer);

    if (!response.ok || response.body === null) {
      this.#controllers.delete(stream.controller);
      stream.controller.abort();
      throw new LugConnectionError(
        `HTTP stream failed with status ${response.status}`,
      );
    }
    void this.#consumeStream(request.id, stream, response.body);
  }

  async #consumeStream(
    id: number,
    stream: HttpStream,
    body: ReadableStream<Uint8Array>,
  ): Promise<void> {
    const parser = new SseParser();
    const reader = body.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          for (const response of parser.finish()) {
            this.#acceptStreamResponse(id, stream, response);
          }
          if (this.#streams.has(id)) {
            throw new LugConnectionError(`HTTP stream ${id} ended without an end frame`);
          }
          return;
        }
        for (const response of parser.push(value)) {
          this.#acceptStreamResponse(id, stream, response);
        }
      }
    } catch (error) {
      if (this.#streams.has(id) && !stream.controller.signal.aborted) {
        const failure =
          error instanceof LugConnectionError || error instanceof LugProtocolError
            ? error
            : new LugConnectionError(`HTTP stream ${id} failed: ${errorMessage(error)}`, {
                cause: error,
              });
        stream.rejectSession(failure);
        stream.inbox.fail(failure);
        this.#streams.delete(id);
      }
    } finally {
      this.#controllers.delete(stream.controller);
      reader.releaseLock();
    }
  }

  #acceptStreamResponse(id: number, stream: HttpStream, response: Response): void {
    if (response.id !== id) {
      const error = new LugProtocolError(
        `HTTP stream ${id} received response for id ${response.id}`,
      );
      stream.rejectSession(error);
      stream.inbox.fail(error);
      this.#streams.delete(id);
      stream.controller.abort(error);
      return;
    }
    if (response.t === "welcome") {
      if (response.version !== 1 || response.session === undefined || response.session === "") {
        const error = new LugProtocolError(`HTTP stream ${id} sent an invalid welcome`);
        stream.rejectSession(error);
        stream.inbox.fail(error);
        this.#streams.delete(id);
        stream.controller.abort(error);
        return;
      }
      stream.sessionValue = response.session;
      stream.resolveSession(response.session);
    }
    stream.inbox.push(response);
    if (response.t === "end" || response.t === "error") {
      this.#streams.delete(id);
      stream.controller.abort();
    }
  }
}

class SseParser {
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });
  #text = "";
  #data: string[] = [];

  public push(chunk: Uint8Array): Response[] {
    this.#text += this.#decoder.decode(chunk, { stream: true });
    if (this.#text.length > MAX_FRAME * 2) {
      throw new LugProtocolError("SSE event exceeds the frame limit");
    }
    return this.#lines(false);
  }

  public finish(): Response[] {
    this.#text += this.#decoder.decode();
    const responses = this.#lines(true);
    if (this.#data.length !== 0) {
      responses.push(this.#event());
    }
    return responses;
  }

  #lines(final: boolean): Response[] {
    const responses: Response[] = [];
    for (;;) {
      const newline = this.#text.indexOf("\n");
      if (newline === -1) {
        if (final && this.#text.length !== 0) {
          this.#line(this.#text, responses);
          this.#text = "";
        }
        return responses;
      }
      let line = this.#text.slice(0, newline);
      this.#text = this.#text.slice(newline + 1);
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      this.#line(line, responses);
    }
  }

  #line(line: string, responses: Response[]): void {
    if (line === "") {
      if (this.#data.length !== 0) {
        responses.push(this.#event());
      }
      return;
    }
    if (line.startsWith(":")) {
      return;
    }
    if (line === "data" || line.startsWith("data:")) {
      let value = line === "data" ? "" : line.slice(5);
      if (value.startsWith(" ")) {
        value = value.slice(1);
      }
      this.#data.push(value);
    }
  }

  #event(): Response {
    const data = this.#data.join("\n");
    this.#data = [];
    return decodeJsonResponse(new TextEncoder().encode(data));
  }
}

function encodeJson(value: unknown): string {
  const frame = encodeFrame(value);
  return new TextDecoder().decode(frame.subarray(4));
}

function decodeJsonResponse(bytes: Uint8Array): Response {
  if (bytes.byteLength > MAX_FRAME) {
    throw new LugProtocolError(`JSON response exceeds the ${MAX_FRAME} byte limit`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch (error) {
    throw new LugProtocolError(`malformed JSON response: ${errorMessage(error)}`, {
      cause: error,
    });
  }
  if (!isResponse(parsed)) {
    throw new LugProtocolError("HTTP body is not a lug response");
  }
  return parsed;
}

async function readLimited(
  body: ReadableStream<Uint8Array> | null,
): Promise<Uint8Array> {
  if (body === null) {
    return new Uint8Array(0);
  }
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      length += value.byteLength;
      if (length > MAX_FRAME) {
        throw new LugProtocolError(`JSON response exceeds the ${MAX_FRAME} byte limit`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function makeDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  void promise.catch(() => undefined);
  return { promise, resolve, reject };
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  operation: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new LugTimeoutError(operation, timeoutMs)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
