import { createConnection, type Socket } from "node:net";
import { encodeFrame, FrameDecoder, MAX_FRAME } from "./codec.js";
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
import type { Id, Request, Response } from "./types.js";
import { isResponse } from "./wire.js";

interface PendingCall {
  resolve: (response: Response) => void;
  reject: (error: unknown) => void;
  timer: NodeJS.Timeout;
}

interface WriteJob {
  frame: Uint8Array;
  resolve: () => void;
  reject: (error: unknown) => void;
}

interface StreamState {
  inbox: ResponseInbox;
}

export class UnixTransport implements Transport {
  readonly #socket: Socket;
  readonly #decoder = new FrameDecoder<Response>();
  readonly #pending = new Map<Id, PendingCall>();
  readonly #streams = new Map<Id, StreamState>();
  readonly #writes: WriteJob[] = [];
  #writing = false;
  #dead: LugConnectionError | undefined;
  #maxFrame = MAX_FRAME;

  private constructor(socket: Socket) {
    this.#socket = socket;
    socket.on("data", (chunk: Buffer) => {
      this.#receive(chunk);
    });
    socket.on("error", (error: Error) => {
      this.#fail(new LugConnectionError(`unix socket failed: ${error.message}`, { cause: error }));
    });
    socket.on("close", () => {
      this.#fail(new LugConnectionError("unix socket closed"));
    });
  }

  public static async connect(path: string, timeoutMs: number): Promise<UnixTransport> {
    const socket = createConnection(path);
    const transport = new UnixTransport(socket);
    await transport.#waitForConnect(timeoutMs);
    const welcome = await transport.call(
      { t: "hello", id: 1, version: 1 },
      timeoutMs,
    );
    if (welcome.t !== "welcome" || welcome.version !== 1) {
      await transport.close();
      throw new LugProtocolError(
        `expected protocol welcome version 1, received ${welcome.t}`,
      );
    }
    if (!isU32(welcome.max_frame) || welcome.max_frame === 0) {
      await transport.close();
      throw new LugProtocolError(`invalid server max_frame ${welcome.max_frame}`);
    }
    transport.#maxFrame = Math.min(welcome.max_frame, MAX_FRAME);
    return transport;
  }

  public call(request: Request, timeoutMs: number): Promise<Response> {
    if (this.#dead !== undefined) {
      return Promise.reject(this.#dead);
    }
    if (this.#pending.has(request.id) || this.#streams.has(request.id)) {
      return Promise.reject(new LugProtocolError(`id ${request.id} is already in flight`));
    }

    return new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(request.id);
        reject(new LugTimeoutError(`request ${request.id}`, timeoutMs));
      }, timeoutMs);
      this.#pending.set(request.id, { resolve, reject, timer });
      void this.#send(request).catch((error: unknown) => {
        const pending = this.#pending.get(request.id);
        if (pending !== undefined) {
          clearTimeout(pending.timer);
          this.#pending.delete(request.id);
          pending.reject(error);
        }
      });
    });
  }

  public subscribe(
    request: Extract<Request, { t: "subscribe" }>,
    timeoutMs: number,
  ): AsyncIterable<Response> {
    if (this.#pending.has(request.id) || this.#streams.has(request.id)) {
      throw new LugProtocolError(`id ${request.id} is already in flight`);
    }
    const inbox = new ResponseInbox();
    const state: StreamState = { inbox };
    const driver: SubscriptionDriver = {
      start: async () => {
        if (this.#dead !== undefined) {
          throw this.#dead;
        }
        this.#streams.set(request.id, state);
        try {
          await this.#sendWithin(request, timeoutMs, `subscribe ${request.id}`);
        } catch (error) {
          this.#streams.delete(request.id);
          inbox.fail(error);
          throw error;
        }
      },
      grant: async () => {
        await this.#sendWithin(
          { t: "credit", id: request.id, grant: 1 },
          timeoutMs,
          `credit ${request.id}`,
        );
      },
      cancel: async () => {
        this.#streams.delete(request.id);
        if (this.#dead === undefined) {
          await this.#sendWithin(
            { t: "cancel", id: request.id },
            timeoutMs,
            `cancel ${request.id}`,
          );
        }
      },
    };
    return new ControlledSubscription(
      inbox,
      request.mode === "reducible" ? 1 : 0,
      driver,
    );
  }

  public async close(): Promise<void> {
    if (this.#dead === undefined) {
      this.#fail(new LugConnectionError("unix client closed"));
    }
    if (!this.#socket.destroyed) {
      this.#socket.destroy();
    }
  }

  async #waitForConnect(timeoutMs: number): Promise<void> {
    if (this.#socket.readyState === "open") {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        this.#socket.destroy();
        reject(new LugTimeoutError("unix connect", timeoutMs));
      }, timeoutMs);
      const onConnect = (): void => {
        cleanup();
        resolve();
      };
      const onError = (error: Error): void => {
        cleanup();
        reject(new LugConnectionError(`unix connect failed: ${error.message}`, { cause: error }));
      };
      const cleanup = (): void => {
        clearTimeout(timer);
        this.#socket.off("connect", onConnect);
        this.#socket.off("error", onError);
      };
      this.#socket.once("connect", onConnect);
      this.#socket.once("error", onError);
    });
  }

  #receive(chunk: Uint8Array): void {
    if (this.#dead !== undefined) {
      return;
    }
    let responses: Response[];
    try {
      responses = this.#decoder.push(chunk);
    } catch (error) {
      const failure = new LugProtocolError("invalid frame from unix server", { cause: error });
      this.#socket.destroy();
      this.#fail(failure);
      return;
    }

    for (const response of responses) {
      if (!isResponse(response)) {
        this.#socket.destroy();
        this.#fail(new LugProtocolError("unix server sent a response without a valid id or tag"));
        return;
      }
      const stream = this.#streams.get(response.id);
      if (stream !== undefined) {
        stream.inbox.push(response);
        if (response.t === "end" || response.t === "error") {
          this.#streams.delete(response.id);
        }
        continue;
      }
      const pending = this.#pending.get(response.id);
      if (pending !== undefined) {
        clearTimeout(pending.timer);
        this.#pending.delete(response.id);
        pending.resolve(response);
      }
    }
  }

  #send(request: Request): Promise<void> {
    if (this.#dead !== undefined) {
      return Promise.reject(this.#dead);
    }
    let frame: Uint8Array;
    try {
      frame = encodeFrame(request, this.#maxFrame);
    } catch (error) {
      return Promise.reject(error);
    }
    return new Promise((resolve, reject) => {
      this.#writes.push({ frame, resolve, reject });
      this.#pumpWrites();
    });
  }

  async #sendWithin(request: Request, timeoutMs: number, operation: string): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        this.#send(request),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => {
            const error = new LugTimeoutError(operation, timeoutMs);
            this.#socket.destroy();
            this.#fail(new LugConnectionError(error.message, { cause: error }));
            reject(error);
          }, timeoutMs);
        }),
      ]);
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    }
  }

  #pumpWrites(): void {
    if (this.#writing || this.#dead !== undefined) {
      return;
    }
    const job = this.#writes.shift();
    if (job === undefined) {
      return;
    }
    this.#writing = true;
    this.#socket.write(job.frame, (error?: Error | null) => {
      this.#writing = false;
      if (error !== undefined && error !== null) {
        const failure = new LugConnectionError(`unix write failed: ${error.message}`, {
          cause: error,
        });
        job.reject(failure);
        this.#fail(failure);
      } else {
        job.resolve();
      }
      this.#pumpWrites();
    });
  }

  #fail(error: LugConnectionError | LugProtocolError): void {
    if (this.#dead !== undefined) {
      return;
    }
    this.#dead =
      error instanceof LugConnectionError
        ? error
        : new LugConnectionError(error.message, { cause: error });
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(this.#dead);
    }
    this.#pending.clear();
    for (const stream of this.#streams.values()) {
      stream.inbox.fail(this.#dead);
    }
    this.#streams.clear();
    for (const job of this.#writes.splice(0)) {
      job.reject(this.#dead);
    }
  }
}

function isU32(value: number): boolean {
  return Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff;
}

