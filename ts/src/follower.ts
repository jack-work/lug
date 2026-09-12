import { applyCavlcPatch, type CavlcPatch, type JsonObject } from "./cavlc.js";
import type { Client } from "./client.js";
import { LugGapError, LugProtocolError } from "./errors.js";
import type { JsonValue, Response, Version } from "./types.js";

export interface FollowerOptions {
  log: string;
  from?: Version;
  onChange?: (value: JsonObject, version: Version) => void;
}

export class Follower {
  readonly #iterator: AsyncIterator<Response>;
  #value: JsonObject;
  #version: Version;
  #finished: Promise<void>;
  public onChange: ((value: JsonObject, version: Version) => void) | undefined;

  private constructor(
    iterator: AsyncIterator<Response>,
    value: JsonObject,
    version: Version,
    onChange: ((value: JsonObject, version: Version) => void) | undefined,
  ) {
    this.#iterator = iterator;
    this.#value = value;
    this.#version = version;
    this.onChange = onChange;
    this.#finished = Promise.resolve();
  }

  public static async follow(
    client: Client,
    options: FollowerOptions,
  ): Promise<Follower> {
    const subscription =
      options.from === undefined
        ? { log: options.log, mode: "reducible" as const }
        : { log: options.log, from: options.from, mode: "reducible" as const };
    const iterator = client.subscribe(subscription)[Symbol.asyncIterator]();
    try {
      const next = await iterator.next();
      if (next.done) {
        throw new LugProtocolError("reducible subscription ended before its view");
      }
      // The server acknowledges a subscription before it pushes anything, and
      // the transports absorb that acknowledgement, so a reducible stream
      // opens on its view and nothing else.
      const response = next.value;
      if (response.t === "error") {
        throw serverError(response);
      }
      if (response.t !== "view" || !isObject(response.value)) {
        throw new LugProtocolError(
          `expected a reducible view preamble, received ${response.t}`,
        );
      }
      const follower = new Follower(
        iterator,
        cloneObject(response.value),
        response.version,
        options.onChange,
      );
      follower.#notify();
      follower.#finished = follower.#run();
      void follower.#finished.catch(() => undefined);
      return follower;
    } catch (error) {
      await iterator.return?.();
      throw error;
    }
  }

  public get value(): JsonObject {
    return cloneObject(this.#value);
  }

  public get version(): Version {
    return this.#version;
  }

  public get finished(): Promise<void> {
    return this.#finished;
  }

  public async close(): Promise<void> {
    await this.#iterator.return?.();
    await this.#finished;
  }

  async #run(): Promise<void> {
    try {
      for (;;) {
        const next = await this.#iterator.next();
        if (next.done) {
          return;
        }
        const response = next.value;
        switch (response.t) {
          case "records":
            for (const record of response.records) {
              if (record.version !== this.#version + 1) {
                throw new LugProtocolError(
                  `record version ${record.version} followed ${this.#version}`,
                );
              }
              if (!isObject(record.patch)) {
                throw new LugProtocolError(
                  `record ${record.version} does not contain a cavlc patch`,
                );
              }
              this.#value = applyCavlcPatch(
                this.#value,
                record.patch as CavlcPatch,
              );
              this.#version = record.version;
              this.#notify();
            }
            break;
          case "gap":
            throw new LugGapError(response.from, response.to);
          case "error":
            throw serverError(response);
          case "end":
            return;
          default:
            throw new LugProtocolError(
              `unexpected ${response.t} response on a reducible stream`,
            );
        }
      }
    } catch (error) {
      await this.#iterator.return?.();
      throw error;
    }
  }

  #notify(): void {
    this.onChange?.(cloneObject(this.#value), this.#version);
  }
}

function serverError(response: Extract<Response, { t: "error" }>): LugProtocolError {
  return new LugProtocolError(`${response.code}: ${response.message}`);
}

function cloneObject(value: JsonObject): JsonObject {
  return JSON.parse(JSON.stringify(value)) as JsonObject;
}

function isObject(value: JsonValue): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
