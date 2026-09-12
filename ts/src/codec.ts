export const MAX_FRAME = 16 * 1024 * 1024;
const HEADER_SIZE = 4;

export class FrameError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = "FrameError";
  }
}

export function encodeFrame(value: unknown, maxFrame = MAX_FRAME): Uint8Array {
  let json: string | undefined;
  try {
    json = JSON.stringify(value);
  } catch (error) {
    throw new FrameError(`cannot encode frame: ${errorMessage(error)}`);
  }
  if (json === undefined) {
    throw new FrameError("cannot encode frame: value is not JSON serializable");
  }

  const body = new TextEncoder().encode(json);
  if (body.byteLength > maxFrame) {
    throw new FrameError(
      `frame of ${body.byteLength} bytes exceeds the ${maxFrame} byte limit`,
    );
  }

  const frame = new Uint8Array(HEADER_SIZE + body.byteLength);
  new DataView(frame.buffer).setUint32(0, body.byteLength, false);
  frame.set(body, HEADER_SIZE);
  return frame;
}

export class FrameDecoder<T> {
  readonly #text = new TextDecoder("utf-8", { fatal: true });
  #buffer = new Uint8Array(0);
  #start = 0;
  #end = 0;

  public push(chunk: Uint8Array): T[] {
    this.#append(chunk);
    const frames: T[] = [];

    while (this.#end - this.#start >= HEADER_SIZE) {
      const view = new DataView(
        this.#buffer.buffer,
        this.#buffer.byteOffset + this.#start,
        HEADER_SIZE,
      );
      const length = view.getUint32(0, false);
      if (length > MAX_FRAME) {
        throw new FrameError(
          `frame of ${length} bytes exceeds the ${MAX_FRAME} byte limit`,
        );
      }
      if (this.#end - this.#start - HEADER_SIZE < length) {
        break;
      }

      const bodyStart = this.#start + HEADER_SIZE;
      const body = this.#buffer.subarray(bodyStart, bodyStart + length);
      try {
        frames.push(JSON.parse(this.#text.decode(body)) as T);
      } catch (error) {
        throw new FrameError(`malformed frame: ${errorMessage(error)}`);
      }
      this.#start = bodyStart + length;
    }

    if (this.#start === this.#end) {
      this.#start = 0;
      this.#end = 0;
    }
    return frames;
  }

  public finish(): void {
    const trailing = this.#end - this.#start;
    if (trailing !== 0) {
      throw new FrameError(`incomplete frame: ${trailing} trailing bytes`);
    }
  }

  #append(chunk: Uint8Array): void {
    if (chunk.byteLength === 0) {
      return;
    }
    const used = this.#end - this.#start;
    const needed = used + chunk.byteLength;
    if (needed > this.#buffer.byteLength) {
      let capacity = Math.max(64, this.#buffer.byteLength);
      while (capacity < needed) {
        capacity *= 2;
      }
      const grown = new Uint8Array(capacity);
      grown.set(this.#buffer.subarray(this.#start, this.#end));
      this.#buffer = grown;
      this.#start = 0;
      this.#end = used;
    } else if (this.#end + chunk.byteLength > this.#buffer.byteLength) {
      this.#buffer.copyWithin(0, this.#start, this.#end);
      this.#start = 0;
      this.#end = used;
    }
    this.#buffer.set(chunk, this.#end);
    this.#end += chunk.byteLength;
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
