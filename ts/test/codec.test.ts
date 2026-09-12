import assert from "node:assert/strict";
import { test } from "node:test";
import {
  FrameDecoder,
  FrameError,
  MAX_FRAME,
  encodeFrame,
} from "../src/codec.js";
import type { Request, Response } from "../src/types.js";

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(
    parts.reduce((length, part) => length + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}

test("frame uses a big-endian length and round trips JSON", () => {
  const request: Request = { t: "ping", id: 7 };
  const frame = encodeFrame(request);
  assert.equal(new DataView(frame.buffer).getUint32(0, false), frame.length - 4);
  assert.deepEqual(new FrameDecoder<Request>().push(frame), [request]);
});

test("decoder accepts every two-chunk split", () => {
  const response: Response = {
    t: "view",
    id: 31,
    version: 9,
    value: { text: "héllo", lines: ["one", "two"] },
  };
  const frame = encodeFrame(response);

  for (let split = 0; split <= frame.length; split += 1) {
    const decoder = new FrameDecoder<Response>();
    const before = decoder.push(frame.subarray(0, split));
    const after = decoder.push(frame.subarray(split));
    assert.deepEqual([...before, ...after], [response]);
    decoder.finish();
  }
});

test("decoder accepts a frame one byte at a time", () => {
  const response: Response = { t: "pong", id: 1 };
  const frame = encodeFrame(response);
  const decoder = new FrameDecoder<Response>();
  const received: Response[] = [];
  for (const byte of frame) {
    received.push(...decoder.push(Uint8Array.of(byte)));
  }
  assert.deepEqual(received, [response]);
});

test("decoder drains several frames from one chunk", () => {
  const responses: Response[] = Array.from({ length: 200 }, (_, index) => ({
    t: "pong" as const,
    id: index + 1,
  }));
  const wire = concat(...responses.map((response) => encodeFrame(response)));
  assert.deepEqual(new FrameDecoder<Response>().push(wire), responses);
});

test("decoder preserves a partial tail after complete frames", () => {
  const first: Response = { t: "pong", id: 1 };
  const second: Response = { t: "end", id: 2 };
  const secondFrame = encodeFrame(second);
  const decoder = new FrameDecoder<Response>();

  assert.deepEqual(
    decoder.push(concat(encodeFrame(first), secondFrame.subarray(0, 6))),
    [first],
  );
  assert.deepEqual(decoder.push(secondFrame.subarray(6)), [second]);
  decoder.finish();
});

test("oversized prefix fails before a body arrives", () => {
  const prefix = new Uint8Array(4);
  new DataView(prefix.buffer).setUint32(0, MAX_FRAME + 1, false);
  assert.throws(
    () => new FrameDecoder<Response>().push(prefix),
    (error: unknown) =>
      error instanceof FrameError && error.message.includes(String(MAX_FRAME + 1)),
  );
});

test("finish rejects a truncated frame", () => {
  const decoder = new FrameDecoder<Response>();
  decoder.push(encodeFrame({ t: "pong", id: 4 }).subarray(0, 5));
  assert.throws(() => decoder.finish(), /incomplete frame/);
});
