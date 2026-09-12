import type { Request, Response } from "./types.js";

export interface Transport {
  call(request: Request, timeoutMs: number): Promise<Response>;
  subscribe(
    request: Extract<Request, { t: "subscribe" }>,
    timeoutMs: number,
  ): AsyncIterable<Response>;
  close(): Promise<void>;
}
