export class LugClientError extends Error {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LugClientError";
  }
}

export class LugTimeoutError extends LugClientError {
  public readonly timeoutMs: number;

  public constructor(operation: string, timeoutMs: number) {
    super(`${operation} timed out after ${timeoutMs}ms`);
    this.name = "LugTimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

export class LugConnectionError extends LugClientError {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LugConnectionError";
  }
}

export class LugProtocolError extends LugClientError {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LugProtocolError";
  }
}

// Versions in `(from, to]` were reclaimed before the follower reached them.
export class LugGapError extends LugProtocolError {
  public readonly from: number;
  public readonly to: number;

  public constructor(from: number, to: number) {
    super(`versions (${from}, ${to}] were reclaimed before this follower read them`);
    this.name = "LugGapError";
    this.from = from;
    this.to = to;
  }
}
