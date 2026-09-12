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
