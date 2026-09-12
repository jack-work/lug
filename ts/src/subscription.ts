import type { Response } from "./types.js";

interface WaitingPull {
  resolve: (result: IteratorResult<Response>) => void;
  reject: (error: unknown) => void;
}

export class ResponseInbox {
  readonly #items: Response[] = [];
  readonly #waiting: WaitingPull[] = [];
  #ended = false;
  #error: unknown;

  public push(response: Response): void {
    if (this.#ended || this.#error !== undefined) {
      return;
    }
    const waiter = this.#waiting.shift();
    if (waiter === undefined) {
      this.#items.push(response);
    } else {
      waiter.resolve({ done: false, value: response });
    }
    if (response.t === "end" || response.t === "error") {
      this.#ended = true;
      this.#drainEnd();
    }
  }

  public canPullWithoutCredit(): boolean {
    return (
      this.#items.length !== 0 ||
      this.#ended ||
      this.#error !== undefined
    );
  }

  public pull(): Promise<IteratorResult<Response>> {
    const item = this.#items.shift();
    if (item !== undefined) {
      return Promise.resolve({ done: false, value: item });
    }
    if (this.#error !== undefined) {
      return Promise.reject(this.#error);
    }
    if (this.#ended) {
      return Promise.resolve({ done: true, value: undefined });
    }
    return new Promise((resolve, reject) => {
      this.#waiting.push({ resolve, reject });
    });
  }

  public fail(error: unknown): void {
    if (this.#ended || this.#error !== undefined) {
      return;
    }
    this.#error = error;
    this.#items.length = 0;
    for (const waiter of this.#waiting.splice(0)) {
      waiter.reject(error);
    }
  }

  public stop(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#items.length = 0;
    this.#drainEnd();
  }

  #drainEnd(): void {
    if (this.#items.length !== 0) {
      return;
    }
    for (const waiter of this.#waiting.splice(0)) {
      waiter.resolve({ done: true, value: undefined });
    }
  }
}

export interface SubscriptionDriver {
  start(): Promise<void>;
  grant(): Promise<void>;
  cancel(): Promise<void>;
}

export class ControlledSubscription implements AsyncIterableIterator<Response> {
  readonly #inbox: ResponseInbox;
  readonly #driver: SubscriptionDriver;
  #uncreditedPulls: number;
  #start: Promise<void> | undefined;
  #returned = false;

  public constructor(
    inbox: ResponseInbox,
    uncreditedPulls: number,
    driver: SubscriptionDriver,
  ) {
    this.#inbox = inbox;
    this.#uncreditedPulls = uncreditedPulls;
    this.#driver = driver;
  }

  public [Symbol.asyncIterator](): AsyncIterableIterator<Response> {
    return this;
  }

  public async next(): Promise<IteratorResult<Response>> {
    if (this.#returned) {
      return { done: true, value: undefined };
    }
    await this.#ensureStarted();
    if (this.#uncreditedPulls !== 0) {
      this.#uncreditedPulls -= 1;
    } else if (!this.#inbox.canPullWithoutCredit()) {
      try {
        await this.#driver.grant();
      } catch (error) {
        this.#inbox.fail(error);
        throw error;
      }
    }
    return this.#inbox.pull();
  }

  public async return(): Promise<IteratorResult<Response>> {
    if (this.#returned) {
      return { done: true, value: undefined };
    }
    this.#returned = true;
    this.#inbox.stop();
    if (this.#start !== undefined) {
      await this.#driver.cancel();
    }
    return { done: true, value: undefined };
  }

  public async throw(error?: unknown): Promise<IteratorResult<Response>> {
    this.#inbox.fail(error);
    await this.return();
    throw error;
  }

  #ensureStarted(): Promise<void> {
    this.#start ??= this.#driver.start();
    return this.#start;
  }
}
