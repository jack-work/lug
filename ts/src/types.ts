export type JsonPrimitive = null | boolean | number | string;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type Id = number;
export type Version = number;

export type Mode = "records" | "reducible";
export type Durability = "memory" | "written" | "durable";

export type Request =
  | { t: "hello"; id: Id; version: number }
  | {
      t: "append";
      id: Id;
      log: string;
      patches: JsonValue[];
      durability: Durability;
    }
  | {
      t: "subscribe";
      id: Id;
      log: string;
      from: Version;
      mode: Mode;
      credit: number;
    }
  | { t: "credit"; id: Id; grant: number }
  | { t: "cancel"; id: Id }
  | { t: "read"; id: Id; log: string; at?: Version }
  | { t: "list"; id: Id }
  | { t: "create"; id: Id; log: string; reducible: boolean }
  | { t: "ping"; id: Id };

export interface Record {
  version: Version;
  patch: JsonValue;
}

export interface LogInfo {
  name: string;
  reducible: boolean;
  version: Version;
  oldest: Version;
  subscribers: number;
}

export type Code =
  | "rejected"
  | "no_such_log"
  | "log_exists"
  | "not_reducible"
  | "out_of_range"
  | "bad_id"
  | "unauthorized"
  | "malformed"
  | "storage"
  | "backpressure"
  | "internal";

export type Response =
  | {
      t: "welcome";
      id: Id;
      version: number;
      max_frame: number;
      session?: string;
    }
  | { t: "ack"; id: Id; versions: Version[]; synced: Version }
  | { t: "records"; id: Id; records: Record[] }
  | { t: "view"; id: Id; version: Version; value: JsonValue }
  | { t: "gap"; id: Id; from: Version; to: Version }
  | { t: "logs"; id: Id; logs: LogInfo[] }
  | { t: "end"; id: Id }
  | { t: "pong"; id: Id }
  | { t: "error"; id: Id; code: Code; message: string };

export type CallRequest =
  | { t: "append"; log: string; patches: JsonValue[]; durability?: Durability }
  | { t: "read"; log: string; at?: Version }
  | { t: "list" }
  | { t: "create"; log: string; reducible: boolean }
  | { t: "ping" };

export interface SubscriptionOptions {
  log: string;
  from?: Version;
  mode?: Mode;
}
