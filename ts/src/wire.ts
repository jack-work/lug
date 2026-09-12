import type {
  Code,
  LogInfo,
  Record as LugRecord,
  Response,
} from "./types.js";

const CODES: ReadonlySet<string> = new Set<Code>([
  "rejected",
  "no_such_log",
  "log_exists",
  "not_reducible",
  "out_of_range",
  "bad_id",
  "unauthorized",
  "malformed",
  "storage",
  "backpressure",
  "internal",
]);

export function isResponse(value: unknown): value is Response {
  if (!isObject(value) || !isId(value.id) || typeof value.t !== "string") {
    return false;
  }
  switch (value.t) {
    case "welcome":
      return (
        isU16(value.version) &&
        isU32(value.max_frame) &&
        (value.session === undefined || typeof value.session === "string")
      );
    case "ack":
      return (
        Array.isArray(value.versions) &&
        value.versions.every(isVersion) &&
        isVersion(value.synced)
      );
    case "records":
      return Array.isArray(value.records) && value.records.every(isRecord);
    case "view":
      return isVersion(value.version) && Object.hasOwn(value, "value");
    case "gap":
      return isVersion(value.from) && isVersion(value.to);
    case "logs":
      return Array.isArray(value.logs) && value.logs.every(isLogInfo);
    case "end":
    case "pong":
      return true;
    case "error":
      return CODES.has(value.code as string) && typeof value.message === "string";
    default:
      return false;
  }
}

function isRecord(value: unknown): value is LugRecord {
  return (
    isObject(value) &&
    isVersion(value.version) &&
    Object.hasOwn(value, "patch")
  );
}

function isLogInfo(value: unknown): value is LogInfo {
  return (
    isObject(value) &&
    typeof value.name === "string" &&
    typeof value.reducible === "boolean" &&
    isVersion(value.version) &&
    isVersion(value.oldest) &&
    isU32(value.subscribers)
  );
}

function isId(value: unknown): value is number {
  return isVersion(value);
}

function isVersion(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isU16(value: unknown): value is number {
  return Number.isInteger(value) && (value as number) >= 0 && (value as number) <= 0xffff;
}

function isU32(value: unknown): value is number {
  return (
    Number.isInteger(value) &&
    (value as number) >= 0 &&
    (value as number) <= 0xffff_ffff
  );
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
