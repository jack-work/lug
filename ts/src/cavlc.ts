import type { JsonPrimitive, JsonValue } from "./types.js";

export type JsonObject = { [key: string]: JsonValue };
export type CavlcUpdate = JsonPrimitive | JsonValue[] | CavlcPatch;

export interface CavlcPatch {
  Create?: JsonObject;
  Update?: { [key: string]: CavlcUpdate };
  Delete?: string[];
}

export class CavlcPatchError extends Error {
  public readonly path: readonly string[];

  public constructor(path: readonly string[], reason: string) {
    const location =
      path.length === 0 ? "root" : path.map((part) => JSON.stringify(part)).join(".");
    super(`${location}: ${reason}`);
    this.name = "CavlcPatchError";
    this.path = [...path];
  }
}

export function applyCavlcPatch(
  value: JsonObject,
  patch: CavlcPatch,
): JsonObject {
  validatePatch(patch, []);
  const next = cloneObject(value);
  applyAt(next, patch, []);
  return next;
}

function validatePatch(value: unknown, path: string[]): asserts value is CavlcPatch {
  if (!isObject(value)) {
    throw new CavlcPatchError(path, "patch must be an object");
  }
  for (const operation of Object.keys(value)) {
    if (operation !== "Create" && operation !== "Update" && operation !== "Delete") {
      throw new CavlcPatchError(path, `unknown operation ${JSON.stringify(operation)}`);
    }
  }

  const create = own(value, "Create");
  const update = own(value, "Update");
  const remove = own(value, "Delete");
  if (create !== undefined && !isObject(create)) {
    throw new CavlcPatchError(path, "Create must be an object");
  }
  if (update !== undefined && !isObject(update)) {
    throw new CavlcPatchError(path, "Update must be an object");
  }
  if (
    remove !== undefined &&
    (!Array.isArray(remove) || remove.some((key) => typeof key !== "string"))
  ) {
    throw new CavlcPatchError(path, "Delete must be an array of strings");
  }

  const creates = isObject(create) ? create : {};
  const updates = isObject(update) ? update : {};
  const deletes = Array.isArray(remove) ? (remove as string[]) : [];
  const keys = new Set<string>();
  for (const key of Object.keys(creates)) {
    keys.add(key);
  }
  for (const [key, child] of Object.entries(updates)) {
    if (keys.has(key)) {
      throw new CavlcPatchError([...path, key], "multiple operations for key");
    }
    keys.add(key);
    if (isObject(child)) {
      validatePatch(child, [...path, key]);
    }
  }
  for (const key of deletes) {
    if (keys.has(key)) {
      throw new CavlcPatchError([...path, key], "multiple operations for key");
    }
    keys.add(key);
  }
}

function applyAt(target: JsonObject, patch: CavlcPatch, path: string[]): void {
  for (const [key, child] of Object.entries(patch.Create ?? {})) {
    const childPath = [...path, key];
    if (Object.hasOwn(target, key)) {
      throw new CavlcPatchError(childPath, "Create requires an absent key");
    }
    define(target, key, cloneJson(child));
  }

  for (const [key, update] of Object.entries(patch.Update ?? {})) {
    const childPath = [...path, key];
    if (!Object.hasOwn(target, key)) {
      throw new CavlcPatchError(
        childPath,
        "Update requires an existing key; use Create first",
      );
    }
    const current = target[key];
    if (isObject(update)) {
      if (!isObject(current)) {
        throw new CavlcPatchError(childPath, "expected an object");
      }
      applyAt(current, update, childPath);
    } else {
      if (isObject(current)) {
        throw new CavlcPatchError(
          childPath,
          "cannot replace an object; Delete it before Create",
        );
      }
      define(target, key, cloneJson(update as JsonValue));
    }
  }

  for (const key of patch.Delete ?? []) {
    const childPath = [...path, key];
    if (!Object.hasOwn(target, key)) {
      throw new CavlcPatchError(childPath, "Delete requires an existing key");
    }
    delete target[key];
  }
}

function cloneJson(value: JsonValue): JsonValue {
  if (Array.isArray(value)) {
    return value.map(cloneJson);
  }
  if (isObject(value)) {
    return cloneObject(value);
  }
  return value;
}

function cloneObject(value: JsonObject): JsonObject {
  const clone: JsonObject = {};
  for (const [key, child] of Object.entries(value)) {
    define(clone, key, cloneJson(child));
  }
  return clone;
}

function define(target: JsonObject, key: string, value: JsonValue): void {
  Object.defineProperty(target, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function own(value: JsonObject, key: string): JsonValue | undefined {
  return Object.hasOwn(value, key) ? value[key] : undefined;
}
