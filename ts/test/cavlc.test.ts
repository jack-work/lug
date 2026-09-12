import assert from "node:assert/strict";
import { test } from "node:test";
import {
  CavlcPatchError,
  applyCavlcPatch,
  type CavlcPatch,
  type JsonObject,
} from "../src/index.js";

test("cavlc applies create, nested update, and delete", () => {
  const initial: JsonObject = {};
  const created = applyCavlcPatch(initial, {
    Create: {
      profile: { name: "Gluck", address: { city: "Atlanta" } },
      count: 0,
    },
  });
  const updated = applyCavlcPatch(created, {
    Update: {
      count: 2,
      profile: { Update: { name: "Figaro" } },
    },
  });
  const nestedDelete = applyCavlcPatch(updated, {
    Update: {
      profile: { Update: { address: { Delete: ["city"] } } },
    },
  });
  const deleted = applyCavlcPatch(nestedDelete, { Delete: ["profile"] });

  assert.deepEqual(initial, {});
  assert.deepEqual(nestedDelete, {
    profile: { name: "Figaro", address: {} },
    count: 2,
  });
  assert.deepEqual(deleted, { count: 2 });
});

test("cavlc treats arrays as opaque leaves", () => {
  const value = applyCavlcPatch({ items: [1, { nested: true }] }, {
    Update: { items: [2, 3] },
  });
  assert.deepEqual(value, { items: [2, 3] });
  assert.throws(
    () =>
      applyCavlcPatch(value, {
        Update: { items: { Create: { child: 1 } } },
      }),
    /expected an object/,
  );
});

test("cavlc rejects failed preconditions without changing the input", () => {
  const initial: JsonObject = { existing: 1, branch: { leaf: true } };
  const cases: CavlcPatch[] = [
    { Create: { existing: 2 } },
    { Update: { missing: 2 } },
    { Delete: ["missing"] },
    { Update: { branch: 2 } },
    { Update: { existing: { Create: { child: 1 } } } },
    { Create: { duplicate: 1 }, Delete: ["duplicate"] },
  ];
  for (const patch of cases) {
    assert.throws(() => applyCavlcPatch(initial, patch), CavlcPatchError);
    assert.deepEqual(initial, { existing: 1, branch: { leaf: true } });
  }
});

test("cavlc accepts literal prototype and dotted keys", () => {
  const patch = JSON.parse(
    '{"Create":{"__proto__":{"safe":true},"a.b":1}}',
  ) as CavlcPatch;
  const value = applyCavlcPatch({}, patch);
  assert.equal(Object.hasOwn(value, "__proto__"), true);
  assert.deepEqual(value["__proto__"], { safe: true });
  assert.equal(value["a.b"], 1);
  assert.equal(Object.getPrototypeOf(value), Object.prototype);
});

test("empty cavlc patch is a no-op value copy", () => {
  const initial: JsonObject = { nested: { value: 1 } };
  const next = applyCavlcPatch(initial, {});
  assert.deepEqual(next, initial);
  assert.notEqual(next, initial);
  assert.notEqual(next.nested, initial.nested);
});
