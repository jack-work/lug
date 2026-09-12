# cavlc

Concurrent AVL control.

In-memory AVL JSON snapshots with a single writer per store. Inspired by
Figaro's `api/form/tree.go` and `internal/store/form.go`, read from
`/home/gluck/dev/figaro-qua/main` at `3cf5c54c`. No actors or disk storage.

## Run the demo

```sh
cd /home/gluck/dev/lug
cargo run -p cavlc-demo
```

Enter these lines individually:

```text
{"Create":{"profile":{"name":"Gluck","address":{"city":"Atlanta"}},"count":0}}
{"Update":{"count":2,"profile":{"Update":{"name":"Figaro"}}}}
{"Update":{"profile":{"Update":{"address":{"Delete":["city"]}}}}}
{"Delete":["profile"]}
:show 1
:log
```

The four patches produce versions 1 through 4. Every command redraws the current
snapshot, including errors. `:show 1` also displays history; it does not move the
current version. `:log` shows committed batches.

A batch lets several patches publish together:

```text
:batch
{"Create":{"profile":{"name":"Gluck"}}}
{"Update":{"profile":{"Update":{"name":"Figaro"}}}}
:apply
:quit
```

The TUI shows both the current and working snapshots while a batch is
open. `:apply` publishes one version with the patches in application order.
`:abort` discards staged edits. Outside `:batch`, each patch commits immediately.
Nothing is written to disk. Ctrl-D, Ctrl-C, or `:quit` exits.

`demo/` is disposable and separate from the library. It uses normal terminal
line editing, without raw mode or an alternate screen. `--plain` appends screens
rather than clearing them. Piped input emits JSON Lines:

```sh
cargo run -q -p cavlc-demo <<'EOF'
{"Create":{"count":0}}
{"Update":{"count":1}}
{"Delete":["count"]}
:log
EOF
```

Each response includes the current version and snapshot. Errors leave state
intact and do not stop later commands. Diagnostics also go to stderr. A piped
run exits 1 if any command failed; bad flags exit 2.

## Three operations

The patch itself is an object, with no wrapper or type tags.

| Operation | Meaning |
| --- | --- |
| `{"Create":{"key":value}}` | Initialize an absent property with any JSON value, including a subtree. |
| `{"Update":{"key":value}}` | Replace an existing leaf. |
| `{"Update":{"key":patch}}` | Descend into an existing object. |
| `{"Delete":["key"]}` | Remove an existing property and its entire subtree. |

`{}` is a no-op. There is no `Set`. No operation supplies or matches old values.

`Create` initializes a new property and any children in its value. For example,
`{"Create":{"profile":{"name":"Gluck"}}}` creates both `profile` and
`profile.name`. Those children are normal addressable nodes, not opaque data.
To add a child to an existing object, use nested `Create` through `Update`.
Parents on an `Update` path must already exist; they are never inferred.

Creation is not replacement: `Create` fails if the property already exists.
A patch can operate on several distinct keys, but cannot have multiple
operations on the same key; use consecutive patches in a batch.

Objects in `Update` always contain operations, never replacement object data.
Updating a leaf with an object patch, or an object with a leaf value, is an
error. Changing between those two kinds requires `Delete` followed by `Create`.
An initialized object is accepted by `Create`, not as an `Update` replacement.

An inner deletion is wrapped in one `Update` per ancestor. Deleting the last
child leaves its parent as `{}`. Deleting the parent removes all children at
once. Deleted objects cannot receive further updates or child creations.
Explicit `Create` may reuse the name, but that is a new creation, not a revival
of the old subtree.

Missing deletes and repeated creates are errors. `null` is a leaf value, not
deletion. Keys containing dots or slashes are literal names. Arrays are opaque
leaf values: replaced whole, never unpacked into addressable properties. JSON
objects inside arrays remain opaque array data. The root is always an object.

## Snapshots, writers, and logs

Each `Store` is one concurrency domain. Writes require `&mut Store`. If several
threads share a domain, put a `Mutex<Store>` around it. Partition into separate
stores when needed; no lock manager or cross-store batch system is built
in. Owned snapshots are immutable and may be read outside the writer lock.

A batch owns a base snapshot and a private working root. `apply_batch` rejects
it if its base version is no longer current, including version 0. Even disjoint
stale changes conflict within one store; separate stores do not conflict.
Batches from another store are also rejected.

Deleting a parent invalidates older batches attempting to add children.
An explicit delete/recreate advances the version even when the resulting JSON
is identical, so an older batch cannot publish into the replacement.
A single `Store::apply(patch)` targets the current state. Callers handling stale
remote intent can compare their version with `batch.base().version()` before
applying patches. Publication checks that the batch base is still current.

A successful batch records its ordered state-changing patches. Initial subtree
values stay in their `Create` record, and delete/recreate boundaries are kept. Equal-value updates and empty patches are skipped. Once a batch
has made changes, it advances the version even if later edits restore the same
JSON. A failed patch leaves earlier staged edits intact; abort the batch
to discard those. `apply_batch` consumes the batch, including on conflict.

```json
{"version":1,"patches":[{"Create":{"profile":{}}},{"Update":{"profile":{"Create":{"name":"Gluck"}}}}]}
```

`Store::replay(initial, records)` applies each batch atomically and rebuilds
its snapshot. Versions must be contiguous. Empty batches, identity patches,
and invalid operations are rejected; no partial store is returned. Supply the
same version-0 state. `from_value` is a snapshot import API, not a patch shortcut.
Version numbers alone do not verify that the supplied initial state is correct.

`patches_between(a, b)` returns batch records in `(a, b]`; invalid bounds
return `None`. All history remains in memory until dropped. Owned snapshots can
outlive the store. This format is not compatible with earlier demo logs, Figaro
logs, or RFC 6902.

## Patch merge

`Patch::merge(&other)` combines disjoint edits, recursively merging updates
beneath the same object. It returns a patch without reading or changing state;
apply that result once with `Store::apply`.

```rust
use cavlc::Patch;

let a: Patch = serde_json::from_str(r#"{"Update":{"profile":{"Update":{"name":"Gluck"}}}}"#).unwrap();
let b: Patch = serde_json::from_str(r#"{"Update":{"profile":{"Delete":["temporary"]}}}"#).unwrap();
let merged = a.merge(&b).unwrap();
```

Overlapping leaf edits and structural operations return `Error::MergeConflict`
with the conflicting path. This operation does not choose a winner or compose
ordered changes such as `Create` followed by `Update`, or `Delete` followed by
`Create`. Keep those as separate patches in a batch. Batch application does not
automatically merge its patches.

## Retained nodes

Clone any `&Value` returned by `get` or `at` to retain that node independently.
Cloning is O(1). Threads may keep these handles after updates, parent deletion,
or dropping the store. `as_atom()` and `as_object()` borrow their contents.
A detached node carries no version or store identity; keep that context beside
it when needed.

```rust
use cavlc::{Patch, Store};

let mut store = Store::new();
let patch: Patch = serde_json::from_str(r#"{"Create":{"text":"hello"}}"#).unwrap();
store.apply(&patch).unwrap();
let node = store.snapshot().root().get("text").unwrap().clone();
drop(store);
std::thread::spawn(move || {
    assert_eq!(node.as_atom().unwrap().as_str(), Some("hello"));
}).join().unwrap();
```

## Before text

Old text lives in the batch's base snapshot. A commit at version `n` has
its prior state at `n - 1`; the patches do not duplicate old text. Replaying the
log reconstructs these versions. For intermediate edits within one batch,
replay that batch's preceding patches against its base.

A client holding the same base version has the old text needed for a future
three-way merge attempt. Current state alone is insufficient. Overlapping edits
may still need a decision. No text merge algorithm is implemented here.

## Library boundaries

- `avl::AvlMap<K, V>`: immutable ordered map. O(1) clones; O(log n) lookup,
  insertion, and deletion. Path copies share untouched nodes and entry payloads.
- `Value`: immutable JSON with one AVL map per object and `Arc`-shared values.
  `as_atom()` borrows a leaf without copying it.
- `Patch` / `Update`: pure checked operations. No I/O or serialization in edits.
- `Store`: private working roots, versioned `Snapshot`s, and ordered `Commit`s.

Publication does not diff the entire state. Snapshot rendering is confined to
the demo and costs proportional to its size.

```rust
use cavlc::{Patch, Store};

let mut store = Store::new();
let create: Patch = serde_json::from_str(r#"{"Create":{"text":"hello"}}"#).unwrap();
store.apply(&create).unwrap();
let before = store.snapshot();
let update: Patch = serde_json::from_str(r#"{"Update":{"text":"hello again"}}"#).unwrap();
store.apply(&update).unwrap();
assert_eq!(before.version(), 1);
assert_eq!(store.snapshot().version(), 2);
```

## Check

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
