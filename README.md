# lug

A local append-only log daemon, in the shape of a very small Kafka.

Each log folds serializable patches into versioned, immutable views.
Subscribers either tail the record stream or follow a materialized view that
the client keeps up to date in memory. Nothing is ever exposed that the
write-ahead log has not already recorded.

Two transports carry the same frames: a unix socket, and HTTP with SSE for
subscriptions. The client pools and multiplexes over whichever you name.

## Why it is shaped this way

**Patches, not values.** A log stores changes. Whether those changes mean
anything is the data structure's business, not the log's. `Noop` keeps
nothing and gives you a plain byte stream; `cavlc::Store` folds them into an
immutable JSON tree you can read at any past version.

**Two-phase application.** `stage` validates a patch against current state
and produces private working state nobody can see. `commit` publishes it.
That split is the only reason the write-ahead log is genuinely write-ahead:
the record reaches disk while the change is still invisible.

**Two counters.** The data structure advances the moment a patch folds into
memory. The *watermark* advances only once the record backing that version is
on disk to the degree the caller asked for. Acknowledgements, reads and
subscriber fan-out all ride the watermark. A write that fails after the fold
truncates memory back, so memory never leads the log.

**Ownership, not locks.** Each log is an actor that owns its state. Writers
reach it through a channel, which serializes appends better than a mutex
would: ordered, bounded, and free of contention on the write path. It also
makes group commit fall out for free. The actor drains whatever piled up
while it was busy, then does one write and at most one flush for the lot. No
timer, no artificial delay: idle, latency is minimal; loaded, the same code
amortizes across hundreds of patches.

**Credit, not hope.** Every subscription has explicit flow control. The
server pushes at most the granted number of records and then stops. Without
that, one slow subscriber fills the socket buffer and stalls every other
stream on the connection, appends included.

## Layout

| | |
| --- | --- |
| `crates/cavlc` | immutable AVL JSON store; the reference reducible |
| `crates/lug-core` | `Reducible`, `Storage`, `Log` |
| `crates/lug-proto` | frames, codec, HTTP routes |
| `crates/lug-wal` | segmented file storage |
| `crates/lug-server` | the daemon |
| `crates/lug-client` | pooled multiplexing hub, and `Follower` |
| `crates/lug-tui` | a thin viewer |
| `ts/` | TypeScript client |

`lug-core` and `lug-proto` are the contracts. See `SPEC.md` for the segment
format, the actor model, the credit protocol and the security posture.

## Build

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
