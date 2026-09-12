# lug

A local append-only log daemon. Each log folds serializable patches into
versioned, immutable views. Subscribers stream records or follow a
materialized view. Nothing is exposed that the write-ahead log has not
already recorded.

## Layout

```
crates/cavlc         immutable AVL JSON store; the reference Reducible
crates/cavlc-demo    local REPL over cavlc, no daemon
crates/lug-core      Reducible, Storage, Log            (contract, frozen)
crates/lug-proto     frames, codec, HTTP route names    (contract, frozen)
crates/lug-wal       segmented file Storage
crates/lug-server    the daemon: actors, fan-out, unix + HTTP listeners
crates/lug-client    the hub: pooled multiplexed client, follower
crates/lug-tui       thin viewer over lug-client
ts/                  TypeScript client, same two transports
```

`lug-core` and `lug-proto` are the interfaces every other crate agrees on.
Changing either needs a reason stated in the commit message.

## Model

A **patch** is serializable and opaque to the log. A **version** is a `u64`;
every patch that changes state mints exactly one, and they are contiguous.
A **view** is an MVCC pointer: immutable, O(1) to clone, free to outlive the
structure that made it, serializable so it can be checkpointed and shipped.

`Reducible` is two-phase. `stage_patch` validates against current state and
produces private work nothing can observe; `commit` publishes it as one new
version. The split exists so `Log` can write its record to disk before the
change becomes visible.

`Storage` is parameterized on that view type, because `write_header`
checkpoints exactly that pointer. A header lets recovery skip every record it
covers.

`Log<D, S>` is itself a `Reducible`, so logs nest. It runs two counters: the
structure advances when a patch folds into memory, the **watermark** advances
only once the backing record is on disk to the requested degree. Acks, reads
and fan-out all ride the watermark. A write that fails after the fold
truncates the structure back to the watermark.

Two data structures ship: `Noop` (counts versions, keeps nothing, so the log
is a plain byte stream) and `cavlc::Store` (reducible, materialized views).

## Segment format

A log is a directory of segments plus one header file.

```
<data>/<log>/header
<data>/<log>/000000000000000001.seg
```

Segment name is the zero-padded version of its first record, 18 digits.

Segment file:

```
magic     8   b"LUGSEG\x00\x01"
reserved  8   zero
records   ...
```

Record:

```
len       u32 le    byte length of payload
crc       u32 le    crc32c over version || payload
version   u64 le
payload   len bytes
```

Recovery scans the last segment forward and stops at the first short read or
CRC mismatch, then truncates the file there. A torn tail after a crash is
normal and is not corruption. Versions must be contiguous across the scan; a
gap is corruption and must fail loudly.

Header file, written whole to `header.tmp` then renamed over `header`, so it
is never partially visible:

```
magic     8   b"LUGHDR\x00\x01"
crc       u32 le    over the JSON that follows
len       u32 le
json      len bytes   { "version": u64, "view": <serialized view> }
```

Segments rotate at 64 MiB. Segments entirely below the header version may be
deleted; `Storage::oldest` reports the lowest version still readable.

Preallocate each segment with `fallocate` and use `fdatasync`, not `fsync`,
so a flush does not drag a metadata update with it.

## Server

One actor per log, owning its `Log`, its ring buffer of recent records, and
its `watch` of the watermark. Writers from any connection reach it through a
bounded mpsc, which is the only serialization point; there is no mutex on the
write path.

The actor drains its inbox opportunistically: take one, then `try_recv` until
empty or the batch is full, then one `append` for the whole batch and at most
one sync. Idle, that is one record and one flush with minimum latency. Under
load the same code path amortizes both across hundreds of patches. No timer,
no artificial delay.

Fan-out is `watch` on the watermark plus a shared ring, not `broadcast`.
A subscriber wakes once and reads a range, so one wakeup can carry hundreds
of records. `broadcast` would duplicate storage and wake once per record per
subscriber.

A subscriber below the ring's oldest entry reads from the segments until it
catches up, then switches back to memory. If it is below `Storage::oldest`,
send `Gap` and resume at the oldest retained version.

**Credit is mandatory.** A stream is pushed at most `credit` records and then
stops until `Request::Credit` arrives. Without it one slow subscriber fills
the socket buffer and stalls every other stream on that connection, appends
included.

Log actors are assigned to cores by `hash(name)`, shared-nothing per log. The
registry is looked up once per subscription or first append, never per patch.

## Transports

Both carry the same frames.

- **Unix socket** `<run>/lug.sock`. One listening socket; concurrency comes
  from accepted connections, not from more listeners. `SO_REUSEPORT` does not
  exist for `AF_UNIX`, so do not reach for it.
- **HTTP** on localhost. `POST /v1/call` takes one `Request` frame and
  returns one `Response` frame. `GET /v1/stream` returns `text/event-stream`,
  one `Response` per `data:` line. The two are separate endpoints; a caller
  may hold a stream open and POST on another connection, correlating them
  with `X-Lug-Session`.

The client hub pools and multiplexes over whichever the caller names. One
connection carries many in-flight requests, matched by id; a single writer
task owns each sink, because a socket is one byte stream and two concurrent
writes interleave into garbage. Pool size defaults to the core count. Pick
the least-loaded connection, not round-robin.

## Security

- Unix socket: `SO_PEERCRED` on accept. Reject any uid but the daemon's own
  unless an ACL allows it. The socket lives in a directory created `0700`
  first, so the parent enforces access regardless of umask timing.
- No abstract-namespace sockets. They have no filesystem permissions.
- HTTP binds loopback only and requires `Authorization: Bearer <token>`. The
  token is read from a file at startup and never logged.
- Per-log ACL by uid, default deny for anyone but the owner.
- Frames above 16 MiB are refused on the length prefix, before allocating.
- Bounded channels everywhere. A wedged client must be slowed or dropped,
  never allowed to grow the daemon's heap.

## CLI

```
lug-server [--config <path>]
```

Config is TOML; every key has a flag override of the same name.

```toml
data      = "/var/lib/lug"      # segment directories, one per log
run       = "/run/lug"          # created 0700 before the socket is bound
socket    = "lug.sock"          # relative to run
http      = "127.0.0.1:7717"    # omit to disable HTTP
token     = "/etc/lug/token"    # bearer token file, 0600, never logged
allow_uid = []                  # additional uids beyond the daemon's own
segment   = "64MiB"
ring      = 4096                # records held in memory per log for fan-out
checkpoint_every = 10000        # versions between automatic checkpoints
```

`--check` validates config and exits. `lug-server` runs in the foreground and
logs to stderr; it never forks. The supervisor owns the process.

```
lug <command>
```

A client CLI over the same hub: `lug ls`, `lug create <log> [--reducible]`,
`lug append <log> [-]`, `lug tail <log> [--from N]`, `lug read <log> [--at N]`.
`--socket <path>` or `--http <url>` chooses the transport; unix is default.

## Rules

- Comments explain why, never what. No comment restates its line.
- No em dashes anywhere, including commit messages.
- `cargo clippy --workspace --all-targets -- -D warnings` is clean.
- Tests are real: drive the actual socket, actually crash and recover, do not
  assert against a mock of your own code.
- Errors carry the failing value. `Result`, never `unwrap` off the hot path.
- No `unsafe` outside a documented syscall wrapper.
