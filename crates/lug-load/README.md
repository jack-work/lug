# lug-load

Linux load and conformance runner. It launches `lug-server --config <toml>`,
talks directly over unix sockets or HTTP/1.1, and removes its private data
and run directories on exit. It does not use lug-client or a server library.
The socket parent is 0700; the generated HTTP bearer file is 0600. No
existing credential file is read. HTTP uses loopback and a fresh token.

```sh
cargo build --release -p lug-load -p lug-server
./target/release/lug-load --smoke
cargo run --release -p lug-load -- --server ./target/release/lug-server \
  --connections 10000 --logs 8 --appenders-per-log 4 \
  --subscribers-per-log 4 --rate 10000 --duration 60 --json
cargo run --release -p lug-load -- --server ./target/release/lug-server \
  --transport http --connections 100 --soak --soak-cycles 20
```

`--smoke` runs one connection, log, appender and subscriber, with 20 durable
appends in a one-second scheduling window. It also runs credit isolation and
SIGKILL/replay, takes a few seconds, and prints four result lines. It cannot
be combined with `--soak`. The server is found beside lug-load first, then
in PATH; `--server` overrides discovery.

`--help` lists all knobs. Missing server executable: exit 0 with an explicit
`skipped` status, never a zero-throughput success. A server that starts and
exits, fails startup, rejects a frame, loses data, or stalls the workload
fails the run. Exit codes: 1 failure, 2 invalid flags, 130 SIGINT, 143 SIGTERM.
SIGTERM is used for normal cleanup, with a five-second deadline before kill.

## Numbers

Default output is a table. `--json` emits one schema-1 object on stdout;
progress, limits and server diagnostics go to stderr. A failed run emits
`status: failed` and does not publish a successful throughput result.

| Field | Exact meaning and limit |
| --- | --- |
| `held_connections` | Simultaneously established, Hello-confirmed call connections, all required to answer a final Ping after the fixed load window. Default 10,000. This is separate from accept throughput. |
| `held_transport_sockets_including_sse` | Unix: exactly `connections` sockets to one pathname/listener. HTTP: that many keep-alive call sockets plus one SSE socket per subscriber. |
| `accepts_per_sec` | Separate short-lived connect + Hello + Welcome + disconnect trials divided by their elapsed time. Includes client work and handshake cost; not a kernel accept counter or a server-only maximum. `--parallel` bounds concurrent attempts. Null when `--accepts 0`. |
| `establishment_handshakes_per_sec` | Ramp-up for the held pool, not the short-lived accept regime. |
| `appends_per_sec` | Acknowledged one-patch appends divided by elapsed scheduling plus drain time. No batches masquerading as single records. |
| `window_appends_per_sec` | Acks observed strictly inside the fixed scheduling window divided by that window. Contrast with target rate and drain-inclusive rate to detect a server falling behind. |
| `append_latency` | HDR histogram, three significant digits, microseconds rounded up. Intended publish time to decoded Ack at the harness. Includes scheduling delay, harness queues, serialization and transport. p50/p99/p999 are observed quantiles, not confidence bounds. Empty histograms contain null quantiles. |
| `subscriber_records_per_sec` | Delivered records summed across subscribers, divided by scheduling plus drain time. One record delivered to four subscribers counts four. `window_subscriber_records_per_sec` counts only observations inside the fixed window. |
| `publish_to_subscriber_latency` | Intended publish time to decoded Records at each subscriber. One sample per delivery. Includes harness parsing and scheduling, not a server timestamp. |
| `bytes_per_sec` | Harness-observed request and response bytes during scheduling plus drain, divided by the same elapsed time. Includes framing, HTTP headers/chunks and credit traffic consumed during that interval. Excludes kernel overhead, ramp-up, probes, final cancellation/heartbeat and late credit writes. |
| `server_rss_cold_bytes` | Quiescent child RSS before probes: a fixed-process baseline, not per-connection memory. |
| `server_rss_warm_baseline_bytes` | Quiescent RSS after probes and log creation, before opening the held pool. |
| `server_rss_connections_only_bytes` | Total RSS after opening call sockets, before workload subscription setup. |
| `server_rss_incremental_per_call_socket_estimate_bytes` | Nonnegative difference between the last two RSS values divided by call socket count. An estimate from one run, not an isolated allocation measurement; allocator retention and background work affect it. Excludes HTTP SSE sockets opened later. |
| `server_rss_peak_bytes` | Maximum child VmRSS sampled every 100 ms during load and at connection establishment. Total RSS, including log state and fan-out buffers. Shorter spikes can be missed. No kernel socket memory or harness memory. |
| `fd_quiescent_*` | `/proc/<pid>/fd` counts in the same process, with the same logs and WAL state, before and after connection/subscription churn. Not RSS and not cross-restart counts. |

RLIMIT_NOFILE is raised to the available hard limit before spawn. Previous,
actual soft and hard limits are reported; the child inherits them. Failure
to obtain enough descriptors is a failed setup, never a reduced connection
count. HTTP may also hit the local ephemeral-port limit.

## Open loop and correctness

Append `n` is due at `start + n / rate`, independent of replies. One fixed
schedule is shared across writer lanes. Latency is measured from that intended
time, not from when a delayed write finally reaches the socket. This avoids
coordinated omission. There is no HDR synthetic correction and no retry.
`--max-inflight` and bounded socket queues fail explicitly on overload;
they never silently drop samples or lower the target rate. The schedule
contains `floor(rate * duration)` appends, then a bounded drain. The default
10,000-connection run does not make 10,000 connections busy: writer and
subscriber counts are independent knobs, multiplexed over the held pool.
One successful run establishes capacity at its stated workload, not that
an accept loop can never become a bottleneck.

Every subscriber must observe versions contiguous from 1, without duplicate
versions or payloads. The ledger compares the complete payload against the
issued append and ties that identity to its acknowledged version. Every
subscriber must agree with that same per-log mapping, including when records
arrive before Acks. Gaps, changed payloads, duplicate Acks, insufficient
synced watermarks and missing final deliveries are errors. Subscribe must acknowledge before
records; every Credit must acknowledge exactly once. Missing or duplicate
control acknowledgements fail too. Ledger memory is
linear in issued appends, not in subscribers times delivered records.

Each run first puts a zero-credit subscriber, an active subscriber and an
appender on the same unix connection. The active side must acknowledge and
deliver 128 x 32 KiB patches within the deadline while the stopped stream
receives nothing. That stream must then replay exactly one record after a
one-record grant, followed by a checked 250 ms silence interval. Duplicate
subscription acknowledgements are rejected. A stall or credit overrun is a
hard failure. HTTP runs use
the same call connection and session-routed SSE endpoints, since HTTP streams
necessarily occupy separate sockets. This is not a test of a peer that stops
reading TCP altogether; it tests the protocol's credit isolation claim.

Every epoch ends with a separate durable probe: 32 durable appends, SIGKILL
immediately after the final decoded Ack, restart using the same data directory,
and exact replay of all 32. A durable workload must also replay every one of
its acknowledged records. Memory/written workloads do not promise crash
survival; their recovery is not asserted. Checkpointing is disabled so records
cannot legitimately disappear beneath these exact-replay checks. This tests
process death, not power loss or dishonest storage hardware.

`--soak` repeats measured epochs, creates fresh logs, churns 128 connections
and zero-credit subscribe/cancel pairs per epoch, and crashes/restarts between
epochs. FD counts must settle and stay within `--fd-slack` (default zero) of
a fixed baseline after each group of 32 churns. Baselines are taken after
WAL warmup, before churn, so legitimate log/segment descriptors are not
mislabelled as socket leaks. Churn is between measured windows, not included
in headline rates. Checks retain only the current epoch's ledger.

## Tests and measurement status

`cargo test -p lug-load` tests the checker and timing code, drives a real unix
socket against an in-process mock with injected faults, and exercises raw
HTTP chunk/SSE parsing and session-routed credit/cancel/replay. CLI tests
check missing-server skips and startup failures. These tests establish harness behavior, not daemon
performance or durable storage. Run the executable against the real daemon
for conformance and headline measurements. No real-server numbers are claimed
by the unit tests or by a missing-binary skip.
