#!/usr/bin/env bash
#
# demo.sh - stand up a lug daemon in a scratch directory, drive it, tear it
# down. Everything lives under one temp dir and nothing touches the system.
#
# Run it with no arguments. It should print a readable transcript and exit 0.

set -euo pipefail

PROG=${0##*/}
ROOT=$(cd "$(dirname "$0")" && pwd)
KEEP=${KEEP:-}
TRANSPORT=unix

die() {
	printf '%s: %s\n' "$PROG" "$*" >&2
	exit 1
}

usage() {
	cat >&2 <<-EOF
		usage: $PROG [--http] [--keep]

		  --http   drive the HTTP transport instead of the unix socket
		  --keep   leave the scratch directory behind and print its path
	EOF
	exit 2
}

while [ $# -gt 0 ]; do
	case $1 in
	--http) TRANSPORT=http ;;
	--keep) KEEP=1 ;;
	-h | --help) usage ;;
	*) die "unknown argument: $1" ;;
	esac
	shift
done

# Colour only when a person is watching, so a piped run stays greppable.
if [ -t 1 ]; then
	dim=$'\033[2m' bold=$'\033[1m' off=$'\033[0m'
else
	dim='' bold='' off=''
fi

say() { printf '\n%s==> %s%s\n' "$bold" "$*" "$off"; }
run() {
	printf '%s   $ %s%s\n' "$dim" "$*" "$off"
	"$@"
}

say "building"
cargo build --release -q --workspace || die "build failed"
SERVER="$ROOT/target/release/lug-server"
LUG="$ROOT/target/release/lug"

# Name the missing piece instead of letting cargo complain about a bin target,
# because during the build-out the honest answer is "not written yet".
missing=
[ -x "$SERVER" ] || missing="$missing lug-server"
[ -x "$LUG" ] || missing="$missing lug"
if [ -n "$missing" ]; then
	cat >&2 <<-EOF
		$PROG: not built yet:$missing

		This script is the acceptance test for the daemon and its CLI, and it
		is written ahead of them on purpose. What does work today:

		  cargo test --workspace
		  cargo run -p cavlc-demo          the local REPL, no daemon
		  (cd ts && npm test)              the TypeScript client
	EOF
	[ -x "$ROOT/target/release/lug-load" ] &&
		printf '  target/release/lug-load --smoke   once lug-server exists
' >&2
	exit 1
fi

DIR=$(mktemp -d /tmp/lug-demo.XXXXXX)
SOCK="$DIR/run/lug.sock"
PORT=17717
SERVER_PID=

cleanup() {
	local rc=$?
	[ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null
	if [ -n "$KEEP" ]; then
		printf '\nscratch kept at %s\n' "$DIR"
	else
		rm -rf "$DIR"
	fi
	exit $rc
}
trap cleanup EXIT INT TERM

mkdir -p "$DIR/data" "$DIR/run"
# The token is a demo secret and lives only in the scratch dir, which mktemp
# already made 0700.
head -c 32 /dev/urandom | base64 >"$DIR/token"
chmod 600 "$DIR/token"

cat >"$DIR/lug.toml" <<-EOF
	data = "$DIR/data"
	run = "$DIR/run"
	socket = "lug.sock"
	http = "127.0.0.1:$PORT"
	token = "$DIR/token"
	segment = "4MiB"
	ring = 1024
EOF

say "config"
sed "s|$DIR|\$DIR|g" "$DIR/lug.toml" | sed "s/^/   /"

say "starting the daemon"
"$SERVER" --config "$DIR/lug.toml" >"$DIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait on the socket rather than sleeping, and give up rather than hang.
for _ in $(seq 1 100); do
	[ -S "$SOCK" ] && break
	kill -0 "$SERVER_PID" 2>/dev/null || {
		cat "$DIR/server.log" >&2
		die "daemon exited during startup"
	}
	sleep 0.05
done
[ -S "$SOCK" ] || { cat "$DIR/server.log" >&2; die "socket never appeared at $SOCK"; }
printf '   daemon up, pid %s\n' "$SERVER_PID"

if [ "$TRANSPORT" = http ]; then
	AT=(--http "http://127.0.0.1:$PORT" --token-file "$DIR/token")
else
	AT=(--socket "$SOCK")
fi

say "a reducible log: patches fold into a materialized view"
run "$LUG" "${AT[@]}" create notes --reducible

for patch in \
	'{"Create":{"title":"lug","tags":[]}}' \
	'{"Create":{"author":{"name":"Gluck"}}}' \
	'{"Update":{"title":"lug: a little log"}}' \
	'{"Update":{"author":{"Create":{"city":"Atlanta"}}}}'; do
	printf '%s   $ echo %s | lug append notes -%s\n' "$dim" "$patch" "$off"
	printf '%s\n' "$patch" | "$LUG" "${AT[@]}" append notes -
done

say "the view, reduced from those four patches"
run "$LUG" "${AT[@]}" read notes

say "the same log as history: every append minted a version"
run "$LUG" "${AT[@]}" tail notes --from 0 --follow=false

say "reading an older version, because the structure is partially persistent"
run "$LUG" "${AT[@]}" read notes --at 2

say "a plain log: no reduction, just an ordered stream"
run "$LUG" "${AT[@]}" create events
for i in 1 2 3; do
	printf '{"event":"tick","n":%d}\n' "$i" | "$LUG" "${AT[@]}" append events -
done
run "$LUG" "${AT[@]}" tail events --from 0 --follow=false

say "live tail, while an appender writes into it"
"$LUG" "${AT[@]}" tail events --from 3 >"$DIR/tail.out" 2>&1 &
TAIL_PID=$!
sleep 0.3
for i in 4 5 6; do
	printf '{"event":"tick","n":%d}\n' "$i" | "$LUG" "${AT[@]}" append events - >/dev/null
done
sleep 0.3
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true
sed "s/^/   /" "$DIR/tail.out"

say "durability: kill the daemon uncleanly and reopen"
run "$LUG" "${AT[@]}" ls
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=
printf '   SIGKILL sent, no clean shutdown, no checkpoint\n'

"$SERVER" --config "$DIR/lug.toml" >>"$DIR/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
	[ -S "$SOCK" ] && break
	sleep 0.05
done
[ -S "$SOCK" ] || { cat "$DIR/server.log" >&2; die "daemon did not come back"; }

say "recovered from the write-ahead log"
run "$LUG" "${AT[@]}" ls
run "$LUG" "${AT[@]}" read notes

say "done"
cat <<-EOF

	   Watch it live:    $LUG ${AT[*]} tail events
	   The view, in a TUI:
	                     target/release/lug-tui ${AT[*]} notes --reducible
	   Prove it holds:   target/release/lug-load --smoke
	   Server log:       $DIR/server.log

EOF
