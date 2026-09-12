#!/usr/bin/env python3
"""Drive the daemon the way demo.sh does, without the lug client binary.

Speaks the framed protocol over the unix socket and plain HTTP on loopback,
using the exact config keys demo.sh writes.
"""
import base64
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
SERVER = os.path.join(ROOT, "target", "debug", "lug-server")


class Conn:
    def __init__(self, path):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect(path)

    def send(self, **frame):
        body = json.dumps(frame).encode()
        self.sock.sendall(struct.pack(">I", len(body)) + body)

    def recv(self):
        head = self._read(4)
        (length,) = struct.unpack(">I", head)
        return json.loads(self._read(length))

    def call(self, **frame):
        self.send(**frame)
        return self.recv()

    def _read(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf += chunk
        return buf


def wait_for_socket(path, deadline=10):
    end = time.time() + deadline
    while time.time() < end:
        try:
            Conn(path)
            return
        except OSError:
            time.sleep(0.02)
    raise SystemExit(f"no daemon answering at {path}")


def check(label, condition, detail=""):
    mark = "ok  " if condition else "FAIL"
    print(f"   {mark} {label}{(' ' + detail) if detail else ''}")
    if not condition:
        raise SystemExit(1)


def main():
    dirname = tempfile.mkdtemp(prefix="lug-demo-check.")
    port = 17719
    token = base64.b64encode(os.urandom(24)).decode()
    with open(os.path.join(dirname, "token"), "w") as f:
        f.write(token + "\n")
    config = os.path.join(dirname, "lug.toml")
    with open(config, "w") as f:
        f.write(
            f'data = "{dirname}/data"\n'
            f'run = "{dirname}/run"\n'
            'socket = "lug.sock"\n'
            f'http = "127.0.0.1:{port}"\n'
            f'token = "{dirname}/token"\n'
            'segment = "4MiB"\n'
            "ring = 1024\n"
        )
    sock_path = os.path.join(dirname, "run", "lug.sock")
    log = open(os.path.join(dirname, "server.log"), "w")

    print("==> starting with demo.sh's config")
    server = subprocess.Popen([SERVER, "--config", config], stdout=log, stderr=log)
    wait_for_socket(sock_path)
    check("socket appeared", os.path.exists(sock_path), sock_path)
    check("run dir is 0700", oct(os.stat(os.path.join(dirname, "run")).st_mode)[-3:] == "700")

    c = Conn(sock_path)
    check("hello", c.call(t="hello", id=1, version=1)["t"] == "welcome")
    check("create reducible", c.call(t="create", id=2, log="notes", reducible=True)["t"] == "logs")
    patches = [
        {"Create": {"title": "lug", "tags": []}},
        {"Create": {"author": {"name": "Gluck"}}},
        {"Update": {"title": "lug: a little log"}},
        {"Update": {"author": {"Create": {"city": "Atlanta"}}}},
    ]
    for i, patch in enumerate(patches):
        ack = c.call(t="append", id=10 + i, log="notes", patches=[patch])
        check(f"append {i + 1}", ack["t"] == "ack", str(ack.get("versions")))
    view = c.call(t="read", id=20, log="notes")
    check("read view", view["t"] == "view" and view["version"] == 4, json.dumps(view["value"]))
    old = c.call(t="read", id=21, log="notes", at=2)
    check("read at 2", old["t"] == "view" and old["version"] == 2)

    check("create plain", c.call(t="create", id=30, log="events", reducible=False)["t"] == "logs")
    for i in range(1, 4):
        c.call(t="append", id=40 + i, log="events", patches=[{"event": "tick", "n": i}])
    c.send(t="subscribe", id=50, log="events", **{"from": 0}, mode="records", credit=100)
    check("subscribe acknowledged first", c.recv() == {"t": "ok", "id": 50})
    records = c.recv()
    check("tail from 0", [r["version"] for r in records["records"]] == [1, 2, 3])

    print("\n==> http transport")
    def http(frame, path="/v1/call"):
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}",
            data=json.dumps(frame).encode(),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        return json.load(urllib.request.urlopen(request, timeout=5))

    check("http ping", http({"t": "ping", "id": 1})["t"] == "pong")
    ack = http({"t": "append", "id": 2, "log": "events", "patches": [{"event": "tick", "n": 4}]})
    check("http append", ack["t"] == "ack", str(ack["versions"]))
    check("http ls", len(http({"t": "list", "id": 3})["logs"]) == 2)

    print("\n==> SIGKILL, no clean shutdown and no checkpoint")
    server.send_signal(signal.SIGKILL)
    server.wait()
    server = subprocess.Popen([SERVER, "--config", config], stdout=log, stderr=log)
    wait_for_socket(sock_path)

    c = Conn(sock_path)
    logs = c.call(t="list", id=1)
    names = sorted(l["name"] for l in logs["logs"])
    check("logs recovered", names == ["events", "notes"], str(names))
    view = c.call(t="read", id=2, log="notes")
    check(
        "view recovered",
        view["value"]["root"]["title"] == "lug: a little log"
        and view["value"]["root"]["author"]["city"] == "Atlanta",
        json.dumps(view["value"]["root"]),
    )
    c.send(t="subscribe", id=3, log="events", **{"from": 0}, mode="records", credit=100)
    c.recv()
    records = c.recv()
    check("records recovered", [r["version"] for r in records["records"]] == [1, 2, 3, 4])
    ack = c.call(t="append", id=4, log="events", patches=[{"event": "tick", "n": 5}])
    check("appends continue", ack["versions"] == [5], str(ack["versions"]))

    print("\n==> SIGTERM")
    server.send_signal(signal.SIGTERM)
    server.wait(timeout=10)
    check("socket unlinked", not os.path.exists(sock_path))
    log.close()
    print(f"\nall good, scratch at {dirname}")


if __name__ == "__main__":
    sys.exit(main())
