// Starts a real lug-server and hands back everything needed to talk to it.
//
// Nothing here mocks the protocol. The point of the conformance suite is that
// the TypeScript client and the Rust daemon were written apart from each
// other, so only the actual binary can say whether they agree.

import { spawn, spawnSync, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";
import { randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

export interface DaemonOptions {
  // Records held in memory per log for fan-out.
  ring?: number;
  // Segment rotation size in bytes. The daemon refuses anything below 4096.
  segment?: number;
  // Versions between checkpoints. A checkpoint is what reclaims segments.
  checkpointEvery?: number;
}

export interface Daemon {
  directory: string;
  socket: string;
  baseUrl: string;
  token: string;
  // Runs the Rust `lug` CLI against this daemon, for cross-checking what the
  // TypeScript client believes against what the other client prints.
  cli(
    args: string[],
    input?: string,
  ): { status: number | null; stdout: string; stderr: string };
  stop(): Promise<void>;
}

type Daemonised = ChildProcessByStdio<null, Readable, Readable>;

function repoRoot(): string {
  let directory = import.meta.dirname;
  for (;;) {
    if (existsSync(join(directory, "Cargo.toml")) && existsSync(join(directory, "crates"))) {
      return directory;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      throw new Error("no repository root above the test directory");
    }
    directory = parent;
  }
}

function binary(name: string): string {
  return resolve(repoRoot(), "target", "release", name);
}

// Why the daemon cannot be driven, or undefined when it can. Passed straight
// to node:test as a skip reason so an absent build reads as a skip rather
// than a wall of connection failures.
export function missingBinaries(): string | undefined {
  const absent = ["lug-server", "lug"].filter((name) => !existsSync(binary(name)));
  if (absent.length === 0) {
    return undefined;
  }
  return `build them first: cargo build --release (missing ${absent.join(", ")})`;
}

export async function startDaemon(options: DaemonOptions = {}): Promise<Daemon> {
  const directory = await mkdtemp(join(tmpdir(), "lug-interop-"));
  const data = join(directory, "data");
  const run = join(directory, "run");
  const tokenPath = join(directory, "token");
  const configPath = join(directory, "lug.toml");
  const token = randomBytes(24).toString("hex");
  await mkdir(data, { recursive: true });
  await mkdir(run, { recursive: true });
  await writeFile(tokenPath, `${token}\n`, { mode: 0o600 });

  const config = [
    `data = ${JSON.stringify(data)}`,
    `run = ${JSON.stringify(run)}`,
    `socket = "lug.sock"`,
    // Port 0, because a fixed port collides with whatever else is running.
    `http = "127.0.0.1:0"`,
    `token = ${JSON.stringify(tokenPath)}`,
    `segment = ${options.segment ?? 64 * 1024 * 1024}`,
    `ring = ${options.ring ?? 4096}`,
    `checkpoint_every = ${options.checkpointEvery ?? 10_000}`,
    `cores = 1`,
    "",
  ].join("\n");
  await writeFile(configPath, config);

  const child = spawn(binary("lug-server"), ["--config", configPath], {
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, NO_COLOR: "1" },
  });
  const socket = join(run, "lug.sock");
  const baseUrl = await ready(child, directory);

  const daemon: Daemon = {
    directory,
    socket,
    baseUrl,
    token,
    // Over the socket, because the CLI wants --http alongside a token file and
    // the point here is a second opinion on the data, not on the transport.
    cli: (args, input) => runCli(binary("lug"), args, { LUG_SOCKET: socket }, input),
    stop: async () => {
      await stop(child);
      await rm(directory, { recursive: true, force: true });
    },
  };
  return daemon;
}

function runCli(
  command: string,
  args: string[],
  env: Record<string, string>,
  input?: string,
): { status: number | null; stdout: string; stderr: string } {
  const result = spawnSync(command, args, {
    env: { ...process.env, ...env },
    encoding: "utf8",
    timeout: 20_000,
    ...(input === undefined ? {} : { input }),
  });
  return { status: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

// Waits for the daemon's own ready line, which is the only place the bound
// HTTP port appears when the config asked for port 0.
async function ready(
  child: Daemonised,
  directory: string,
): Promise<string> {
  let log = "";
  return new Promise<string>((resolveReady, rejectReady) => {
    const timer = setTimeout(() => {
      cleanup();
      child.kill("SIGKILL");
      rejectReady(new Error(`lug-server did not start in 15s, log:\n${log}`));
    }, 15_000);
    const onData = (chunk: Buffer): void => {
      // The daemon colours its log whether or not stderr is a terminal, and
      // the escapes land between the field name and its value.
      log += chunk.toString("utf8").replace(/\u001b\[[0-9;]*m/g, "");
      const found = /lug-server ready.*http=Some\(([^)]+)\)/.exec(log);
      if (found?.[1] !== undefined) {
        cleanup();
        resolveReady(`http://${found[1]}`);
      }
    };
    const onExit = (code: number | null): void => {
      cleanup();
      rejectReady(new Error(`lug-server exited with ${code} in ${directory}, log:\n${log}`));
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      child.stderr.off("data", onData);
      child.off("exit", onExit);
    };
    child.stderr.on("data", onData);
    child.stdout.on("data", () => undefined);
    child.on("exit", onExit);
  });
}

async function stop(child: Daemonised): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const exited = new Promise<void>((resolveExit) => {
    child.once("exit", () => resolveExit());
  });
  child.kill("SIGTERM");
  const killer = setTimeout(() => child.kill("SIGKILL"), 5_000);
  await exited;
  clearTimeout(killer);
}
