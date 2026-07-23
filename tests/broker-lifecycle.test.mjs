import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import process from "node:process";
import test from "node:test";
import { spawn } from "node:child_process";

import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { makeTempDir } from "./helpers.mjs";
import {
  ensureBrokerSession,
  loadBrokerSession,
  saveBrokerSession,
  sendBrokerShutdown,
  shutdownBrokerSession
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import {
  isProcessTreeRunning,
  terminateProcessTree
} from "../plugins/codex/scripts/lib/process.mjs";
import { resolveStateDir } from "../plugins/codex/scripts/lib/state.mjs";

test("concurrent ensureBrokerSession calls share a single authenticated broker", async () => {
  const workspace = makeTempDir();
  const binDir = makeTempDir();
  installFakeCodex(binDir);
  const env = buildEnv(binDir);

  const [left, right] = await Promise.all([
    ensureBrokerSession(workspace, { env }),
    ensureBrokerSession(workspace, { env })
  ]);

  assert.ok(left, "first ensureBrokerSession returned no session");
  assert.ok(right, "second ensureBrokerSession returned no session");
  assert.equal(left.endpoint, right.endpoint);
  assert.equal(left.pid, right.pid);
  assert.equal(left.instanceToken, right.instanceToken);

  const persisted = loadBrokerSession(workspace);
  assert.ok(persisted);
  assert.equal(persisted.endpoint, left.endpoint);
  assert.equal(persisted.instanceToken, left.instanceToken);

  const outcome = await shutdownBrokerSession(workspace, {
    session: left,
    killProcess: terminateProcessTree
  });
  assert.equal(outcome.exited, true);
  assert.equal(loadBrokerSession(workspace), null);
});

test("broker rejects a shutdown token that does not identify its instance", async (t) => {
  const workspace = makeTempDir();
  const binDir = makeTempDir();
  installFakeCodex(binDir);
  const session = await ensureBrokerSession(workspace, { env: buildEnv(binDir) });
  assert.ok(session?.pid);

  t.after(async () => {
    if (loadBrokerSession(workspace)) {
      await shutdownBrokerSession(workspace, {
        session,
        killProcess: terminateProcessTree
      });
    }
  });

  const response = await sendBrokerShutdown(session.endpoint, {
    instanceToken: "wrong-instance-token",
    timeoutMs: 500
  });

  assert.match(response?.error?.message ?? "", /identity did not match/i);
  assert.equal(isProcessTreeRunning(session.pid), true);
  assert.ok(loadBrokerSession(workspace));
});

test("shutdown request stops waiting at its deadline", { skip: process.platform === "win32" }, async (t) => {
  const sessionDir = makeTempDir("cxc-unresponsive-");
  const socketPath = path.join(sessionDir, "broker.sock");
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("data", () => {
      // Deliberately never answer.
    });
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    for (const socket of sockets) {
      socket.destroy();
    }
    await new Promise((resolve) => server.close(resolve));
  });

  const startedAt = Date.now();
  const response = await sendBrokerShutdown(`unix:${socketPath}`, {
    instanceToken: "instance-token-1234567890",
    timeoutMs: 40
  });

  assert.equal(response, null);
  assert.ok(Date.now() - startedAt < 500, "shutdown request exceeded its deadline");
  assert.equal(fs.existsSync(socketPath), true);
});

test("shutdown preserves an unowned endpoint and its persisted state", { skip: process.platform === "win32" }, async (t) => {
  const workspace = makeTempDir();
  const sessionDir = makeTempDir("cxc-unowned-");
  const socketPath = path.join(sessionDir, "fake-broker.sock");
  const server = net.createServer();
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    if (fs.existsSync(socketPath)) {
      fs.unlinkSync(socketPath);
    }
  });

  const session = {
    endpoint: `unix:${socketPath}`,
    pid: null,
    pidFile: null,
    logFile: null,
    sessionDir
  };
  saveBrokerSession(workspace, session);

  await assert.rejects(
    shutdownBrokerSession(workspace, {
      session,
      timeoutMs: 40,
      killProcess: terminateProcessTree
    }),
    /ownership could not be verified/i
  );

  assert.equal(fs.existsSync(socketPath), true);
  assert.deepEqual(loadBrokerSession(workspace), session);
});

test("broker metadata, pid, and log files are private", { skip: process.platform === "win32" }, async () => {
  const workspace = makeTempDir();
  const binDir = makeTempDir();
  installFakeCodex(binDir);
  const session = await ensureBrokerSession(workspace, { env: buildEnv(binDir) });
  assert.ok(session);

  const persistedStateFile = path.join(resolveStateDir(workspace), "broker.json");

  assert.equal(fs.statSync(session.sessionDir).mode & 0o777, 0o700);
  assert.equal(fs.statSync(session.pidFile).mode & 0o777, 0o600);
  assert.equal(fs.statSync(session.logFile).mode & 0o777, 0o600);
  assert.equal(fs.statSync(persistedStateFile).mode & 0o777, 0o600);

  await shutdownBrokerSession(workspace, {
    session,
    killProcess: terminateProcessTree
  });
});

async function spawnSocketHolder(socketPath) {
  const child = spawn(
    process.execPath,
    [
      "-e",
      `const net = require("node:net");
       const server = net.createServer();
       server.listen(${JSON.stringify(socketPath)}, () => process.stdout.write("ready\\n"));
       setInterval(() => {}, 60000);`
    ],
    { stdio: ["ignore", "pipe", "ignore"] }
  );
  await new Promise((resolve, reject) => {
    child.stdout.on("data", (chunk) => {
      if (String(chunk).includes("ready")) {
        resolve();
      }
    });
    child.once("error", reject);
    child.once("exit", () => reject(new Error("socket holder exited before binding")));
  });
  return child;
}

test("shutdown reclaims the socket of an owned broker that died without acking", { skip: process.platform === "win32" }, async () => {
  const workspace = makeTempDir();
  const sessionDir = makeTempDir("cxc-stale-");
  const socketPath = path.join(sessionDir, "broker.sock");

  // Killing the holder with SIGKILL skips its cleanup, so the socket file stays
  // on disk with nothing listening — exactly what a crashed broker leaves behind.
  const holder = await spawnSocketHolder(socketPath);
  const deadPid = holder.pid;
  holder.kill("SIGKILL");
  await new Promise((resolve) => holder.once("exit", resolve));
  assert.equal(fs.existsSync(socketPath), true, "stale socket should survive the kill");

  const session = {
    endpoint: `unix:${socketPath}`,
    pid: deadPid,
    pidFile: null,
    logFile: null,
    sessionDir,
    instanceToken: "stale-instance-token"
  };
  saveBrokerSession(workspace, session);

  const outcome = await shutdownBrokerSession(workspace, {
    session,
    timeoutMs: 40,
    killProcess: terminateProcessTree
  });

  assert.equal(outcome.exited, true);
  assert.equal(outcome.reclaimedStaleEndpoint, true);
  assert.equal(fs.existsSync(socketPath), false, "stale socket should be unlinked");
  assert.equal(loadBrokerSession(workspace), null);
});

test("ensureBrokerSession recovers from a stale socket instead of failing", { skip: process.platform === "win32" }, async (t) => {
  const workspace = makeTempDir();
  const sessionDir = makeTempDir("cxc-stale-ensure-");
  const socketPath = path.join(sessionDir, "broker.sock");
  const binDir = makeTempDir();
  installFakeCodex(binDir);

  const holder = await spawnSocketHolder(socketPath);
  const deadPid = holder.pid;
  holder.kill("SIGKILL");
  await new Promise((resolve) => holder.once("exit", resolve));

  saveBrokerSession(workspace, {
    endpoint: `unix:${socketPath}`,
    pid: deadPid,
    pidFile: null,
    logFile: null,
    sessionDir,
    instanceToken: "stale-instance-token"
  });

  const session = await ensureBrokerSession(workspace, { env: buildEnv(binDir) });
  t.after(async () => {
    if (session) {
      await shutdownBrokerSession(workspace, { session, killProcess: terminateProcessTree });
    }
  });

  assert.ok(session, "a stale socket must not block the replacement broker");
  assert.notEqual(session.endpoint, `unix:${socketPath}`);
});

test("shutdown still refuses to unlink an endpoint someone is listening on", { skip: process.platform === "win32" }, async (t) => {
  const workspace = makeTempDir();
  const sessionDir = makeTempDir("cxc-live-listener-");
  const socketPath = path.join(sessionDir, "broker.sock");

  // Dead PID, but a live listener owns the path: reclaiming would delete the
  // socket of an unrelated process, so the shutdown must keep failing.
  const holder = await spawnSocketHolder(socketPath);
  const stalePid = holder.pid;
  holder.kill("SIGKILL");
  await new Promise((resolve) => holder.once("exit", resolve));
  if (fs.existsSync(socketPath)) {
    fs.unlinkSync(socketPath);
  }
  // A listener that accepts but never answers — the shape of a hung broker. Its
  // sockets must be tracked: server.close() waits for every accepted connection
  // to end, and the shutdown request leaves one open.
  const accepted = new Set();
  const server = net.createServer((socket) => {
    accepted.add(socket);
    socket.on("close", () => accepted.delete(socket));
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    for (const socket of accepted) {
      socket.destroy();
    }
    await new Promise((resolve) => server.close(resolve));
    if (fs.existsSync(socketPath)) {
      fs.unlinkSync(socketPath);
    }
  });

  const session = {
    endpoint: `unix:${socketPath}`,
    pid: stalePid,
    pidFile: null,
    logFile: null,
    sessionDir,
    instanceToken: "stale-instance-token"
  };
  saveBrokerSession(workspace, session);

  await assert.rejects(
    shutdownBrokerSession(workspace, {
      session,
      timeoutMs: 40,
      killProcess: terminateProcessTree
    }),
    /ownership could not be verified/i
  );

  assert.equal(fs.existsSync(socketPath), true);
  assert.deepEqual(loadBrokerSession(workspace), session);
});

test("a refused endpoint alone does not justify reclaiming while the owner lives", { skip: process.platform === "win32" }, async (t) => {
  const workspace = makeTempDir();
  const sessionDir = makeTempDir("cxc-live-owner-");
  const socketPath = path.join(sessionDir, "broker.sock");

  // Live owner, refused endpoint: the socket file exists but nothing listens on
  // it, which on its own looks exactly like the stale case. The recorded PID is
  // still running, so the shutdown must not unlink anything.
  const holder = await spawnSocketHolder(socketPath);
  t.after(() => {
    holder.kill("SIGKILL");
  });
  fs.unlinkSync(socketPath);
  fs.writeFileSync(socketPath, "");

  const session = {
    endpoint: `unix:${socketPath}`,
    pid: holder.pid,
    pidFile: null,
    logFile: null,
    sessionDir,
    instanceToken: "live-owner-token"
  };
  saveBrokerSession(workspace, session);

  await assert.rejects(
    shutdownBrokerSession(workspace, {
      session,
      timeoutMs: 40,
      killProcess: () => {}
    }),
    /ownership could not be verified|did not exit/i
  );

  assert.equal(fs.existsSync(socketPath), true);
  assert.deepEqual(loadBrokerSession(workspace), session);
});

test("an endpoint outside the plugin session directory is never reclaimed", { skip: process.platform === "win32" }, async () => {
  const workspace = makeTempDir();
  const sessionDir = makeTempDir("cxc-outside-");
  const elsewhere = makeTempDir("not-a-session-dir-");
  const socketPath = path.join(elsewhere, "broker.sock");

  const holder = await spawnSocketHolder(socketPath);
  const deadPid = holder.pid;
  holder.kill("SIGKILL");
  await new Promise((resolve) => holder.once("exit", resolve));
  assert.equal(fs.existsSync(socketPath), true);

  // Everything else looks reclaimable — dead PID, refused connect — but the
  // endpoint does not live under the session directory we created.
  const session = {
    endpoint: `unix:${socketPath}`,
    pid: deadPid,
    pidFile: null,
    logFile: null,
    sessionDir,
    instanceToken: "outside-token"
  };
  saveBrokerSession(workspace, session);

  await assert.rejects(
    shutdownBrokerSession(workspace, {
      session,
      timeoutMs: 40,
      killProcess: terminateProcessTree
    }),
    /ownership could not be verified/i
  );

  assert.equal(fs.existsSync(socketPath), true, "foreign socket must survive");
  fs.unlinkSync(socketPath);
});
