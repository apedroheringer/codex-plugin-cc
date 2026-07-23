import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import process from "node:process";
import test from "node:test";

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
