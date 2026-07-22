import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";
import { after } from "node:test";

import { loadBrokerSession, sendBrokerShutdown, teardownBrokerSession } from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import { terminateProcessTree } from "../plugins/codex/scripts/lib/process.mjs";

const trackedTempDirs = [];

export function makeTempDir(prefix = "codex-plugin-test-") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  trackedTempDirs.push(dir);
  return dir;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function pidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function readBrokerPid(session) {
  if (Number.isFinite(session.pid)) {
    return session.pid;
  }
  try {
    const parsed = Number.parseInt(fs.readFileSync(session.pidFile, "utf8").trim(), 10);
    return Number.isFinite(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

// Global teardown: any broker a test started (directly or lazily) and did not
// tear down is shut down here, so the suite never leaves broker/app-server
// processes behind — even when a test fails or returns early.
after(async () => {
  const leakedPids = [];
  const leakedSessions = [];
  for (const dir of trackedTempDirs) {
    const session = loadBrokerSession(dir);
    if (!session) {
      continue;
    }
    leakedSessions.push(session);
    if (session.endpoint) {
      await sendBrokerShutdown(session.endpoint);
    }
    const pid = readBrokerPid(session);
    if (Number.isFinite(pid)) {
      leakedPids.push(pid);
    }
  }

  const deadline = Date.now() + 3000;
  for (const pid of leakedPids) {
    while (pidAlive(pid) && Date.now() < deadline) {
      await sleep(50);
    }
    if (pidAlive(pid)) {
      terminateProcessTree(pid);
      await sleep(200);
    }
    if (pidAlive(pid)) {
      throw new Error(`Leaked broker process ${pid} survived suite teardown.`);
    }
  }

  for (const session of leakedSessions) {
    teardownBrokerSession({
      endpoint: session.endpoint ?? null,
      pidFile: session.pidFile ?? null,
      logFile: session.logFile ?? null,
      sessionDir: session.sessionDir ?? null
    });
  }
});

export function writeExecutable(filePath, source) {
  fs.writeFileSync(filePath, source, { encoding: "utf8", mode: 0o755 });
}

export function run(command, args, options = {}) {
  return spawnSync(command, args, {
    cwd: options.cwd,
    env: options.env,
    encoding: "utf8",
    input: options.input,
    shell: options.shell ?? (process.platform === "win32" && !path.isAbsolute(command)),
    windowsHide: true
  });
}

export function initGitRepo(cwd) {
  run("git", ["init", "-b", "main"], { cwd });
  run("git", ["config", "user.name", "Codex Plugin Tests"], { cwd });
  run("git", ["config", "user.email", "tests@example.com"], { cwd });
  run("git", ["config", "commit.gpgsign", "false"], { cwd });
  run("git", ["config", "tag.gpgsign", "false"], { cwd });
}
