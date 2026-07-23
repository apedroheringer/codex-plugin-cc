import { randomUUID } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { createBrokerEndpoint, parseBrokerEndpoint } from "./broker-endpoint.mjs";
import { withLock } from "./locking.mjs";
import {
  isProcessRunning,
  isProcessTreeRunning,
  processHasLaunchToken,
  terminateProcessTree,
  waitForProcessExit
} from "./process.mjs";
import { resolveStateDir } from "./state.mjs";

export const PID_FILE_ENV = "CODEX_COMPANION_APP_SERVER_PID_FILE";
export const LOG_FILE_ENV = "CODEX_COMPANION_APP_SERVER_LOG_FILE";
const BROKER_STATE_FILE = "broker.json";
const PRIVATE_DIR_MODE = 0o700;
const PRIVATE_FILE_MODE = 0o600;

function setMode(filePath, mode) {
  try {
    fs.chmodSync(filePath, mode);
  } catch {
    // Windows and restrictive filesystems may not implement POSIX modes.
  }
}

function ensurePrivateDir(dir) {
  fs.mkdirSync(dir, { recursive: true, mode: PRIVATE_DIR_MODE });
  setMode(dir, PRIVATE_DIR_MODE);
}

export function createBrokerSessionDir(prefix = "cxc-") {
  const sessionDir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  setMode(sessionDir, PRIVATE_DIR_MODE);
  return sessionDir;
}

function connectToEndpoint(endpoint) {
  const target = parseBrokerEndpoint(endpoint);
  return net.createConnection({ path: target.path });
}

export async function waitForBrokerEndpoint(endpoint, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ready = await new Promise((resolve) => {
      const socket = connectToEndpoint(endpoint);
      let settled = false;
      const finish = (value) => {
        if (settled) {
          return;
        }
        settled = true;
        socket.destroy();
        resolve(value);
      };
      socket.setTimeout(Math.max(1, Math.min(150, deadline - Date.now())), () => finish(false));
      socket.on("connect", () => finish(true));
      socket.on("error", () => finish(false));
    });
    if (ready) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

export async function sendBrokerShutdown(endpoint, options = {}) {
  return await new Promise((resolve) => {
    const socket = connectToEndpoint(endpoint);
    let settled = false;
    let buffer = "";
    const finish = (result = null) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(result);
    };
    socket.setEncoding("utf8");
    socket.setTimeout(options.timeoutMs ?? 2000, () => finish(null));
    socket.on("connect", () => {
      socket.write(
        `${JSON.stringify({
          id: 1,
          method: "broker/shutdown",
          params: { instanceToken: options.instanceToken ?? null }
        })}\n`
      );
    });
    socket.on("data", (chunk) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex === -1) {
        return;
      }
      try {
        const response = JSON.parse(buffer.slice(0, newlineIndex));
        finish({ result: response?.result ?? null, error: response?.error ?? null });
      } catch {
        finish(null);
      }
    });
    socket.on("error", () => finish(null));
    socket.on("close", () => finish(null));
  });
}

function isValidPid(pid) {
  return Number.isSafeInteger(pid) && pid > 0;
}

export function spawnBrokerProcess({
  scriptPath,
  cwd,
  endpoint,
  pidFile,
  logFile,
  instanceToken,
  env = process.env
}) {
  const logFd = fs.openSync(logFile, "a", PRIVATE_FILE_MODE);
  try {
    setMode(logFile, PRIVATE_FILE_MODE);
    const child = spawn(
      process.execPath,
      [
        scriptPath,
        "serve",
        "--endpoint",
        endpoint,
        "--cwd",
        cwd,
        "--pid-file",
        pidFile,
        "--instance-token",
        instanceToken
      ],
      {
        cwd,
        env,
        detached: true,
        stdio: ["ignore", logFd, logFd]
      }
    );
    child.unref();
    return child;
  } finally {
    fs.closeSync(logFd);
  }
}

function resolveBrokerStateFile(cwd) {
  return path.join(resolveStateDir(cwd), BROKER_STATE_FILE);
}

export function loadBrokerSession(cwd) {
  const stateFile = resolveBrokerStateFile(cwd);
  if (!fs.existsSync(stateFile)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(stateFile, "utf8"));
  } catch {
    return null;
  }
}

export function saveBrokerSession(cwd, session) {
  const stateDir = resolveStateDir(cwd);
  ensurePrivateDir(stateDir);
  const stateFile = resolveBrokerStateFile(cwd);
  const tmpFile = `${stateFile}.${process.pid}.${randomUUID()}.tmp`;
  try {
    const fd = fs.openSync(tmpFile, "wx", PRIVATE_FILE_MODE);
    try {
      setMode(tmpFile, PRIVATE_FILE_MODE);
      fs.writeFileSync(fd, `${JSON.stringify(session, null, 2)}\n`, "utf8");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(tmpFile, stateFile);
    setMode(stateFile, PRIVATE_FILE_MODE);
  } catch (error) {
    try {
      fs.unlinkSync(tmpFile);
    } catch {
      // Temp file may not have been created or may already be gone.
    }
    throw error;
  }
}

export function clearBrokerSession(cwd) {
  const stateFile = resolveBrokerStateFile(cwd);
  try {
    fs.unlinkSync(stateFile);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

function resolveBrokerPid(session) {
  const statePid = isValidPid(session.pid) ? session.pid : null;
  let filePid = null;
  if (session.pidFile && fs.existsSync(session.pidFile)) {
    const rawPid = fs.readFileSync(session.pidFile, "utf8").trim();
    if (/^\d+$/.test(rawPid)) {
      const parsedPid = Number(rawPid);
      filePid = isValidPid(parsedPid) ? parsedPid : null;
    }
  }
  if (statePid && filePid && statePid !== filePid) {
    throw new Error(`Codex app-server broker PID mismatch (${statePid} != ${filePid}).`);
  }
  return statePid ?? filePid;
}

function endpointArtifactExists(endpoint) {
  if (!endpoint) {
    return false;
  }
  try {
    const target = parseBrokerEndpoint(endpoint);
    // Named pipes have no filesystem artifact for teardown to unlink.
    return target.kind === "unix" && fs.existsSync(target.path);
  } catch {
    return true;
  }
}

function canDiscardUnownedSession(session, pid) {
  const processExited = !isValidPid(pid) || !isProcessTreeRunning(pid);
  return processExited && !endpointArtifactExists(session.endpoint);
}

/**
 * Resolves false only when nothing can possibly be listening on the endpoint.
 *
 * A refused connection (or a socket path that is already gone) is the one signal
 * that positively rules out a live listener. Every other outcome — including a
 * timeout, which is what a live-but-hung broker produces — resolves true so that
 * callers stay conservative and never unlink a socket that someone else owns.
 */
function endpointAcceptsConnection(endpoint, timeoutMs) {
  // socket.setTimeout(0) disables the timer outright, which would leave this
  // promise pending forever on a connection that never settles.
  const deadlineMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 250;
  return new Promise((resolve) => {
    let socket;
    try {
      socket = connectToEndpoint(endpoint);
    } catch {
      resolve(true);
      return;
    }
    let settled = false;
    const finish = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(value);
    };
    socket.setTimeout(deadlineMs, () => finish(true));
    socket.on("connect", () => finish(true));
    socket.on("error", (error) => {
      const code = error?.code;
      finish(!(code === "ECONNREFUSED" || code === "ENOENT"));
    });
  });
}

/**
 * True when the endpoint path lives inside the session directory this plugin
 * created. Those directories come from mkdtemp with mode 0700, so a path under
 * one is ours by construction — an unrelated process cannot have placed its
 * socket there. Reclaiming is confined to that subtree so a persisted endpoint
 * pointing anywhere else is never unlinked.
 */
function endpointIsInsideSessionDir(session) {
  if (!session.sessionDir || !session.endpoint) {
    return false;
  }
  try {
    const target = parseBrokerEndpoint(session.endpoint);
    if (target.kind !== "unix") {
      return false;
    }
    const sessionDir = path.resolve(session.sessionDir);
    const relative = path.relative(sessionDir, path.resolve(target.path));
    return relative !== "" && !relative.startsWith("..") && !path.isAbsolute(relative);
  } catch {
    return false;
  }
}

/**
 * A stale socket left behind by an owned broker that died before acknowledging
 * shutdown. Ownership cannot be proven by RPC in that case — the process that
 * would answer is gone — so possession is established from independent signals
 * instead, all of which must hold:
 *
 *   1. the socket sits inside the 0700 session directory we created;
 *   2. neither the recorded PID nor its process group is still running;
 *   3. connecting to the endpoint is refused.
 *
 * No single one is sufficient. A hung broker also fails to answer, PID numbers
 * get reused, and a refused connect only proves nothing is listening *right
 * now* — a process that has bound but not yet listened also refuses. Requiring
 * all three keeps the blast radius inside our own temp directory.
 */
async function canReclaimStaleEndpoint(session, pid, options = {}) {
  if (!endpointIsInsideSessionDir(session)) {
    return false;
  }
  // isProcessTreeRunning() checks the process *group* on Linux, so a reused PID
  // in another group reads as dead. Pair it with the plain PID check before
  // treating the owner as gone.
  if (isValidPid(pid) && (isProcessTreeRunning(pid, options) || isProcessRunning(pid, options))) {
    return false;
  }
  return !(await endpointAcceptsConnection(session.endpoint, options.reclaimProbeTimeoutMs));
}

export async function shutdownBrokerSession(cwd, options = {}) {
  const session = options.session ?? loadBrokerSession(cwd);
  if (!session) {
    return { found: false, exited: true, forced: false, reclaimedStaleEndpoint: false };
  }

  const pid = resolveBrokerPid(session);
  if (session.endpoint && !session.instanceToken) {
    if (canDiscardUnownedSession(session, pid)) {
      teardownBrokerSession({
        endpoint: null,
        pidFile: session.pidFile ?? null,
        logFile: session.logFile ?? null,
        sessionDir: session.sessionDir ?? null
      });
      clearBrokerSession(cwd);
      return { found: true, exited: true, forced: false, reclaimedStaleEndpoint: false };
    }
    throw new Error("Codex app-server broker ownership could not be verified; persisted state was preserved.");
  }

  let shutdownResponse = null;
  if (session.endpoint) {
    shutdownResponse = await sendBrokerShutdown(session.endpoint, {
      timeoutMs: options.timeoutMs,
      instanceToken: session.instanceToken
    });
  }
  if (shutdownResponse?.error) {
    throw new Error(
      `Codex app-server broker rejected shutdown identity; persisted state was preserved: ${
        shutdownResponse.error.message ?? "unknown error"
      }`
    );
  }

  const shutdownAck = shutdownResponse?.result ?? null;
  const acknowledgedPid = isValidPid(shutdownAck?.pid) ? shutdownAck.pid : null;
  const ownershipVerified =
    Boolean(session.instanceToken) &&
    shutdownAck?.instanceToken === session.instanceToken &&
    acknowledgedPid !== null &&
    (pid === null || pid === acknowledgedPid);
  if (shutdownAck && !ownershipVerified) {
    throw new Error("Codex app-server broker shutdown identity did not match persisted state.");
  }

  let verifiedPid = ownershipVerified ? acknowledgedPid : null;
  let exited = isValidPid(pid)
    ? await waitForProcessExit(pid, {
        timeoutMs: 0,
        intervalMs: options.intervalMs,
        killImpl: options.killImpl,
        platform: options.platform
      })
    : false;

  if (!shutdownAck && isValidPid(pid) && !exited) {
    const ownsPersistedProcess = options.verifyProcess
      ? options.verifyProcess(pid, session.instanceToken)
      : processHasLaunchToken(pid, session.instanceToken, {
          marker: "--instance-token",
          platform: options.platform,
          timeoutMs: options.timeoutMs,
          runCommandImpl: options.runCommandImpl
        });
    if (!ownsPersistedProcess) {
      if (isProcessTreeRunning(pid, options)) {
        throw new Error("Codex app-server broker ownership could not be verified; persisted state was preserved.");
      }
      exited = true;
    } else {
      verifiedPid = pid;
    }
  }

  if (!shutdownAck && session.instanceToken && !isValidPid(pid)) {
    throw new Error("Codex app-server broker PID is unavailable; persisted ownership state was preserved.");
  }

  if (!exited && isValidPid(verifiedPid)) {
    exited = await waitForProcessExit(verifiedPid, {
      timeoutMs: options.timeoutMs,
      intervalMs: options.intervalMs,
      killImpl: options.killImpl,
      platform: options.platform
    });
  }

  let forced = false;
  if (!exited && isValidPid(verifiedPid) && options.killProcess) {
    const stillOwnsProcess = options.verifyProcess
      ? options.verifyProcess(verifiedPid, session.instanceToken)
      : processHasLaunchToken(verifiedPid, session.instanceToken, {
          marker: "--instance-token",
          platform: options.platform,
          timeoutMs: options.timeoutMs,
          runCommandImpl: options.runCommandImpl
        });
    if (!stillOwnsProcess) {
      throw new Error("Codex app-server broker process ownership changed before forced shutdown.");
    }
    options.killProcess(verifiedPid);
    forced = true;
    exited = await waitForProcessExit(verifiedPid, {
      timeoutMs: options.timeoutMs,
      intervalMs: options.intervalMs,
      killImpl: options.killImpl,
      platform: options.platform
    });
  }

  if (!exited) {
    throw new Error(`Codex app-server broker ${verifiedPid ?? session.endpoint ?? "unknown"} did not exit.`);
  }
  // A broker that is killed after binding its socket can never send a shutdown
  // ack, so ownershipVerified stays false while the socket file survives. Left
  // fatal, that single stale socket wedges every later command in the workspace,
  // because ensureBrokerSession() shuts the old session down before starting a
  // replacement. Reclaim it when it is provably dead instead of throwing.
  let reclaimedStaleEndpoint = false;
  if (!ownershipVerified && endpointArtifactExists(session.endpoint)) {
    reclaimedStaleEndpoint = await canReclaimStaleEndpoint(session, pid, options);
    if (!reclaimedStaleEndpoint) {
      throw new Error("Codex app-server broker endpoint ownership could not be verified; persisted state was preserved.");
    }
  }

  const endpointIsOurs = ownershipVerified || reclaimedStaleEndpoint;
  teardownBrokerSession({
    endpoint: endpointIsOurs ? session.endpoint ?? null : null,
    pidFile: session.pidFile ?? null,
    logFile: session.logFile ?? null,
    sessionDir: session.sessionDir ?? null,
    ownershipVerified: endpointIsOurs
  });
  clearBrokerSession(cwd);
  return { found: true, exited: true, forced, reclaimedStaleEndpoint };
}

async function isBrokerEndpointReady(endpoint) {
  if (!endpoint) {
    return false;
  }
  try {
    return await waitForBrokerEndpoint(endpoint, 150);
  } catch {
    return false;
  }
}

export async function ensureBrokerSession(cwd, options = {}) {
  const stateDir = resolveStateDir(cwd);
  ensurePrivateDir(stateDir);
  return withLock(
    path.join(stateDir, ".broker.lock"),
    () => ensureBrokerSessionLocked(cwd, options),
    { timeoutMs: options.lockTimeoutMs ?? 10000 }
  );
}

async function ensureBrokerSessionLocked(cwd, options = {}) {
  const existing = loadBrokerSession(cwd);
  if (existing && (await isBrokerEndpointReady(existing.endpoint))) {
    return existing;
  }

  if (existing) {
    await shutdownBrokerSession(cwd, {
      session: existing,
      killProcess: options.killProcess ?? terminateProcessTree,
      verifyProcess: options.verifyProcess,
      runCommandImpl: options.runCommandImpl,
      killImpl: options.killImpl,
      platform: options.platform,
      timeoutMs: options.timeoutMs,
      intervalMs: options.intervalMs
    });
  }

  const sessionDir = createBrokerSessionDir();
  const endpointFactory = options.createBrokerEndpoint ?? createBrokerEndpoint;
  const endpoint = endpointFactory(sessionDir, options.platform);
  const pidFile = path.join(sessionDir, "broker.pid");
  const logFile = path.join(sessionDir, "broker.log");
  const scriptPath =
    options.scriptPath ?? fileURLToPath(new URL("../app-server-broker.mjs", import.meta.url));
  const instanceToken = options.instanceToken ?? randomUUID();

  const child = spawnBrokerProcess({
    scriptPath,
    cwd,
    endpoint,
    pidFile,
    logFile,
    instanceToken,
    env: options.env ?? process.env
  });

  const session = {
    endpoint,
    pidFile,
    logFile,
    sessionDir,
    pid: child.pid ?? null,
    instanceToken
  };
  saveBrokerSession(cwd, session);

  const ready = await waitForBrokerEndpoint(endpoint, options.timeoutMs ?? 2000);
  if (!ready) {
    await shutdownBrokerSession(cwd, {
      session,
      killProcess: options.killProcess ?? terminateProcessTree,
      verifyProcess: options.verifyProcess,
      runCommandImpl: options.runCommandImpl,
      killImpl: options.killImpl,
      platform: options.platform,
      timeoutMs: options.timeoutMs,
      intervalMs: options.intervalMs
    });
    return null;
  }

  return session;
}

export function teardownBrokerSession({
  endpoint = null,
  pidFile,
  logFile,
  sessionDir = null,
  ownershipVerified = false
}) {
  if (endpoint && !ownershipVerified) {
    throw new Error("Refusing to remove an unverified broker endpoint.");
  }

  for (const filePath of [pidFile, logFile]) {
    if (!filePath) {
      continue;
    }
    try {
      fs.unlinkSync(filePath);
    } catch (error) {
      if (error?.code !== "ENOENT") {
        throw error;
      }
    }
  }

  if (endpoint) {
    const target = parseBrokerEndpoint(endpoint);
    if (target.kind === "unix") {
      try {
        fs.unlinkSync(target.path);
      } catch (error) {
        if (error?.code !== "ENOENT") {
          throw error;
        }
      }
    }
  }

  const resolvedSessionDir =
    sessionDir ?? (pidFile ? path.dirname(pidFile) : logFile ? path.dirname(logFile) : null);
  if (resolvedSessionDir) {
    try {
      fs.rmdirSync(resolvedSessionDir);
    } catch (error) {
      if (error?.code !== "ENOENT" && error?.code !== "ENOTEMPTY") {
        throw error;
      }
    }
  }
}
