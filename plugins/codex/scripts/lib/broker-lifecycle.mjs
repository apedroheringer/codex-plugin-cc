import { randomUUID } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { createBrokerEndpoint, parseBrokerEndpoint } from "./broker-endpoint.mjs";
import {
  ensurePrivateDir,
  PRIVATE_DIR_MODE,
  PRIVATE_FILE_MODE,
  removeFileIfExists,
  setMode,
  writeJsonFileAtomic
} from "./fs.mjs";
import { withLock } from "./locking.mjs";
import {
  isProcessRunning,
  isProcessTreeRunning,
  isValidPid,
  processHasLaunchSequence,
  processHasLaunchToken,
  terminateProcessTree,
  waitForProcessExit
} from "./process.mjs";
import { resolveStateDir } from "./state.mjs";

const BROKER_STATE_FILE = "broker.json";

function createBrokerSessionDir(prefix = "cxc-") {
  const sessionDir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  setMode(sessionDir, PRIVATE_DIR_MODE);
  return sessionDir;
}

function connectToEndpoint(endpoint) {
  const target = parseBrokerEndpoint(endpoint);
  return net.createConnection({ path: target.path });
}

/**
 * One-shot connection probe. Resolves "connect" when something accepted the
 * connection, "timeout" when nothing settled within timeoutMs, "invalid" when
 * the endpoint cannot even be parsed, and the error code otherwise.
 */
function probeEndpoint(endpoint, timeoutMs) {
  return new Promise((resolve) => {
    let socket;
    try {
      socket = connectToEndpoint(endpoint);
    } catch {
      resolve("invalid");
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
    // socket.setTimeout(0) disables the timer outright, which would leave this
    // promise pending forever on a connection that never settles.
    socket.setTimeout(Math.max(1, timeoutMs), () => finish("timeout"));
    socket.on("connect", () => finish("connect"));
    socket.on("error", (error) => finish(error?.code ?? "error"));
  });
}

async function waitForBrokerEndpoint(endpoint, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const probe = await probeEndpoint(endpoint, Math.min(150, deadline - Date.now()));
    if (probe === "connect") {
      return true;
    }
    if (probe === "invalid") {
      return false;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

export function sendBrokerShutdown(endpoint, options = {}) {
  const timeoutMs = Number.isFinite(options.timeoutMs)
    ? Math.max(1, options.timeoutMs)
    : 2000;
  return new Promise((resolve) => {
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
    socket.setTimeout(timeoutMs, () => finish(null));
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

function spawnBrokerProcess({
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
  try {
    return JSON.parse(fs.readFileSync(resolveBrokerStateFile(cwd), "utf8"));
  } catch {
    return null;
  }
}

export function saveBrokerSession(cwd, session) {
  const stateDir = resolveStateDir(cwd);
  ensurePrivateDir(stateDir);
  writeJsonFileAtomic(resolveBrokerStateFile(cwd), session);
}

function clearBrokerSession(cwd) {
  removeFileIfExists(resolveBrokerStateFile(cwd));
}

function resolveBrokerPid(session) {
  const statePid = isValidPid(session.pid) ? session.pid : null;
  let filePid = null;
  let rawPid = null;
  if (session.pidFile) {
    try {
      rawPid = fs.readFileSync(session.pidFile, "utf8").trim();
    } catch {
      // A pid file that vanished or cannot be read is treated as absent.
    }
  }
  if (rawPid !== null && /^\d+$/.test(rawPid)) {
    const parsedPid = Number(rawPid);
    filePid = isValidPid(parsedPid) ? parsedPid : null;
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

function canDiscardUnownedSession(session, pid, options = {}) {
  const processExited = !isValidPid(pid) || !isProcessTreeRunning(pid, options);
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
async function endpointAcceptsConnection(endpoint, timeoutMs) {
  const deadlineMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 250;
  const probe = await probeEndpoint(endpoint, deadlineMs);
  return probe !== "ECONNREFUSED" && probe !== "ENOENT";
}

function resolveOwnedSessionDir(sessionDir) {
  if (typeof sessionDir !== "string") {
    return null;
  }
  const resolved = path.resolve(sessionDir);
  const relative = path.relative(path.resolve(os.tmpdir()), resolved);
  if (path.dirname(relative) !== "." || !path.basename(relative).startsWith("cxc-")) {
    return null;
  }
  try {
    const stats = fs.lstatSync(resolved);
    return stats.isDirectory() && !stats.isSymbolicLink() ? resolved : null;
  } catch {
    return null;
  }
}

function endpointBelongsToSession(session, platform = process.platform) {
  const sessionDir = resolveOwnedSessionDir(session.sessionDir);
  if (!sessionDir || !session.endpoint) {
    return false;
  }
  try {
    return session.endpoint === createBrokerEndpoint(sessionDir, platform);
  } catch {
    return false;
  }
}

function processMatchesLegacyBroker(session, pid, options = {}) {
  const platform = options.platform ?? process.platform;
  let expectedEndpoint;
  try {
    expectedEndpoint = createBrokerEndpoint(session.sessionDir, platform);
  } catch {
    return false;
  }
  if (
    !isValidPid(pid) ||
    session.endpoint !== expectedEndpoint ||
    typeof session.pidFile !== "string" ||
    session.pidFile !== path.join(session.sessionDir, "broker.pid")
  ) {
    return false;
  }
  const probeOptions = {
    platform,
    timeoutMs: options.timeoutMs,
    runCommandImpl: options.runCommandImpl
  };
  // The broker's original --cwd argument is not persisted, and the current
  // invocation may address the same workspace through a different path, so
  // ownership is proven by the launch artifacts unique to this session: its
  // endpoint and its pid file inside the mkdtemp session directory.
  return (
    processHasLaunchSequence(pid, ["serve", "--endpoint", session.endpoint], probeOptions) &&
    processHasLaunchSequence(pid, ["--pid-file", session.pidFile], probeOptions)
  );
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
  if (!endpointBelongsToSession(session, options.platform)) {
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

function processMatchesInstanceToken(pid, instanceToken, options) {
  return processHasLaunchToken(pid, instanceToken, { ...options, marker: "--instance-token" });
}

// Legacy sessions have no instance token, so their processes are re-verified
// by launch artifacts; tokened sessions are re-verified by the token.
function ownsBrokerProcess(session, pid, legacySession, options) {
  return legacySession
    ? processMatchesLegacyBroker(session, pid, options)
    : processMatchesInstanceToken(pid, session.instanceToken, options);
}

export async function shutdownBrokerSession(cwd, options = {}) {
  return withBrokerLock(cwd, options, () => shutdownBrokerSessionLocked(cwd, options));
}

async function shutdownBrokerSessionLocked(cwd, options = {}) {
  const session = loadBrokerSession(cwd);
  if (!session) {
    return { found: false, exited: true, forced: false, reclaimedStaleEndpoint: false };
  }

  const pid = resolveBrokerPid(session);
  const legacySession = Boolean(session.endpoint && !session.instanceToken);
  let legacyProcessVerified = false;
  if (legacySession) {
    if (canDiscardUnownedSession(session, pid, options)) {
      teardownAndClear(cwd, session, false);
      return { found: true, exited: true, forced: false, reclaimedStaleEndpoint: false };
    }
    legacyProcessVerified = processMatchesLegacyBroker(session, pid, options);
    const legacyEndpointIsSafelyStale =
      isValidPid(pid) && (await canReclaimStaleEndpoint(session, pid, options));
    if (!legacyProcessVerified && !legacyEndpointIsSafelyStale) {
      throw new Error("Codex app-server broker ownership could not be verified; persisted state was preserved.");
    }
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
    (Boolean(session.instanceToken) &&
      shutdownAck?.instanceToken === session.instanceToken &&
      acknowledgedPid !== null &&
      (pid === null || pid === acknowledgedPid)) ||
    (legacySession && legacyProcessVerified && Boolean(shutdownAck));
  if (shutdownAck && !ownershipVerified) {
    throw new Error("Codex app-server broker shutdown identity did not match persisted state.");
  }

  let verifiedPid = ownershipVerified ? acknowledgedPid ?? pid : null;
  let exited = isValidPid(pid) ? await waitForProcessExit(pid, { ...options, timeoutMs: 0 }) : false;

  let processOwnershipProven = false;
  if (!shutdownAck && isValidPid(pid) && !exited) {
    const ownsPersistedProcess = ownsBrokerProcess(session, pid, legacySession, options);
    if (!ownsPersistedProcess) {
      if (isProcessTreeRunning(pid, options)) {
        throw new Error("Codex app-server broker ownership could not be verified; persisted state was preserved.");
      }
      exited = true;
    } else {
      verifiedPid = pid;
      processOwnershipProven = true;
    }
  }

  if (!shutdownAck && session.instanceToken && !isValidPid(pid)) {
    throw new Error("Codex app-server broker PID is unavailable; persisted ownership state was preserved.");
  }

  if (!exited && isValidPid(verifiedPid)) {
    exited = await waitForProcessExit(verifiedPid, options);
  }

  let forced = false;
  if (!exited && isValidPid(verifiedPid) && options.killProcess) {
    const stillOwnsProcess = ownsBrokerProcess(session, verifiedPid, legacySession, options);
    if (!stillOwnsProcess) {
      throw new Error("Codex app-server broker process ownership changed before forced shutdown.");
    }
    processOwnershipProven = true;
    options.killProcess(verifiedPid);
    forced = true;
    exited = await waitForProcessExit(verifiedPid, options);
  }

  if (!exited) {
    throw new Error(`Codex app-server broker ${verifiedPid ?? session.endpoint ?? "unknown"} did not exit.`);
  }
  // A broker that is killed after binding its socket can never send a shutdown
  // ack, so ownershipVerified stays false while the socket file survives. Left
  // fatal, that single stale socket wedges every later command in the workspace,
  // because ensureBrokerSession() shuts the old session down before starting a
  // replacement. Ownership proven against the live process (token or launch
  // artifacts) already covers the endpoint; only an unproven leftover needs the
  // connection probe, which can race the kernel right after a forced kill.
  const endpointProven = ownershipVerified || processOwnershipProven;
  let reclaimedStaleEndpoint = false;
  if (!endpointProven && endpointArtifactExists(session.endpoint)) {
    reclaimedStaleEndpoint = await canReclaimStaleEndpoint(session, pid, options);
    if (!reclaimedStaleEndpoint) {
      throw new Error("Codex app-server broker endpoint ownership could not be verified; persisted state was preserved.");
    }
  }

  const endpointIsOurs = endpointProven || reclaimedStaleEndpoint;
  teardownAndClear(cwd, session, endpointIsOurs);
  return { found: true, exited: true, forced, reclaimedStaleEndpoint };
}

export async function ensureBrokerSession(cwd, options = {}) {
  return withBrokerLock(cwd, options, () => ensureBrokerSessionLocked(cwd, options));
}

function withBrokerLock(cwd, options, action) {
  const stateDir = resolveStateDir(cwd);
  ensurePrivateDir(stateDir);
  return withLock(
    path.join(stateDir, ".broker.lock"),
    action,
    { timeoutMs: options.lockTimeoutMs ?? 10000 }
  );
}

async function ensureBrokerSessionLocked(cwd, options = {}) {
  const shutdownOptions = {
    ...options,
    killProcess: options.killProcess ?? terminateProcessTree
  };
  const existing = loadBrokerSession(cwd);
  if (existing?.endpoint && (await waitForBrokerEndpoint(existing.endpoint, 150))) {
    return existing;
  }

  if (existing) {
    await shutdownBrokerSessionLocked(cwd, shutdownOptions);
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
    await shutdownBrokerSessionLocked(cwd, shutdownOptions);
    return null;
  }

  return session;
}

function teardownAndClear(cwd, session, endpointIsOurs) {
  teardownBrokerSession({
    endpoint: endpointIsOurs ? session.endpoint ?? null : null,
    pidFile: session.pidFile ?? null,
    logFile: session.logFile ?? null,
    sessionDir: session.sessionDir ?? null,
    ownershipVerified: endpointIsOurs
  });
  clearBrokerSession(cwd);
}

function teardownBrokerSession({
  endpoint = null,
  pidFile,
  logFile,
  sessionDir = null,
  ownershipVerified = false
}) {
  if (endpoint && !ownershipVerified) {
    throw new Error("Refusing to remove an unverified broker endpoint.");
  }

  const ownedSessionDir = resolveOwnedSessionDir(sessionDir);
  const files = [];
  if (ownedSessionDir && pidFile === path.join(ownedSessionDir, "broker.pid")) {
    files.push(pidFile);
  }
  if (ownedSessionDir && logFile === path.join(ownedSessionDir, "broker.log")) {
    files.push(logFile);
  }
  if (ownedSessionDir && endpoint === createBrokerEndpoint(ownedSessionDir)) {
    const target = parseBrokerEndpoint(endpoint);
    if (target.kind === "unix") {
      files.push(target.path);
    }
  }

  for (const filePath of files) {
    removeFileIfExists(filePath);
  }

  if (ownedSessionDir) {
    try {
      fs.rmdirSync(ownedSessionDir);
    } catch (error) {
      if (error?.code !== "ENOENT" && error?.code !== "ENOTEMPTY") {
        throw error;
      }
    }
  }
}
