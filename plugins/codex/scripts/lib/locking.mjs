import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

import { getProcessIdentity, isProcessRunning } from "./process.mjs";

const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_STALE_MS = 30000;
const RETRY_DELAY_MS = 25;
const OWNER_FILE_PREFIX = "owner-";
const OWNER_FILE_SUFFIX = ".json";

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ownerFileName(token) {
  return `${OWNER_FILE_PREFIX}${token}${OWNER_FILE_SUFFIX}`;
}

function writePrivateJson(filePath, value) {
  const fd = fs.openSync(filePath, "wx", 0o600);
  try {
    fs.writeFileSync(fd, `${JSON.stringify(value)}\n`, "utf8");
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

function removeCandidate(handle) {
  try {
    fs.unlinkSync(handle.candidateOwnerFile);
  } catch {
    // Candidate may already have been published or cleaned up.
  }
  try {
    fs.rmdirSync(handle.candidateDir);
  } catch {
    // Candidate may already have been published or cleaned up.
  }
}

function tryAcquire(lockDir) {
  const token = randomUUID();
  const candidateDir = `${lockDir}.candidate-${token}`;
  const candidateOwnerFile = path.join(candidateDir, ownerFileName(token));
  const handle = {
    lockDir,
    token,
    ownerFile: path.join(lockDir, ownerFileName(token)),
    candidateDir,
    candidateOwnerFile
  };

  fs.mkdirSync(candidateDir, { mode: 0o700 });
  try {
    writePrivateJson(candidateOwnerFile, {
      pid: process.pid,
      token,
      createdAt: Date.now(),
      processIdentity: getProcessIdentity(process.pid)
    });
    fs.renameSync(candidateDir, lockDir);
    return handle;
  } catch (error) {
    removeCandidate(handle);
    const targetExists = fs.existsSync(lockDir);
    if (
      error?.code === "EEXIST" ||
      error?.code === "ENOTEMPTY" ||
      (error?.code === "EPERM" && targetExists)
    ) {
      return null;
    }
    throw error;
  }
}

function readLockState(lockDir) {
  let entries;
  let ageMs;
  try {
    entries = fs.readdirSync(lockDir);
    ageMs = Date.now() - fs.statSync(lockDir).mtimeMs;
  } catch {
    return { kind: "missing" };
  }

  const ownerFiles = entries.filter(
    (entry) => entry.startsWith(OWNER_FILE_PREFIX) && entry.endsWith(OWNER_FILE_SUFFIX)
  );
  if (ownerFiles.length === 0 && entries.length === 0) {
    return { kind: "empty", ageMs };
  }
  if (ownerFiles.length !== 1 || entries.length !== 1) {
    return { kind: "corrupt", ageMs };
  }

  const fileName = ownerFiles[0];
  const ownerFile = path.join(lockDir, fileName);
  try {
    const owner = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
    const expectedFileName = ownerFileName(owner.token);
    if (
      !Number.isFinite(owner.pid) ||
      owner.pid <= 0 ||
      typeof owner.token !== "string" ||
      fileName !== expectedFileName
    ) {
      return { kind: "corrupt", ageMs };
    }
    return { kind: "owned", owner, ownerFile, ageMs };
  } catch {
    return { kind: "corrupt", ageMs };
  }
}

function removeOwnedLock(ownerFile, lockDir) {
  try {
    // The token is part of the filename, so an old owner/reclaimer cannot
    // unlink a successor's ownership record.
    fs.unlinkSync(ownerFile);
  } catch {
    return false;
  }
  try {
    fs.rmdirSync(lockDir);
  } catch {
    // A successor may already have atomically replaced the now-empty dir.
  }
  return true;
}

function reclaimAbandonedLock(lockDir, options) {
  const state = readLockState(lockDir);
  if (state.kind === "missing") {
    return true;
  }
  if (state.kind === "owned") {
    const processRunning = options.isProcessRunning ?? isProcessRunning;
    if (
      processRunning(state.owner.pid, {
        identity: state.owner.processIdentity ?? undefined
      })
    ) {
      return false;
    }
    return removeOwnedLock(state.ownerFile, lockDir);
  }
  if (state.kind === "empty" && state.ageMs > options.staleMs) {
    try {
      fs.rmdirSync(lockDir);
      return true;
    } catch {
      return false;
    }
  }
  // Unknown non-empty contents fail closed. Published locks always contain
  // one complete owner file, so silently deleting anything else is unsafe.
  return false;
}

function normalizeOptions(options) {
  return {
    timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    staleMs: options.staleMs ?? DEFAULT_STALE_MS,
    retryDelayMs: options.retryDelayMs ?? RETRY_DELAY_MS,
    isProcessRunning: options.isProcessRunning
  };
}

export function acquireLockSync(lockDir, options = {}) {
  const normalized = normalizeOptions(options);
  const deadline = Date.now() + normalized.timeoutMs;
  while (true) {
    const handle = tryAcquire(lockDir);
    if (handle) {
      return handle;
    }
    reclaimAbandonedLock(lockDir, normalized);
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for lock: ${lockDir}`);
    }
    sleepSync(normalized.retryDelayMs);
  }
}

export async function acquireLock(lockDir, options = {}) {
  const normalized = normalizeOptions(options);
  const deadline = Date.now() + normalized.timeoutMs;
  while (true) {
    const handle = tryAcquire(lockDir);
    if (handle) {
      return handle;
    }
    reclaimAbandonedLock(lockDir, normalized);
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for lock: ${lockDir}`);
    }
    await sleep(normalized.retryDelayMs);
  }
}

export function releaseLock(handle) {
  if (!handle?.lockDir || !handle?.ownerFile) {
    return false;
  }
  return removeOwnedLock(handle.ownerFile, handle.lockDir);
}

export function withLockSync(lockDir, fn, options = {}) {
  const handle = acquireLockSync(lockDir, options);
  try {
    return fn();
  } finally {
    releaseLock(handle);
  }
}

export async function withLock(lockDir, fn, options = {}) {
  const handle = await acquireLock(lockDir, options);
  try {
    return await fn();
  } finally {
    releaseLock(handle);
  }
}
