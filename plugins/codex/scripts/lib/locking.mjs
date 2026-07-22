import fs from "node:fs";

const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_STALE_MS = 30000;
const RETRY_DELAY_MS = 25;

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function tryAcquire(lockDir) {
  try {
    fs.mkdirSync(lockDir);
    return true;
  } catch (error) {
    if (error?.code === "EEXIST") {
      return false;
    }
    throw error;
  }
}

function reclaimIfStale(lockDir, staleMs) {
  try {
    const age = Date.now() - fs.statSync(lockDir).mtimeMs;
    if (age > staleMs) {
      fs.rmdirSync(lockDir);
    }
  } catch {
    // Lock released (or reclaimed by someone else) between stat and rmdir.
  }
}

export function acquireLockSync(lockDir, options = {}) {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const staleMs = options.staleMs ?? DEFAULT_STALE_MS;
  const deadline = Date.now() + timeoutMs;
  while (!tryAcquire(lockDir)) {
    reclaimIfStale(lockDir, staleMs);
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for lock: ${lockDir}`);
    }
    sleepSync(RETRY_DELAY_MS);
  }
}

export async function acquireLock(lockDir, options = {}) {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const staleMs = options.staleMs ?? DEFAULT_STALE_MS;
  const deadline = Date.now() + timeoutMs;
  while (!tryAcquire(lockDir)) {
    reclaimIfStale(lockDir, staleMs);
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for lock: ${lockDir}`);
    }
    await sleep(RETRY_DELAY_MS);
  }
}

export function releaseLock(lockDir) {
  try {
    fs.rmdirSync(lockDir);
  } catch {
    // Already released or reclaimed as stale; nothing left to do.
  }
}

export function withLockSync(lockDir, fn, options = {}) {
  acquireLockSync(lockDir, options);
  try {
    return fn();
  } finally {
    releaseLock(lockDir);
  }
}

export async function withLock(lockDir, fn, options = {}) {
  await acquireLock(lockDir, options);
  try {
    return await fn();
  } finally {
    releaseLock(lockDir);
  }
}
