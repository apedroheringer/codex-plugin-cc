import process from "node:process";
import test from "node:test";
import assert from "node:assert/strict";

import {
  isProcessRunning,
  runCommand,
  runCommandChecked,
  terminateProcessTree
} from "../plugins/codex/scripts/lib/process.mjs";

const SELF_TERMINATING_SCRIPT = "process.kill(process.pid, 'SIGTERM'); setInterval(() => {}, 1000);";

test("runCommand reports a signal-terminated process as a failure", { skip: process.platform === "win32" }, () => {
  const result = runCommand(process.execPath, ["-e", SELF_TERMINATING_SCRIPT]);

  assert.equal(result.signal, "SIGTERM");
  assert.equal(result.status, null);
});

test("runCommandChecked throws when the process dies from a signal", { skip: process.platform === "win32" }, () => {
  assert.throws(
    () => runCommandChecked(process.execPath, ["-e", SELF_TERMINATING_SCRIPT]),
    /signal=SIGTERM/
  );
});

test("Linux zombie processes are treated as exited even when signal 0 succeeds", () => {
  const running = isProcessRunning(1234, {
    platform: "linux",
    killImpl() {},
    readProcessStat() {
      return { state: "Z", startTime: "42" };
    }
  });

  assert.equal(running, false);
});

test("terminateProcessTree uses taskkill on Windows", () => {
  let captured = null;
  const outcome = terminateProcessTree(1234, {
    platform: "win32",
    runCommandImpl(command, args) {
      captured = { command, args };
      return {
        command,
        args,
        status: 0,
        signal: null,
        stdout: "",
        stderr: "",
        error: null
      };
    },
    killImpl() {
      throw new Error("kill fallback should not run");
    }
  });

  assert.deepEqual(captured, {
    command: "taskkill",
    args: ["/PID", "1234", "/T", "/F"]
  });
  assert.equal(outcome.delivered, true);
  assert.equal(outcome.method, "taskkill");
});

test("terminateProcessTree treats missing Windows processes as already stopped", () => {
  const outcome = terminateProcessTree(1234, {
    platform: "win32",
    runCommandImpl(command, args) {
      return {
        command,
        args,
        status: 128,
        signal: null,
        stdout: "ERROR: The process \"1234\" not found.",
        stderr: "",
        error: null
      };
    }
  });

  assert.equal(outcome.attempted, true);
  assert.equal(outcome.method, "taskkill");
  assert.equal(outcome.result.status, 128);
  assert.match(outcome.result.stdout, /not found/i);
});
