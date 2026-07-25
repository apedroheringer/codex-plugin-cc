import process from "node:process";
import test from "node:test";
import assert from "node:assert/strict";

import {
  getProcessIdentity,
  isProcessRunning,
  processHasLaunchSequence,
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

test("Linux processes with unreadable /proc metadata stay running", () => {
  const running = isProcessRunning(1234, {
    platform: "linux",
    identity: "42",
    killImpl() {},
    readProcessStat() {
      return null;
    }
  });

  assert.equal(running, true);
});

test("macOS process identity uses the process start time", () => {
  let captured = null;
  const identity = getProcessIdentity(1234, {
    platform: "darwin",
    runCommandImpl(command, args) {
      captured = { command, args };
      return {
        command,
        args,
        status: 0,
        signal: null,
        stdout: "Fri Jul 25 01:02:03 2026\n",
        stderr: "",
        error: null
      };
    }
  });

  assert.deepEqual(captured, {
    command: "ps",
    args: ["-ww", "-p", "1234", "-o", "lstart="]
  });
  assert.equal(identity, "Fri Jul 25 01:02:03 2026");
});

test("Windows process identity uses PowerShell start-time ticks", () => {
  let capturedCommand = null;
  const identity = getProcessIdentity(1234, {
    platform: "win32",
    runCommandImpl(command, args) {
      capturedCommand = { command, args };
      return {
        command,
        args,
        status: 0,
        signal: null,
        stdout: "638890021230000000",
        stderr: "",
        error: null
      };
    }
  });

  assert.equal(capturedCommand.command, "powershell.exe");
  assert.match(capturedCommand.args.at(-1), /Get-Process -Id 1234/);
  assert.equal(identity, "638890021230000000");
});

test("non-Linux process identity distinguishes a reused PID", () => {
  const running = isProcessRunning(1234, {
    platform: "darwin",
    identity: "original-start",
    killImpl() {},
    runCommandImpl(command, args) {
      return {
        command,
        args,
        status: 0,
        signal: null,
        stdout: "replacement-start\n",
        stderr: "",
        error: null
      };
    }
  });

  assert.equal(running, false);
});

test("process launch sequence fallback requires arguments in order", () => {
  const runCommandImpl = (command, args) => ({
    command,
    args,
    status: 0,
    signal: null,
    stdout: "node app-server-broker.mjs serve --endpoint pipe:broker --cwd /workspace --pid-file /tmp/broker.pid",
    stderr: "",
    error: null
  });

  assert.equal(
    processHasLaunchSequence(
      1234,
      ["serve", "--endpoint", "pipe:broker", "--cwd", "/workspace", "--pid-file", "/tmp/broker.pid"],
      { platform: "darwin", runCommandImpl }
    ),
    true
  );
  assert.equal(
    processHasLaunchSequence(1234, ["--cwd", "/workspace", "--endpoint", "pipe:broker"], {
      platform: "darwin",
      runCommandImpl
    }),
    false
  );
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
