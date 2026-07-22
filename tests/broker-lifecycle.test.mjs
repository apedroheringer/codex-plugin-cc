import test from "node:test";
import assert from "node:assert/strict";

import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { makeTempDir } from "./helpers.mjs";
import {
  ensureBrokerSession,
  loadBrokerSession,
  sendBrokerShutdown
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";

test("concurrent ensureBrokerSession calls share a single broker", async () => {
  const workspace = makeTempDir();
  const binDir = makeTempDir();
  installFakeCodex(binDir);
  const env = buildEnv(binDir);

  let first = null;
  try {
    const [left, right] = await Promise.all([
      ensureBrokerSession(workspace, { env }),
      ensureBrokerSession(workspace, { env })
    ]);
    first = left ?? right;

    assert.ok(left, "first ensureBrokerSession returned no session");
    assert.ok(right, "second ensureBrokerSession returned no session");
    assert.equal(left.endpoint, right.endpoint);
    assert.equal(left.pid, right.pid);

    const persisted = loadBrokerSession(workspace);
    assert.ok(persisted);
    assert.equal(persisted.endpoint, left.endpoint);
  } finally {
    if (first?.endpoint) {
      await sendBrokerShutdown(first.endpoint);
    }
  }
});
