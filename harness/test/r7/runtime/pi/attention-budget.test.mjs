import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const extensionPath = process.env.MNEMON_PI_EXTENSION;
if (!extensionPath) throw new Error("MNEMON_PI_EXTENSION is required");
const { default: mnemondExtension } = await import(extensionPath);

async function withFakeHarness(fn) {
  const directory = await mkdtemp(path.join(tmpdir(), "mnemon-pi-attention-"));
  const executable = path.join(directory, "mnemon-harness");
  const log = path.join(directory, "calls.log");
  const oldPath = process.env.PATH;
  const oldLog = process.env.MNEMON_HOOK_LOG;
  const oldFail = process.env.MNEMON_HOOK_FAIL;
  await writeFile(
    executable,
    '#!/bin/sh\ncat >/dev/null\nprintf "%s\\n" "$*" >>"$MNEMON_HOOK_LOG"\ntest "${MNEMON_HOOK_FAIL:-0}" != 1\n',
  );
  await chmod(executable, 0o755);
  process.env.PATH = `${directory}:${oldPath ?? ""}`;
  process.env.MNEMON_HOOK_LOG = log;
  delete process.env.MNEMON_HOOK_FAIL;
  try {
    await fn({ log });
  } finally {
    if (oldPath === undefined) delete process.env.PATH;
    else process.env.PATH = oldPath;
    if (oldLog === undefined) delete process.env.MNEMON_HOOK_LOG;
    else process.env.MNEMON_HOOK_LOG = oldLog;
    if (oldFail === undefined) delete process.env.MNEMON_HOOK_FAIL;
    else process.env.MNEMON_HOOK_FAIL = oldFail;
    await rm(directory, { recursive: true, force: true });
  }
}

function fakePi(initialTools = ["bash", "read", "delegate"]) {
  const handlers = new Map();
  const setCalls = [];
  let activeTools = [...initialTools];
  let getFailure = false;
  let setFailure = false;
  const pi = {
    on(name, handler) {
      assert.equal(handlers.has(name), false, `duplicate ${name} handler`);
      handlers.set(name, handler);
    },
    getActiveTools() {
      if (getFailure) throw new Error("get failure");
      return [...activeTools];
    },
    setActiveTools(names) {
      setCalls.push([...names]);
      if (setFailure) throw new Error("set failure");
      activeTools = [...names];
    },
  };
  mnemondExtension(pi);
  return {
    handlers,
    setCalls,
    activeTools: () => [...activeTools],
    replaceTools: (tools) => { activeTools = [...tools]; },
    failGet: (value) => { getFailure = value; },
    failSet: (value) => { setFailure = value; },
  };
}

function abortContext() {
  let count = 0;
  return { context: { abort() { count += 1; } }, count: () => count };
}

async function attach(runtime) {
  const result = await runtime.handlers.get("before_agent_start")({}, {});
  assert.equal(result?.message?.customType, "mnemond");
  return result;
}

async function exhaust(runtime, context) {
  const toolCall = runtime.handlers.get("tool_call");
  for (let attempt = 1; attempt <= 16; attempt += 1) {
    assert.equal(await toolCall({ toolName: "bash", toolCallId: `${attempt}` }, context), undefined);
  }
  return toolCall({ toolName: "bash", toolCallId: "17" }, context);
}

test("a governed Pi run executes at most sixteen tool calls and gets one tool-free settlement turn", async () => {
  await withFakeHarness(async ({ log }) => {
    const runtime = fakePi();
    const abort = abortContext();
    await attach(runtime);

    const blocked = await exhaust(runtime, abort.context);
    assert.equal(blocked.block, true);
    assert.match(blocked.reason, /Attention budget exhausted/);
    assert.match(blocked.reason, /This tool did not run/);
    assert.doesNotMatch(blocked.reason.toLowerCase(), /accepted|completed|receipt/);
    assert.deepEqual(runtime.activeTools(), []);
    assert.deepEqual(runtime.setCalls, [[]]);

    const alsoBlocked = await runtime.handlers.get("tool_call")(
      { toolName: "read", toolCallId: "18" },
      abort.context,
    );
    assert.equal(alsoBlocked.block, true);
    assert.deepEqual(runtime.setCalls, [[]], "a parallel excess call changed the saved tool snapshot");

    await runtime.handlers.get("turn_start")({ turnIndex: 2 }, abort.context);
    assert.equal(abort.count(), 0, "the single tool-free settlement turn was aborted");
    await runtime.handlers.get("turn_start")({ turnIndex: 3 }, abort.context);
    await runtime.handlers.get("turn_start")({ turnIndex: 4 }, abort.context);
    assert.equal(abort.count(), 1, "the hard fallback was not idempotent");

    await runtime.handlers.get("agent_settled")({}, {});
    assert.deepEqual(runtime.activeTools(), ["bash", "read", "delegate"]);
    assert.deepEqual(runtime.setCalls, [[], ["bash", "read", "delegate"]]);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), ["hook attach --json"]);
  });
});

test("automatic continuation cannot regain tools before agent_settled", async () => {
  await withFakeHarness(async () => {
    const runtime = fakePi(["bash", "read"]);
    const abort = abortContext();
    await attach(runtime);
    await exhaust(runtime, abort.context);

    assert.equal(runtime.handlers.has("agent_end"), false);
    assert.deepEqual(runtime.activeTools(), []);
    const blocked = await runtime.handlers.get("tool_call")(
      { toolName: "bash", toolCallId: "retry" },
      abort.context,
    );
    assert.equal(blocked.block, true);
    assert.deepEqual(runtime.activeTools(), []);

    await runtime.handlers.get("agent_settled")({}, {});
    await attach(runtime);
    assert.equal(
      await runtime.handlers.get("tool_call")(
        { toolName: "bash", toolCallId: "new-run" },
        abort.context,
      ),
      undefined,
    );
  });
});

test("the cutoff restores the exact current tool set on replacement and shutdown", async () => {
  await withFakeHarness(async ({ log }) => {
    const runtime = fakePi(["bash"]);
    const abort = abortContext();
    await attach(runtime);
    runtime.replaceTools(["bash", "read", "delegate"]);
    await exhaust(runtime, abort.context);
    assert.deepEqual(runtime.activeTools(), []);

    await attach(runtime);
    assert.deepEqual(runtime.activeTools(), ["bash", "read", "delegate"]);
    assert.equal(
      await runtime.handlers.get("tool_call")(
        { toolName: "bash", toolCallId: "replacement" },
        abort.context,
      ),
      undefined,
    );
    for (let attempt = 2; attempt <= 16; attempt += 1) {
      assert.equal(
        await runtime.handlers.get("tool_call")(
          { toolName: "bash", toolCallId: `replacement-${attempt}` },
          abort.context,
        ),
        undefined,
      );
    }
    assert.equal(
      (await runtime.handlers.get("tool_call")(
        { toolName: "bash", toolCallId: "replacement-17" },
        abort.context,
      )).block,
      true,
    );
    assert.deepEqual(runtime.activeTools(), []);

    await runtime.handlers.get("session_shutdown")({}, {});
    assert.deepEqual(runtime.activeTools(), ["bash", "read", "delegate"]);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), [
      "hook attach --json",
      "hook attach --json",
      "hook end --json",
    ]);
  });
});

test("attachment and Host API failures stay bounded without creating a fresh tool budget", async () => {
  await withFakeHarness(async () => {
    const runtime = fakePi(["bash"]);
    const abort = abortContext();
    process.env.MNEMON_HOOK_FAIL = "1";
    assert.equal(await runtime.handlers.get("before_agent_start")({}, {}), undefined);
    assert.equal(
      await runtime.handlers.get("tool_call")(
        { toolName: "bash", toolCallId: "unattached" },
        abort.context,
      ),
      undefined,
    );
    assert.deepEqual(runtime.activeTools(), ["bash"]);

    process.env.MNEMON_HOOK_FAIL = "0";
    await attach(runtime);
    runtime.failSet(true);
    const blocked = await exhaust(runtime, abort.context);
    assert.equal(blocked.block, true);
    assert.equal(abort.count(), 1, "a failed tool override did not abort the run");
    runtime.failSet(false);
    await runtime.handlers.get("agent_settled")({}, {});
    assert.deepEqual(runtime.activeTools(), ["bash"]);

    await attach(runtime);
    runtime.failGet(true);
    const getBlocked = await exhaust(runtime, abort.context);
    assert.equal(getBlocked.block, true);
    assert.equal(abort.count(), 2, "a failed tool snapshot did not abort the next run");
    runtime.failGet(false);
    await runtime.handlers.get("agent_settled")({}, {});
  });
});

test("a failed tool restore retains authority and blocks the next governed run", async () => {
  await withFakeHarness(async ({ log }) => {
    const runtime = fakePi(["bash", "read"]);
    const abort = abortContext();
    await attach(runtime);
    await exhaust(runtime, abort.context);
    assert.deepEqual(runtime.activeTools(), []);

    runtime.failSet(true);
    await runtime.handlers.get("agent_settled")({}, {});
    assert.deepEqual(runtime.activeTools(), []);
    assert.equal(await runtime.handlers.get("before_agent_start")({}, {}), undefined);
    assert.deepEqual(runtime.activeTools(), []);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), ["hook attach --json"]);

    runtime.failSet(false);
    await attach(runtime);
    assert.deepEqual(runtime.activeTools(), ["bash", "read"]);
    assert.equal(
      await runtime.handlers.get("tool_call")(
        { toolName: "bash", toolCallId: "recovered" },
        abort.context,
      ),
      undefined,
    );
  });
});
