import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const extensionPath = process.env.MNEMON_PI_EXTENSION;
if (!extensionPath) throw new Error("MNEMON_PI_EXTENSION is required");
const { default: mnemondExtension } = await import(extensionPath);

async function withFakeMnemond(fn) {
  const directory = await mkdtemp(path.join(tmpdir(), "mnemon-pi-attention-"));
  const executable = path.join(directory, "mnemon");
  const log = path.join(directory, "calls.log");
  const submitInput = path.join(directory, "submit.jsonl");
  const oldPath = process.env.PATH;
  const oldLog = process.env.MNEMON_HOOK_LOG;
  const oldFail = process.env.MNEMON_HOOK_FAIL;
  const oldSubmitInput = process.env.MNEMON_SUBMIT_INPUT;
  await writeFile(
    executable,
    '#!/bin/sh\ninput=$(cat)\nprintf "%s\\n" "$*" >>"$MNEMON_HOOK_LOG"\n' +
      'if test "$*" = "agency agent submit --json"; then printf "%s\\n" "$input" >>"$MNEMON_SUBMIT_INPUT"; ' +
      'printf "%s\\n" \'{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":false}\'; fi\n' +
      'test "${MNEMON_HOOK_FAIL:-0}" != 1\n',
  );
  await chmod(executable, 0o755);
  process.env.PATH = `${directory}:${oldPath ?? ""}`;
  process.env.MNEMON_HOOK_LOG = log;
  process.env.MNEMON_SUBMIT_INPUT = submitInput;
  delete process.env.MNEMON_HOOK_FAIL;
  try {
    await fn({ log, submitInput });
  } finally {
    if (oldPath === undefined) delete process.env.PATH;
    else process.env.PATH = oldPath;
    if (oldLog === undefined) delete process.env.MNEMON_HOOK_LOG;
    else process.env.MNEMON_HOOK_LOG = oldLog;
    if (oldFail === undefined) delete process.env.MNEMON_HOOK_FAIL;
    else process.env.MNEMON_HOOK_FAIL = oldFail;
    if (oldSubmitInput === undefined) delete process.env.MNEMON_SUBMIT_INPUT;
    else process.env.MNEMON_SUBMIT_INPUT = oldSubmitInput;
    await rm(directory, { recursive: true, force: true });
  }
}

function fakePi(initialTools = ["bash", "read", "delegate"]) {
  const handlers = new Map();
  const setCalls = [];
  const registeredTools = new Map();
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
    registerTool(tool) {
      assert.equal(registeredTools.has(tool.name), false, `duplicate ${tool.name} tool`);
      registeredTools.set(tool.name, tool);
      if (!activeTools.includes(tool.name)) activeTools.push(tool.name);
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
    tool: (name) => registeredTools.get(name),
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

test("a governed Pi run preserves only a bounded native Effect slot after sixteen exploration calls", async () => {
  await withFakeMnemond(async ({ log }) => {
    const runtime = fakePi();
    const abort = abortContext();
    await attach(runtime);

    const blocked = await exhaust(runtime, abort.context);
    assert.equal(blocked.block, true);
    assert.match(blocked.reason, /Attention budget exhausted/);
    assert.match(blocked.reason, /This tool did not run/);
    assert.doesNotMatch(blocked.reason.toLowerCase(), /accepted|completed|receipt/);
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);
    assert.deepEqual(runtime.setCalls, [["mnemond_submit"]]);

    const alsoBlocked = await runtime.handlers.get("tool_call")(
      { toolName: "read", toolCallId: "18" },
      abort.context,
    );
    assert.equal(alsoBlocked.block, true);
    assert.deepEqual(runtime.setCalls, [["mnemond_submit"]], "a parallel excess call changed the saved tool snapshot");

    await runtime.handlers.get("turn_start")({ turnIndex: 2 }, abort.context);
    assert.equal(abort.count(), 0, "the first Effect settlement opportunity was aborted");
    await runtime.handlers.get("turn_start")({ turnIndex: 3 }, abort.context);
    assert.equal(abort.count(), 0, "the correction opportunity was aborted");
    await runtime.handlers.get("turn_start")({ turnIndex: 4 }, abort.context);
    assert.equal(abort.count(), 1, "the hard fallback was not idempotent");

    await runtime.handlers.get("agent_settled")({}, {});
    assert.deepEqual(runtime.activeTools(), ["bash", "read", "delegate", "mnemond_submit"]);
    assert.deepEqual(runtime.setCalls, [
      ["mnemond_submit"],
      ["bash", "read", "delegate", "mnemond_submit"],
    ]);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), ["agency hook attach --json"]);
  });
});

test("Effect settlement is separate from exploration and executes exactly one fixed stdin command", async () => {
  await withFakeMnemond(async ({ log, submitInput }) => {
    const runtime = fakePi(["bash", "read"]);
    const abort = abortContext();
    await attach(runtime);
    const toolCall = runtime.handlers.get("tool_call");
    const submit = runtime.tool("mnemond_submit");
    const firstIntent = { kind: "opaque.first", payload: "one", consequence: "handling.advance" };
    const secondIntent = { kind: "opaque.second", payload: "two", consequence: "handling.resolve.unresolved" };

    assert.equal(await toolCall({ toolName: "mnemond_submit", toolCallId: "settle-1" }, abort.context), undefined);
    const first = await submit.execute("settle-1", { intent: firstIntent }, new AbortController().signal);
    assert.equal(first.details.status, "settled");
    assert.match(first.content[0].text, /mnemon\.agent\.receipt/);
    assert.equal(await runtime.handlers.get("tool_result")({
      toolName: "mnemond_submit",
      details: first.details,
    }), undefined);

    for (let attempt = 1; attempt <= 16; attempt += 1) {
      assert.equal(await toolCall({ toolName: "bash", toolCallId: `explore-${attempt}` }, abort.context), undefined);
    }
    const blocked = await toolCall({ toolName: "read", toolCallId: "explore-17" }, abort.context);
    assert.equal(blocked.block, true);
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);

    assert.equal(await toolCall({ toolName: "mnemond_submit", toolCallId: "settle-2" }, abort.context), undefined);
    assert.deepEqual(runtime.activeTools(), [], "the second Effect attempt did not close the slot");
    const second = await submit.execute("settle-2", { intent: secondIntent }, new AbortController().signal);
    assert.equal(second.details.status, "settled");
    assert.deepEqual(await runtime.handlers.get("tool_result")({
      toolName: "mnemond_submit",
      details: { schema: "mnemon.pi.effect", version: 1, status: "failed" },
    }), { isError: true });
    assert.deepEqual(await runtime.handlers.get("tool_result")({
      toolName: "mnemond_submit",
      details: { schema: "wrong", version: 1, status: "settled" },
    }), { isError: true });
    assert.deepEqual(await runtime.handlers.get("tool_result")({
      toolName: "mnemond_submit",
    }), { isError: true });
    assert.equal((await toolCall({ toolName: "mnemond_submit", toolCallId: "settle-3" }, abort.context)).block, true);

    await runtime.handlers.get("turn_start")({ turnIndex: 5 }, abort.context);
    assert.equal(abort.count(), 0, "the final response turn after settlement was aborted");
    await runtime.handlers.get("turn_start")({ turnIndex: 6 }, abort.context);
    assert.equal(abort.count(), 1, "the final response bound did not close the run");
    assert.deepEqual((await readFile(submitInput, "utf8")).trim().split("\n"), [
      JSON.stringify(firstIntent),
      JSON.stringify(secondIntent),
    ]);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), [
      "agency hook attach --json",
      "agency agent submit --json",
      "agency agent submit --json",
    ]);
  });
});

test("automatic continuation cannot regain tools before agent_settled", async () => {
  await withFakeMnemond(async () => {
    const runtime = fakePi(["bash", "read"]);
    const abort = abortContext();
    await attach(runtime);
    await exhaust(runtime, abort.context);

    assert.equal(runtime.handlers.has("agent_end"), false);
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);
    const blocked = await runtime.handlers.get("tool_call")(
      { toolName: "bash", toolCallId: "retry" },
      abort.context,
    );
    assert.equal(blocked.block, true);
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);

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

test("cutoff never re-enables a settlement tool removed by the Host allowlist", async () => {
  await withFakeMnemond(async () => {
    const runtime = fakePi(["bash", "read"]);
    const abort = abortContext();
    await attach(runtime);
    runtime.replaceTools(["bash", "read"]);
    await exhaust(runtime, abort.context);
    assert.deepEqual(runtime.activeTools(), []);
    assert.deepEqual(runtime.setCalls.at(-1), []);
  });
});

test("automatic continuation cannot remint attachment or attention before settlement", async () => {
  await withFakeMnemond(async ({ log }) => {
    const runtime = fakePi(["bash"]);
    const abort = abortContext();
    await attach(runtime);
    runtime.replaceTools(["bash", "read", "delegate"]);
    await exhaust(runtime, abort.context);
    assert.deepEqual(runtime.activeTools(), []);

    assert.equal(await runtime.handlers.get("before_agent_start")({}, {}), undefined);
    assert.deepEqual(runtime.activeTools(), []);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), [
      "agency hook attach --json",
    ]);

    await runtime.handlers.get("agent_settled")({}, {});
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
      "agency hook attach --json",
      "agency hook attach --json",
      "agency hook end --json",
    ]);
  });
});

test("attachment and Host API failures stay bounded without creating a fresh tool budget", async () => {
  await withFakeMnemond(async () => {
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
    assert.deepEqual(runtime.activeTools(), ["bash", "mnemond_submit"]);

    process.env.MNEMON_HOOK_FAIL = "0";
    await attach(runtime);
    runtime.failSet(true);
    const blocked = await exhaust(runtime, abort.context);
    assert.equal(blocked.block, true);
    assert.equal(abort.count(), 1, "a failed tool override did not abort the run");
    runtime.failSet(false);
    await runtime.handlers.get("agent_settled")({}, {});
    assert.deepEqual(runtime.activeTools(), ["bash", "mnemond_submit"]);

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
  await withFakeMnemond(async ({ log }) => {
    const runtime = fakePi(["bash", "read"]);
    const abort = abortContext();
    await attach(runtime);
    await exhaust(runtime, abort.context);
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);

    runtime.failSet(true);
    await runtime.handlers.get("agent_settled")({}, {});
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);
    assert.equal(await runtime.handlers.get("before_agent_start")({}, {}), undefined);
    assert.deepEqual(runtime.activeTools(), ["mnemond_submit"]);
    assert.deepEqual((await readFile(log, "utf8")).trim().split("\n"), ["agency hook attach --json"]);

    runtime.failSet(false);
    await attach(runtime);
    assert.deepEqual(runtime.activeTools(), ["bash", "read", "mnemond_submit"]);
    assert.equal(
      await runtime.handlers.get("tool_call")(
        { toolName: "bash", toolCallId: "recovered" },
        abort.context,
      ),
      undefined,
    );
  });
});
