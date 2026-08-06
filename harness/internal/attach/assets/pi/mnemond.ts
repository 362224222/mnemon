import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const HOOK_CUE = "mnemond state is available; read .pi/skills/mnemond/SKILL.md and use mnemon-harness from PATH.";
const MAX_OUTPUT_BYTES = 4096;
const ATTACH_TIMEOUT_MS = 5000;
const ATTACH_ATTEMPTS = 2;
const MAX_TOOL_CALL_ATTEMPTS_PER_RUN = 16;
const ATTENTION_EXHAUSTED_REASON =
  "Attention budget exhausted. This tool did not run. Do not retry tools. Give a concise final response from current evidence and state unresolved work.";

function boundaryEnvelope(boundary: string): string {
  return JSON.stringify({ boundary, schema: "mnemon.hook.boundary", version: 1 });
}

function runBoundary(args: string[], boundary: string): boolean {
  try {
    execFileSync("mnemon-harness", args, {
      input: boundaryEnvelope(boundary),
      maxBuffer: MAX_OUTPUT_BYTES,
      stdio: ["pipe", "ignore", "ignore"],
      timeout: ATTACH_TIMEOUT_MS,
    });
    return true;
  } catch {
    return false;
  }
}

function attachBoundary(boundary: string): boolean {
  for (let attempt = 0; attempt < ATTACH_ATTEMPTS; attempt += 1) {
    if (runBoundary(["hook", "attach", "--json"], boundary)) return true;
  }
  return false;
}

function endBoundary(boundary: string): boolean {
  return runBoundary(["hook", "end", "--json"], boundary);
}

export default function (pi: ExtensionAPI) {
  let activeBoundary: string | undefined;
  let governedRun = false;
  let toolCallAttempts = 0;
  let budgetExhausted = false;
  let postBudgetTurns = 0;
  let abortIssued = false;
  let ownsToolOverride = false;
  let savedActiveTools: string[] | undefined;

  function abortOnce(ctx: { abort(): void }): void {
    if (abortIssued) return;
    try {
      ctx.abort();
      abortIssued = true;
    } catch {
      // Tool calls remain blocked; a later turn may retry the Host abort.
    }
  }

  function resetAttention(): boolean {
    governedRun = false;
    toolCallAttempts = 0;
    budgetExhausted = false;
    postBudgetTurns = 0;
    abortIssued = false;
    if (!ownsToolOverride) {
      savedActiveTools = undefined;
      return true;
    }
    if (savedActiveTools === undefined) return false;
    try {
      pi.setActiveTools(savedActiveTools);
      ownsToolOverride = false;
      savedActiveTools = undefined;
      return true;
    } catch {
      // Retain ownership and the exact snapshot so the next boundary can retry.
      return false;
    }
  }

  pi.on("before_agent_start", async () => {
    if (!resetAttention()) return undefined;
    const boundary = randomBytes(32).toString("base64url");
    if (!attachBoundary(boundary)) return undefined;
    activeBoundary = boundary;
    governedRun = true;
    return {
      message: {
        customType: "mnemond",
        content: HOOK_CUE,
        display: false,
      },
    };
  });

  pi.on("tool_call", async (_event, ctx) => {
    if (!governedRun) return undefined;
    if (!budgetExhausted && toolCallAttempts < MAX_TOOL_CALL_ATTEMPTS_PER_RUN) {
      toolCallAttempts += 1;
      return undefined;
    }
    if (!budgetExhausted) {
      budgetExhausted = true;
      try {
        savedActiveTools = [...pi.getActiveTools()];
        ownsToolOverride = true;
        pi.setActiveTools([]);
      } catch {
        abortOnce(ctx);
      }
    }
    return { block: true, reason: ATTENTION_EXHAUSTED_REASON };
  });

  pi.on("turn_start", async (_event, ctx) => {
    if (!governedRun || !budgetExhausted) return;
    postBudgetTurns += 1;
    if (postBudgetTurns > 1) abortOnce(ctx);
  });

  // agent_end may be followed by an automatic retry or compaction. Only the
  // fully settled callback may release this run's attention boundary.
  pi.on("agent_settled", async () => {
    resetAttention();
  });

  pi.on("session_shutdown", async () => {
    resetAttention();
    const boundary = activeBoundary;
    activeBoundary = undefined;
    if (boundary !== undefined) endBoundary(boundary);
  });
}
