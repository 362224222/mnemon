import { execFile, execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const HOOK_CUE = "mnemond state is available; read .pi/skills/mnemond/SKILL.md and use its exact Pi tools and artifact commands.";
const MAX_OUTPUT_BYTES = 4096;
const ATTACH_TIMEOUT_MS = 5000;
const SUBMIT_TIMEOUT_MS = 5000;
const ATTACH_ATTEMPTS = 2;
const MAX_TOOL_CALL_ATTEMPTS_PER_RUN = 16;
const MAX_EFFECT_SETTLEMENT_ATTEMPTS = 2;
const MAX_INTENT_BYTES = 12 * 1024;
const EFFECT_SETTLEMENT_TOOL = "mnemond_submit";
const ATTENTION_EXHAUSTED_REASON =
  "Attention budget exhausted. This tool did not run. Only mnemond_submit may remain.";

const SubmitParameters = {
  type: "object",
  properties: {
    intent: {
      type: "object",
      description: "One Intent object copied from the current mnemond View",
      additionalProperties: true,
    },
  },
  required: ["intent"],
  additionalProperties: false,
} as const;

function boundaryEnvelope(boundary: string): string {
  return JSON.stringify({ boundary, schema: "mnemon.hook.boundary", version: 1 });
}

function runBoundary(args: string[], boundary: string): boolean {
  try {
    execFileSync("mnemond", args, {
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

function intentInput(value: unknown): string | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value) ||
      Object.keys(value).length === 0) return undefined;
  try {
    const encoded = JSON.stringify(value);
    if (Buffer.byteLength(encoded, "utf8") > MAX_INTENT_BYTES) return undefined;
    return encoded;
  } catch {
    return undefined;
  }
}

function submitIntent(encoded: string, signal: AbortSignal): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = execFile("mnemond", ["agent", "submit", "--json"], {
      encoding: "utf8",
      maxBuffer: MAX_OUTPUT_BYTES,
      shell: false,
      signal,
      timeout: SUBMIT_TIMEOUT_MS,
    }, (error, result) => {
      if (error) reject(error);
      else resolve(result);
    });
    if (child.stdin === null) {
      child.kill();
      reject(new Error("submit stdin unavailable"));
      return;
    }
    child.stdin.on("error", () => {
      // The owned child callback reports the bounded process outcome.
    });
    child.stdin.end(encoded);
  });
}

export default function (pi: ExtensionAPI) {
  let activeBoundary: string | undefined;
  let governedRun = false;
  let toolCallAttempts = 0;
  let budgetExhausted = false;
  let effectSettlementAttempts = 0;
  let postBudgetTurns = 0;
  let postSettlementFinalTurns = 0;
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
    effectSettlementAttempts = 0;
    postBudgetTurns = 0;
    postSettlementFinalTurns = 0;
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

  pi.registerTool({
    name: EFFECT_SETTLEMENT_TOOL,
    label: "Submit mnemond Intent",
    description:
      "Submit one bounded Intent from the current View. The Receipt alone reports its Effect.",
    parameters: SubmitParameters as never,

    async execute(_toolCallId, params, signal) {
      const encoded = intentInput(params?.intent);
      if (encoded === undefined) {
        return {
          content: [{ type: "text" as const, text: "Invalid bounded Intent object." }],
          details: { schema: "mnemon.pi.effect", version: 1, status: "input_invalid" },
        };
      }
      try {
        const receiptText = await submitIntent(encoded, signal);
        return {
          content: [{ type: "text" as const, text: receiptText }],
          details: { schema: "mnemon.pi.effect", version: 1, status: "settled" },
        };
      } catch {
        return {
          content: [{ type: "text" as const, text: "Submit failed; correct once or stop." }],
          details: { schema: "mnemon.pi.effect", version: 1, status: "failed" },
        };
      }
    },
  });

  pi.on("tool_result", async (event) => {
    if (event.toolName !== EFFECT_SETTLEMENT_TOOL) return;
    const details = event.details as
      | { schema?: unknown; version?: unknown; status?: unknown }
      | undefined;
    if (details?.schema !== "mnemon.pi.effect" || details.version !== 1 ||
        details.status !== "settled") return { isError: true };
  });

  pi.on("before_agent_start", async () => {
    if (governedRun) return undefined;
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
    if (_event.toolName === EFFECT_SETTLEMENT_TOOL) {
      if (effectSettlementAttempts >= MAX_EFFECT_SETTLEMENT_ATTEMPTS) {
        return { block: true, reason: ATTENTION_EXHAUSTED_REASON };
      }
      effectSettlementAttempts += 1;
      postBudgetTurns = 0;
      postSettlementFinalTurns = 0;
      if (budgetExhausted && effectSettlementAttempts === MAX_EFFECT_SETTLEMENT_ATTEMPTS) {
        try {
          pi.setActiveTools([]);
        } catch {
          // The tool_call gate still blocks every later attempt.
        }
      }
      return undefined;
    }
    if (!budgetExhausted && toolCallAttempts < MAX_TOOL_CALL_ATTEMPTS_PER_RUN) {
      toolCallAttempts += 1;
      return undefined;
    }
    if (!budgetExhausted) {
      budgetExhausted = true;
      try {
        savedActiveTools = [...pi.getActiveTools()];
        ownsToolOverride = true;
        const settlementAllowed = savedActiveTools.includes(EFFECT_SETTLEMENT_TOOL) &&
          effectSettlementAttempts < MAX_EFFECT_SETTLEMENT_ATTEMPTS;
        pi.setActiveTools(settlementAllowed ? [EFFECT_SETTLEMENT_TOOL] : []);
      } catch {
        abortOnce(ctx);
      }
    }
    return { block: true, reason: ATTENTION_EXHAUSTED_REASON };
  });

  pi.on("turn_start", async (_event, ctx) => {
    if (!governedRun || !budgetExhausted) return;
    if (effectSettlementAttempts >= MAX_EFFECT_SETTLEMENT_ATTEMPTS) {
      postSettlementFinalTurns += 1;
      if (postSettlementFinalTurns > 1) abortOnce(ctx);
      return;
    }
    postBudgetTurns += 1;
    if (postBudgetTurns > MAX_EFFECT_SETTLEMENT_ATTEMPTS - effectSettlementAttempts) abortOnce(ctx);
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
