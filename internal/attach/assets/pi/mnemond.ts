import { execFile, execFileSync, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const HOOK_CUE = "mnemond state is available; read .pi/skills/mnemond/SKILL.md and use its exact Pi tools and artifact commands.";
const MAX_BOUNDARY_OUTPUT_BYTES = 4096;
const MAX_RECEIPT_OUTPUT_BYTES = (4 << 10) + 1;
const ATTACH_TIMEOUT_MS = 5000;
const SUBMIT_TIMEOUT_MS = 5000;
const SUBMIT_SHUTDOWN_GRACE_MS = 100;
const ATTACH_ATTEMPTS = 2;
const MAX_INTENT_BYTES = 12 * 1024;
const MAX_DIAGNOSTIC_BYTES = 512;
const SUBMIT_TOOL = "mnemond_submit";

class SubmitInterruptedError extends Error {}

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
    execFileSync("mnemon", ["agency", ...args], {
      input: boundaryEnvelope(boundary),
      maxBuffer: MAX_BOUNDARY_OUTPUT_BYTES,
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

function hasExactKeys(value: object, expected: string[]): boolean {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length &&
    actual.every((key, index) => key === expected[index]);
}

function parseReceiptOutput(stdout: string): string | undefined {
  if (Buffer.byteLength(stdout, "utf8") > MAX_RECEIPT_OUTPUT_BYTES ||
      !stdout.endsWith("\n") || stdout.indexOf("\n") !== stdout.length - 1) {
    return undefined;
  }
  const raw = stdout.slice(0, -1);
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  const receipt = value as {
    schema?: unknown;
    version?: unknown;
    outcome?: unknown;
    replayed?: unknown;
    diagnostic?: unknown;
  };
  if (receipt.schema !== "mnemon.agent.receipt" || receipt.version !== 1 ||
      typeof receipt.replayed !== "boolean") return undefined;
  if (receipt.outcome === "accepted") {
    if (!hasExactKeys(receipt, ["outcome", "replayed", "schema", "version"])) return undefined;
  } else if (receipt.outcome === "rejected") {
    if (!hasExactKeys(receipt,
      ["diagnostic", "outcome", "replayed", "schema", "version"]) ||
      typeof receipt.diagnostic !== "string" || receipt.diagnostic.length === 0 ||
      Buffer.byteLength(receipt.diagnostic, "utf8") > MAX_DIAGNOSTIC_BYTES) return undefined;
  } else {
    return undefined;
  }
  return raw;
}

function signalOwnedChild(child: ChildProcess, signal: NodeJS.Signals): void {
  if (child.exitCode === null && child.signalCode === null) child.kill(signal);
}

function submitIntent(encoded: string, signal: AbortSignal): Promise<string> {
  return new Promise((resolve, reject) => {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    let interrupted = false;
    let listeningForAbort = false;
    let child: ChildProcess;
    const interrupt = () => {
      if (interrupted) return;
      interrupted = true;
      signalOwnedChild(child, "SIGTERM");
      killTimer = setTimeout(() => signalOwnedChild(child, "SIGKILL"),
        SUBMIT_SHUTDOWN_GRACE_MS);
      killTimer.unref?.();
    };
    const stdinError = () => interrupt();
    const cleanup = () => {
      if (timeout !== undefined) clearTimeout(timeout);
      if (killTimer !== undefined) clearTimeout(killTimer);
      if (listeningForAbort) signal.removeEventListener("abort", interrupt);
      child.stdin?.off("error", stdinError);
    };
    child = execFile("mnemon", ["agency", "agent", "submit", "--json"], {
      encoding: "utf8",
      maxBuffer: MAX_RECEIPT_OUTPUT_BYTES,
      shell: false,
    }, (error, stdout, stderr) => {
      cleanup();
      const receipt = !interrupted && error === null && stderr === "" ?
        parseReceiptOutput(stdout) : undefined;
      if (interrupted) reject(new SubmitInterruptedError("submit interrupted"));
      else if (receipt === undefined) reject(new Error("submit unavailable"));
      else resolve(receipt);
    });
    timeout = setTimeout(interrupt, SUBMIT_TIMEOUT_MS);
    timeout.unref?.();
    if (signal.aborted) interrupt();
    else {
      signal.addEventListener("abort", interrupt, { once: true });
      listeningForAbort = true;
    }
    if (child.stdin === null) {
      interrupt();
      return;
    }
    child.stdin.once("error", stdinError);
    try {
      child.stdin.end(encoded);
    } catch {
      interrupt();
    }
  });
}

function submitResult(text: string, status: "settled" | "failed" | "input_invalid") {
  return {
    content: [{ type: "text" as const, text }],
    details: { schema: "mnemon.pi.effect", version: 1, status },
  };
}

export default function (pi: ExtensionAPI) {
  let activeBoundary: string | undefined;

  function releaseBoundary(): boolean {
    const boundary = activeBoundary;
    if (boundary === undefined) return true;
    if (!endBoundary(boundary)) return false;
    activeBoundary = undefined;
    return true;
  }

  pi.registerTool({
    name: SUBMIT_TOOL,
    label: "Submit mnemond Intent",
    description:
      "Submit one bounded Intent from the current View. The validated Receipt alone reports its Effect.",
    parameters: SubmitParameters as never,

    async execute(_toolCallId, params, signal) {
      const encoded = intentInput(params?.intent);
      if (encoded === undefined) {
        return submitResult("Invalid bounded Intent object.", "input_invalid");
      }
      try {
        return submitResult(await submitIntent(encoded, signal), "settled");
      } catch {
        return submitResult("Submit unavailable.", "failed");
      }
    },
  });

  pi.on("tool_result", async (event) => {
    if (event.toolName !== SUBMIT_TOOL) return;
    const details = event.details as
      | { schema?: unknown; version?: unknown; status?: unknown }
      | undefined;
    if (details?.schema !== "mnemon.pi.effect" || details.version !== 1 ||
        details.status !== "settled") return { isError: true };
  });

  pi.on("before_agent_start", async () => {
    if (!releaseBoundary()) return undefined;
    const boundary = randomBytes(32).toString("base64url");
    if (!attachBoundary(boundary)) return undefined;
    activeBoundary = boundary;
    return {
      message: {
        customType: "mnemond",
        content: HOOK_CUE,
        display: false,
      },
    };
  });

  pi.on("agent_settled", async () => {
    releaseBoundary();
  });

  pi.on("session_shutdown", async () => {
    releaseBoundary();
  });
}
