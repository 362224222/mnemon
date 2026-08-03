import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const HOOK_CUE = "mnemond has bounded state for this runtime; use the installed mnemond guide to inspect it.";
const MAX_OUTPUT_BYTES = 4096;
const ATTACH_TIMEOUT_MS = 5000;
const ATTACH_ATTEMPTS = 2;
let activeBoundary: string | undefined;

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
  pi.on("before_agent_start", async () => {
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

  pi.on("session_shutdown", async () => {
    const boundary = activeBoundary;
    activeBoundary = undefined;
    if (boundary !== undefined) endBoundary(boundary);
  });
}
