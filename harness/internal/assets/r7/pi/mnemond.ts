import { execFileSync } from "node:child_process";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const HOOK_CUE = "mnemond has bounded state for this runtime; use the installed mnemond guide to inspect it.";
const MAX_OUTPUT_BYTES = 4096;
const ATTACH_TIMEOUT_MS = 1500;

function attachBoundary(): boolean {
  try {
    execFileSync("mnemon-harness", ["hook", "attach", "--json"], {
      maxBuffer: MAX_OUTPUT_BYTES,
      stdio: ["ignore", "ignore", "ignore"],
      timeout: ATTACH_TIMEOUT_MS,
    });
    return true;
  } catch {
    return false;
  }
}

export default function (pi: ExtensionAPI) {
  pi.on("before_agent_start", async () => {
    if (!attachBoundary()) return undefined;
    return {
      message: {
        customType: "mnemond",
        content: HOOK_CUE,
        display: false,
      },
    };
  });
}
