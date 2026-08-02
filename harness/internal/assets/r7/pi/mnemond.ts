import { execFileSync } from "node:child_process";
import { isAbsolute, normalize } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const HOOK_CUE = "mnemond has bounded state for this runtime; use the installed mnemond guide to inspect it.";
const DEFAULT_EXECUTABLE = "mnemon-harness";
const EXECUTABLE_ENV = "MNEMON_HARNESS_EXECUTABLE";
const SOCKET_ENV = "MNEMON_HARNESS_SOCKET";
const MAX_PATH_BYTES = 4096;
const MAX_OUTPUT_BYTES = 4096;
const ATTACH_TIMEOUT_MS = 1500;

type AttachCommand = {
  executable: string;
  args: string[];
};

function canonicalAbsolutePath(value: string | undefined): string | undefined {
  if (
    value === undefined ||
    value.length === 0 ||
    Buffer.byteLength(value, "utf8") > MAX_PATH_BYTES ||
    value.includes("\0") ||
    !isAbsolute(value) ||
    normalize(value) !== value
  ) {
    return undefined;
  }
  return value;
}

function attachCommand(): AttachCommand | undefined {
  const configuredExecutable = process.env[EXECUTABLE_ENV];
  const executable = configuredExecutable === undefined
    ? DEFAULT_EXECUTABLE
    : canonicalAbsolutePath(configuredExecutable);
  if (executable === undefined) return undefined;

  const configuredSocket = process.env[SOCKET_ENV];
  const socket = configuredSocket === undefined
    ? undefined
    : canonicalAbsolutePath(configuredSocket);
  if (configuredSocket !== undefined && socket === undefined) return undefined;

  const args = ["hook", "attach", "--json"];
  if (socket !== undefined) args.push("--socket", socket);
  return { executable, args };
}

function attachBoundary(): boolean {
  const command = attachCommand();
  if (command === undefined) return false;

  try {
    execFileSync(command.executable, command.args, {
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
