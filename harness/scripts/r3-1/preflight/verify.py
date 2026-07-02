#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def command_available(name):
    return shutil.which(name) is not None


def run_git_status(root):
    proc = subprocess.run(
        ["git", "status", "--short", "--branch"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return {
        "name": "git status --short --branch",
        "status": "ok" if proc.returncode == 0 else "failed",
        "exit_code": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def env_file_check():
    path = Path.home() / ".mnemon" / "cloudflare-bootstrap.env"
    if not path.exists():
        return None, {"category": "skipped_missing_capability", "name": "cloudflare env", "detail": str(path)}
    mode = stat.S_IMODE(path.stat().st_mode)
    ok = mode == 0o600
    assertion = {
        "name": "cloudflare bootstrap env permissions are 0600",
        "passed": ok,
        "detail": oct(mode),
    }
    allowed = {"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "MNEMON_CLOUDFLARE_WORKER_NAME"}
    extra = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip().removeprefix("export ").strip()
        if key not in allowed:
            extra.append(key)
    if extra:
        assertion["passed"] = False
        assertion["detail"] += " unexpected keys: " + ",".join(extra)
    return assertion, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--summary", required=True)
    args = parser.parse_args()

    required = ["go", "python3", "git"]
    optional = ["docker", "wrangler", "multica", "node", "npm"]

    assertions = []
    skipped = []
    failures = []

    for name in required:
        ok = command_available(name)
        assertions.append({"name": f"{name} available", "passed": ok})
        if not ok:
            failures.append({"category": "environment_failure", "name": f"{name} missing", "detail": name})

    for name in optional:
        if not command_available(name):
            skipped.append({"category": "skipped_missing_capability", "name": f"{name} missing", "detail": name})

    env_assertion, env_skip = env_file_check()
    if env_assertion:
        assertions.append(env_assertion)
        if not env_assertion["passed"]:
            failures.append({"category": "environment_failure", "name": env_assertion["name"], "detail": env_assertion["detail"]})
    if env_skip:
        skipped.append(env_skip)

    commands = [run_git_status(args.root)]
    if commands[0]["status"] != "ok":
        failures.append({"category": "environment_failure", "name": "git status failed", "detail": commands[0]["stderr"]})

    status = "ok" if not failures else "failed"
    summary = {
        "schema_version": 1,
        "phase": "preflight",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": commands,
        "assertions": assertions,
        "skipped": skipped,
        "failures": failures,
    }
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    raise SystemExit(0 if status == "ok" else 1)


if __name__ == "__main__":
    main()
