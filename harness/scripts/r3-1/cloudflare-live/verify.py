#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--exit-code", required=True, type=int)
    parser.add_argument("--workspace", required=True)
    args = parser.parse_args()

    log_text = Path(args.log).read_text(encoding="utf-8", errors="replace")
    failures = []
    missing_workers_dev = "code: 10063" in log_text or "You need a workers.dev subdomain" in log_text
    if args.exit_code != 0 and not missing_workers_dev:
        failures.append({
            "category": "environment_failure",
            "name": "cloudflare live bootstrap",
            "detail": f"bootstrap exited {args.exit_code}; see {args.log}",
        })

    config_path = Path(args.workspace) / ".mnemon" / "harness" / "config.json"
    remotes_path = Path(args.workspace) / ".mnemon" / "harness" / "sync" / "remotes.json"
    assertions = [
        {"name": "mnemon-harness hub bootstrap cloudflare exited successfully", "passed": args.exit_code == 0},
        {"name": "bootstrap output includes connected marker", "passed": "MnemonHub connected" in log_text},
        {"name": "bootstrap output does not contain Cloudflare token literal", "passed": "cfat_" not in log_text and "CLOUDFLARE_API_TOKEN=" not in log_text},
        {"name": "local product config was written", "passed": config_path.exists()},
        {"name": "local sync remote was written", "passed": remotes_path.exists()},
    ]
    skipped = []
    if missing_workers_dev:
        skipped.append({
            "category": "skipped_missing_capability",
            "name": "cloudflare workers.dev subdomain",
            "detail": "Cloudflare API code 10063: open the Workers dashboard once to create the account workers.dev subdomain, then rerun this phase.",
        })
    status = "skipped" if skipped else ("ok" if not failures and all(item["passed"] for item in assertions) else "failed")
    summary = {
        "schema_version": 1,
        "phase": "cloudflare-live",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": [{
            "name": "go run ./harness/cmd/mnemon-harness hub bootstrap cloudflare --root <phase-workspace> --env-file <private-env>",
            "status": "ok" if args.exit_code == 0 else "failed",
            "exit_code": args.exit_code,
            "log": args.log,
        }],
        "assertions": assertions,
        "skipped": skipped,
        "failures": failures,
    }
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    raise SystemExit(0 if status in {"ok", "skipped"} else 1)


if __name__ == "__main__":
    main()
