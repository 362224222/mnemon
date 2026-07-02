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
    args = parser.parse_args()

    log_text = Path(args.log).read_text(encoding="utf-8", errors="replace")
    failures = []
    if args.exit_code != 0:
        failures.append({
            "category": "protocol_failure",
            "name": "mnemonhub contract go tests",
            "detail": f"go test exited {args.exit_code}; see {args.log}",
        })

    assertions = [
        {"name": "go test ./harness/internal/event ./harness/internal/contract ./harness/internal/mnemonhub ./harness/internal/mnemonhub/... ./harness/internal/coreguard ./harness/internal/productconfig", "passed": args.exit_code == 0},
        {"name": "mnemonhub output does not mention Cloudflare token", "passed": "CLOUDFLARE_API_TOKEN=" not in log_text},
    ]
    status = "ok" if not failures and all(a["passed"] for a in assertions) else "failed"
    summary = {
        "schema_version": 1,
        "phase": "mnemonhub-contract",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": [{
            "name": "go test ./harness/internal/event ./harness/internal/contract ./harness/internal/mnemonhub ./harness/internal/mnemonhub/... ./harness/internal/coreguard ./harness/internal/productconfig",
            "status": "ok" if args.exit_code == 0 else "failed",
            "exit_code": args.exit_code,
            "log": args.log,
        }],
        "assertions": assertions,
        "skipped": [],
        "failures": failures,
    }
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    raise SystemExit(0 if status == "ok" else 1)


if __name__ == "__main__":
    main()
