#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read(path):
    p = Path(path)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8", errors="replace")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--bootstrap-log", required=True)
    parser.add_argument("--bootstrap-exit", required=True, type=int)
    parser.add_argument("--probe-log", required=True)
    parser.add_argument("--probe-exit", required=True, type=int)
    parser.add_argument("--endpoint", required=True)
    args = parser.parse_args()

    bootstrap = read(args.bootstrap_log)
    probe = read(args.probe_log)
    logs = bootstrap + "\n" + probe
    missing_workers_dev = "code: 10063" in bootstrap or "You need a workers.dev subdomain" in bootstrap
    assertions = [
        {"name": "cloudflare bootstrap exited successfully", "passed": args.bootstrap_exit == 0},
        {"name": "bootstrap reported workers.dev endpoint", "passed": args.endpoint.startswith("https://") and args.endpoint.endswith(".workers.dev")},
        {"name": "docker probe pushed one event", "passed": args.probe_exit == 0 and '"accepted": 1' in probe},
        {"name": "docker probe status saw received events", "passed": args.probe_exit == 0 and '"received":' in probe},
        {"name": "logs do not contain Cloudflare API token literal", "passed": "cfat_" not in logs and "CLOUDFLARE_API_TOKEN=" not in logs},
    ]
    skipped = []
    if missing_workers_dev:
        skipped.append({
            "category": "skipped_missing_capability",
            "name": "cloudflare workers.dev subdomain",
            "detail": "Cloudflare API code 10063: account workers.dev subdomain is not enabled.",
        })
    failures = []
    if not skipped and not all(item["passed"] for item in assertions):
        failures.append({
            "category": "test_failure",
            "name": "docker Cloudflare MnemonHub sync",
            "detail": f"see {args.bootstrap_log} and {args.probe_log}",
        })
    status = "skipped" if skipped else ("ok" if not failures else "failed")
    summary = {
        "schema_version": 1,
        "phase": "docker-cloudflare",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": [
            {"name": "mnemon-harness hub bootstrap cloudflare", "status": "ok" if args.bootstrap_exit == 0 else "failed", "exit_code": args.bootstrap_exit, "log": args.bootstrap_log},
            {"name": "docker run golang:1.24 go run docker/probe.go", "status": "ok" if args.probe_exit == 0 else "failed", "exit_code": args.probe_exit, "log": args.probe_log},
        ],
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
