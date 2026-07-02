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
    parser.add_argument("--up-exit", required=True, type=int)
    parser.add_argument("--node-a-exit", required=True, type=int)
    parser.add_argument("--node-b-exit", required=True, type=int)
    parser.add_argument("--down-exit", required=True, type=int)
    parser.add_argument("--hub-log", required=True)
    parser.add_argument("--node-a-log", required=True)
    parser.add_argument("--node-b-log", required=True)
    parser.add_argument("--down-log", required=True)
    args = parser.parse_args()

    hub = read(args.hub_log)
    node_a = read(args.node_a_log)
    node_b = read(args.node_b_log)
    all_logs = "\n".join([hub, node_a, node_b, read(args.down_log)])
    assertions = [
        {"name": "docker compose started hub", "passed": args.up_exit == 0},
        {"name": "node-a pushed one synced event", "passed": args.node_a_exit == 0 and '"accepted": 1' in node_a},
        {"name": "node-b pulled at least one synced event", "passed": args.node_b_exit == 0 and '"events": 1' in node_b},
        {"name": "node-b status saw received event", "passed": args.node_b_exit == 0 and '"received": 1' in node_b},
        {"name": "hub audit saw push and pull", "passed": "verb=sync.push result=ok" in hub and "verb=sync.pull result=ok" in hub},
        {"name": "compose cleanup completed", "passed": args.down_exit == 0},
        {"name": "logs do not contain Cloudflare token literal", "passed": "cfat_" not in all_logs},
    ]
    failures = []
    if not all(item["passed"] for item in assertions):
        failures.append({
            "category": "test_failure",
            "name": "docker local MnemonHub sync",
            "detail": f"see {args.hub_log}, {args.node_a_log}, and {args.node_b_log}",
        })
    status = "ok" if not failures else "failed"
    summary = {
        "schema_version": 1,
        "phase": "docker-local",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": [
            {"name": "docker compose up -d hub", "status": "ok" if args.up_exit == 0 else "failed", "exit_code": args.up_exit, "log": args.hub_log},
            {"name": "docker compose run --rm node-a", "status": "ok" if args.node_a_exit == 0 else "failed", "exit_code": args.node_a_exit, "log": args.node_a_log},
            {"name": "docker compose run --rm node-b", "status": "ok" if args.node_b_exit == 0 else "failed", "exit_code": args.node_b_exit, "log": args.node_b_log},
            {"name": "docker compose down -v", "status": "ok" if args.down_exit == 0 else "failed", "exit_code": args.down_exit, "log": args.down_log},
        ],
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
