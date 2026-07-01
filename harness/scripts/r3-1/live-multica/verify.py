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


def load_json(path, fallback):
    try:
        return json.loads(read(path))
    except Exception:
        return fallback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--daemon-log", required=True)
    parser.add_argument("--daemon-exit", required=True, type=int)
    parser.add_argument("--runtimes-log", required=True)
    parser.add_argument("--runtimes-exit", required=True, type=int)
    parser.add_argument("--agents-log", required=True)
    parser.add_argument("--agents-exit", required=True, type=int)
    parser.add_argument("--create-log", required=True)
    parser.add_argument("--create-exit", required=True, type=int)
    parser.add_argument("--runs-log", required=True)
    parser.add_argument("--runs-exit", required=True, type=int)
    parser.add_argument("--messages-log", required=True)
    parser.add_argument("--messages-exit", required=True, type=int)
    parser.add_argument("--meta-log", required=True)
    args = parser.parse_args()

    meta = load_json(args.meta_log, {})
    runtimes = load_json(args.runtimes_log, [])
    agents = load_json(args.agents_log, [])
    messages = read(args.messages_log)
    logs = "\n".join(read(p) for p in [args.daemon_log, args.runtimes_log, args.agents_log, args.create_log, args.runs_log, args.messages_log])
    runtime = next((item for item in runtimes if item.get("id") == meta.get("runtime_id")), {})
    agent = next((item for item in agents if item.get("name") == meta.get("agent_name")), {})
    assertions = [
        {"name": "Multica daemon status is available", "passed": args.daemon_exit == 0 and bool(meta.get("workspace_id"))},
        {"name": "mnemon-runtime is online", "passed": args.runtimes_exit == 0 and runtime.get("status") == "online" and "mnemon-runtime" in str(runtime.get("name", ""))},
        {"name": "mnemon-runtime exposes codex app-server launch", "passed": runtime.get("provider") == "codex" and "app-server" in str(runtime.get("launch_header", ""))},
        {"name": "mnemon-planner agent is bound to mnemon runtime", "passed": args.agents_exit == 0 and agent.get("runtime_id") == meta.get("runtime_id")},
        {"name": "live smoke issue was created", "passed": args.create_exit == 0 and bool(meta.get("issue_id"))},
        {"name": "live smoke issue produced a run", "passed": args.runs_exit == 0 and bool(meta.get("task_id"))},
        {"name": "run messages were readable", "passed": args.messages_exit == 0 and (not messages or "error" not in messages.lower())},
        {"name": "logs do not expose Multica token literal", "passed": "mul_" not in logs},
    ]
    failures = []
    if not all(item["passed"] for item in assertions):
        failures.append({
            "category": "live_failure",
            "name": "live Multica runtime smoke",
            "detail": f"see {args.meta_log}, {args.create_log}, {args.runs_log}, and {args.messages_log}",
        })
    status = "ok" if not failures else "failed"
    summary = {
        "schema_version": 1,
        "phase": "live-multica",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": [
            {"name": "multica daemon status", "status": "ok" if args.daemon_exit == 0 else "failed", "exit_code": args.daemon_exit, "log": args.daemon_log},
            {"name": "multica runtime list", "status": "ok" if args.runtimes_exit == 0 else "failed", "exit_code": args.runtimes_exit, "log": args.runtimes_log},
            {"name": "multica agent list", "status": "ok" if args.agents_exit == 0 else "failed", "exit_code": args.agents_exit, "log": args.agents_log},
            {"name": "multica issue create", "status": "ok" if args.create_exit == 0 else "failed", "exit_code": args.create_exit, "log": args.create_log},
            {"name": "multica issue runs", "status": "ok" if args.runs_exit == 0 else "failed", "exit_code": args.runs_exit, "log": args.runs_log},
            {"name": "multica issue run-messages", "status": "ok" if args.messages_exit == 0 else "failed", "exit_code": args.messages_exit, "log": args.messages_log},
        ],
        "assertions": assertions,
        "skipped": [],
        "failures": failures,
        "metadata": meta,
    }
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    raise SystemExit(0 if status == "ok" else 1)


if __name__ == "__main__":
    main()
