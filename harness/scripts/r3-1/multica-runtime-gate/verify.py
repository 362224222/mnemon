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
    wanted = [
        "TestRuntimeProxiesProviderAppServerWhenNoIssueGateIsPresent",
        "TestRuntimeGateDoesNotStartProviderForIssueActivation",
        "TestRuntimeGateUsesTurnIssueTagBeforeStartingProvider",
        "TestRuntimeProviderCommandDefaultsToCodexJSONRPC",
    ]
    assertions = [
        {"name": "gate-focused runtime tests exited successfully", "passed": args.exit_code == 0},
        {"name": "go test selected mnemon-multica-runtime package", "passed": "github.com/mnemon-dev/mnemon/harness/cmd/mnemon-multica-runtime" in log_text},
    ]
    for name in wanted:
        assertions.append({"name": f"{name} executed", "passed": name in log_text})

    failures = []
    if args.exit_code != 0:
        failures.append({
            "category": "test_failure",
            "name": "multica runtime gate tests",
            "detail": f"go test exited {args.exit_code}; see {args.log}",
        })
    status = "ok" if not failures and all(item["passed"] for item in assertions) else "failed"
    summary = {
        "schema_version": 1,
        "phase": "multica-runtime-gate",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": [{
            "name": "go test ./harness/cmd/mnemon-multica-runtime -run <gate-tests>",
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
