#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_summary(path, phase, status, commands=None, assertions=None, skipped=None, failures=None):
    data = {
        "schema_version": 1,
        "phase": phase,
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": commands or [],
        "assertions": assertions or [],
        "skipped": skipped or [],
        "failures": failures or [],
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--status", required=True, choices=["ok", "failed", "skipped"])
    parser.add_argument("--skip", action="append", default=[])
    parser.add_argument("--failure", action="append", default=[])
    args = parser.parse_args()

    skipped = [{"category": "skipped_missing_capability", "detail": item} for item in args.skip]
    failures = [{"category": "environment_failure", "detail": item} for item in args.failure]
    write_summary(args.summary, args.phase, args.status, skipped=skipped, failures=failures)


if __name__ == "__main__":
    main()
