#!/usr/bin/env python3
"""Fail-closed Draft 2020-12 validation for R5 scenario/evidence JSON."""

from __future__ import annotations

import json
import pathlib
import sys

try:
    import jsonschema
except ImportError as error:  # pragma: no cover - environment gate
    raise SystemExit(f"r5-e2e: python jsonschema is required: {error}")


def main() -> int:
    check_only = len(sys.argv) == 3 and sys.argv[1] == "--check-schema"
    if len(sys.argv) != 3:
        raise SystemExit("usage: schema_validate.py [--check-schema] SCHEMA|SCHEMA DOCUMENT")
    schema_path = pathlib.Path(sys.argv[2] if check_only else sys.argv[1]).resolve(strict=True)
    with schema_path.open("r", encoding="utf-8") as source:
        schema = json.load(source)
    jsonschema.Draft202012Validator.check_schema(schema)
    if check_only:
        return 0
    document_path = pathlib.Path(sys.argv[2]).resolve(strict=True)
    with document_path.open("r", encoding="utf-8") as source:
        document = json.load(source)
    validator = jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER
    )
    errors = sorted(validator.iter_errors(document), key=lambda item: list(item.path))
    if errors:
        for error in errors[:20]:
            location = "/".join(str(part) for part in error.absolute_path) or "<root>"
            print(f"r5-e2e: {document_path}: {location}: {error.message}", file=sys.stderr)
        if len(errors) > 20:
            print(f"r5-e2e: {len(errors) - 20} more schema errors", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
