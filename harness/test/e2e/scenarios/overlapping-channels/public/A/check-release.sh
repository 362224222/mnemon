#!/bin/sh
set -eu

jq -e '.name == "mnemon-example-service" and .api_version == "v2" and .dependency_lock == true' release.json >/dev/null
grep -q '^openapi: 3.1.0$' api.yaml
