#!/bin/sh
set -eu
exec "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/run_suite.sh" --runtime scripted "$@"
