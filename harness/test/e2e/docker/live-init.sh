#!/bin/sh
set -eu

test "$(id -u)" -eq 0
source_file=/run/secrets/provider_credential
target_home=/run/r5-auth/codex-home
test -f "$source_file"
test -s "$source_file"
install -d -o 10001 -g 10001 -m 0700 "$target_home"
install -o 10001 -g 10001 -m 0600 "$source_file" "$target_home/auth.json"

# The root process exists only for the owner-safe copy. PID 1 becomes the
# unprivileged idle process; every docker exec is also pinned to 10001:10001.
exec setpriv --reuid=10001 --regid=10001 --init-groups /opt/r5/bin/idle
