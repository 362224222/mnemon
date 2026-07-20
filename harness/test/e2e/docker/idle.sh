#!/bin/sh
set -eu

# PID 1 stays intentionally boring. mnemond is started only through the public
# setup/bounded-ensure paths exercised by the test.
trap 'exit 0' TERM INT
while :; do
    sleep 3600 &
    wait "$!"
done
