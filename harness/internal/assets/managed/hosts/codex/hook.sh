#!/bin/sh
set -eu

if cue=$(mnemon-harness hook check); then
	if [ -n "$cue" ]; then
		printf '%s\n' "$cue"
	fi
	exit 0
fi

printf '%s\n' 'mnemon-harness hook check failed; managed Agent execution is blocked' >&2
exit 2
