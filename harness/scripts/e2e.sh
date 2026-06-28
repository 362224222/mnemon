#!/usr/bin/env bash
# End-to-end system acceptance: the full hot path (setup -> local run -> observe(EventDraft) ->
# channel -> intake -> synchronous tick -> rule -> kernel -> event view -> pull/status), plus the
# negative diagnostic case and the refresh no-clobber, for BOTH hosts (codex + claude-code).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d)"
MH="$WORK/mnemon-harness"
PIDFILE="$WORK/run.pid"
cleanup() {
	[ -f "$PIDFILE" ] && kill "$(cat "$PIDFILE")" 2>/dev/null || true
	# the sync-pair stanza runs three background processes; reap any survivor on ANY exit path
	local f
	for f in "$WORK"/*.pid; do
		[ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
	done
	pkill -f "$WORK/mnemon-hub" 2>/dev/null || true
	rm -rf "$WORK"
}
trap cleanup EXIT

echo "building mnemon-harness..."
go build -o "$MH" ./harness/cmd/mnemon-harness

fail() {
	echo "E2E FAIL ($CUR_HOST): $1" >&2
	exit 1
}

run_host() {
	local host="$1" principal="$2" port="$3" configdir="$4"
	CUR_HOST="$host"
	local proj="$WORK/proj-$host"
	mkdir -p "$proj"
	echo "=== E2E host=$host port=$port ==="
	(
		cd "$proj"
		local addr="http://127.0.0.1:$port"
		local tok=".mnemon/harness/channel/credentials/$(printf '%s' "$principal" | tr '@' '-').token"

		"$MH" setup --host "$host" --principal "$principal" --control-url "$addr" >/dev/null

		# start Local Mnemon (creates governed.db on first serve)
		"$MH" local run >"$WORK/run-$host.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"

		# wait until the channel answers a status call
		local up=0 i
		for i in $(seq 1 60); do
			if "$MH" control status --addr "$addr" --principal "$principal" --token-file "$tok" >/dev/null 2>&1; then
				up=1
				break
			fi
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-$host.log"; exit 1; }

		# observe a valid candidate -> synchronous tick admits -> kernel applies
		local out
		out="$("$MH" control observe --addr "$addr" --principal "$principal" --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id m1 \
			--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"E2E progress works for '"$host"'"}}')"
		case "$out" in *ticked=true*) ;; *) echo "observe: $out"; exit 1 ;; esac

		# pull returns the admitted progress event state (one event subject)
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		case "$out" in *event_subjects=1*) ;; *) echo "pull: $out"; exit 1 ;; esac

		# status digest non-empty
		out="$("$MH" control status --addr "$addr" --principal "$principal" --token-file "$tok")"
		case "$out" in *digest=[0-9a-f]*) ;; *) echo "status: $out"; exit 1 ;; esac

		# negative: a secret-like candidate is denied; pull still shows exactly one event subject
		"$MH" control observe --addr "$addr" --principal "$principal" --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id bad1 \
			--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"api_key=sk-abcdefABCDEF123456"}}' >/dev/null
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		case "$out" in *event_subjects=1*) ;; *) echo "negative pull leaked: $out"; exit 1 ;; esac

		# R1: write is immediately visible through render context; no background workspace mirror.
		"$MH" control observe --addr "$addr" --principal "$principal" --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id m2 \
			--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"E2E render context '"$host"'"}}' >/dev/null
		out="$("$MH" control render --addr "$addr" --principal "$principal" --token-file "$tok" --intent context.packet)"
		case "$out" in *"E2E render context $host"*) ;; *) echo "render context missing progress: $out"; exit 1 ;; esac

		# setup no-clobber: hand-edit the generic lifecycle hook, rerun setup, assert the edit is preserved.
		local hook="$configdir/hooks/mnemon-r1/prime.sh"
		printf '# E2E USER EDIT\n\n%s' "$(cat "$hook")" >"$hook.tmp" && mv "$hook.tmp" "$hook"
		"$MH" setup --host "$host" --principal "$principal" --control-url "$addr" >/dev/null
		grep -q "E2E USER EDIT" "$hook" || { echo "setup clobbered standard hook"; exit 1; }

		# stop Local Mnemon and reap it quietly (releases the port + the store lock before the next host)
		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
	) || fail "host flow failed (see $WORK/run-$host.log)"
	sleep 0.3
	echo "    host=$host OK"
}

# run_observe_skill exercises the host skill integration entry point: generate the generic
# mnemon-observe SKILL.md from the live registry. This is an access surface, not a harness event package.
run_observe_skill() {
	local host="$1" principal="$2" addr="http://127.0.0.1:8787"
	CUR_HOST="$host-observe-skill"
	local proj="$WORK/proj-observe-skill-$host"
	mkdir -p "$proj"
	echo "=== E2E observe skill generation ($host) ==="
	(
		cd "$proj"
		"$MH" setup --host "$host" --principal "$principal" --control-url "$addr" >/dev/null
		"$MH" loop observe-skill --write ".mnemon/generated/mnemon-observe" >/dev/null
		grep -q "# mnemon-observe" ".mnemon/generated/mnemon-observe/SKILL.md" || { echo "observe skill header missing"; exit 1; }
		grep -q "progress_digest.write_candidate.observed" ".mnemon/generated/mnemon-observe/SKILL.md" || { echo "observe skill missing progress event"; exit 1; }
		grep -q "assignment.write_candidate.observed" ".mnemon/generated/mnemon-observe/SKILL.md" || { echo "observe skill missing assignment event"; exit 1; }
	) || fail "observe skill generation failed"
	sleep 0.3
	echo "    observe skill generation ($host) OK"
}

# run_note proves the platform claim on the PRODUCT path (note and decision fixtures)
# via the EXTERNAL-PACKAGE route: neither is a standard event package, so each stands up from a
# .mnemon/loops/<name>/capability.json package directory plus the SAME config.loops +
# bindings.json edit (the run_external_goal mechanism; supply path changed, admission semantics
# unchanged). setup still fail-closes `--loop note` (external packages carry no host assets), so
# the stanza does what a platform operator would: lay the packages, then edit the setup-written
# config.json loops list + bindings.json scope.
run_note() {
	local principal="codex@project" addr="http://127.0.0.1:8787"
	CUR_HOST="note-external-package"
	local proj="$WORK/proj-note"
	mkdir -p "$proj"
	echo "=== E2E note+decision external event packages ==="
	(
		cd "$proj"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal "$principal" --control-url "$addr" >/dev/null

		# The external packages: directory presence = event package declaration.
		# capability.json remains the external adapter filename for compatibility.
		mkdir -p .mnemon/loops/note .mnemon/loops/decision
		cat >.mnemon/loops/note/capability.json <<-'JSONEOF'
		{
		  "schema_version": 2,
		  "name": "note",
		  "observed_type": "note.write_candidate.observed",
		  "proposed_type": "note.write.proposed",
		  "resource_kind": "note",
		  "items_field": "items",
		  "fields": [
		    {
		      "section": "narrative",
		      "name": "text",
		      "validators": [
		        {"id": "required", "params": {"missing_style": "empty"}},
		        {"id": "safety:unsafe"}
		      ]
		    }
		  ],
		  "render": {
		    "content": {"member": "bullet-list", "params": {"title": "# Notes", "field": "text"}}
		  }
		}
		JSONEOF
		cat >.mnemon/loops/decision/capability.json <<-'JSONEOF'
		{
		  "schema_version": 2,
		  "name": "decision",
		  "observed_type": "decision.write_candidate.observed",
		  "proposed_type": "decision.write.proposed",
		  "resource_kind": "decision",
		  "items_field": "items",
		  "fields": [
		    {
		      "section": "narrative",
		      "name": "text",
		      "validators": [
		        {"id": "required", "params": {"missing_style": "empty"}},
		        {"id": "safety:unsafe"}
		      ]
		    }
		  ],
		  "render": {
		    "content": {"member": "bullet-list", "params": {"title": "# Decisions", "field": "text"}}
		  }
		}
		JSONEOF

		# The config edit: enable the note/decision loops + widen the binding to their types/scopes.
		python3 - <<-'PYEOF'
		import json
		cfg = json.load(open(".mnemon/harness/local/config.json"))
		cfg["loops"].append("note")
		cfg["loops"].append("decision")
		json.dump(cfg, open(".mnemon/harness/local/config.json", "w"), indent=2)
		doc = json.load(open(".mnemon/harness/channel/bindings.json"))
		b = doc["bindings"][0]
		b["allowed_observed_types"].append("note.write_candidate.observed")
		b["subscription_scope"].append({"kind": "note", "id": "project"})
		b["allowed_observed_types"].append("decision.write_candidate.observed")
		b["subscription_scope"].append({"kind": "decision", "id": "project"})
		json.dump(doc, open(".mnemon/harness/channel/bindings.json", "w"), indent=2)
		PYEOF

		"$MH" local run >"$WORK/run-note.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"
		local up=0 i
		for i in $(seq 1 60); do
			if "$MH" control status --addr "$addr" --principal "$principal" --token-file "$tok" >/dev/null 2>&1; then
				up=1
				break
			fi
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-note.log"; exit 1; }

		# `event_subjects=N` counts written event subjects, so digest checks remain the stricter proof here.
		# The content digest folds Kind:ID:Version+fields per scoped ref: an admitted note write
		# necessarily changes it. ticked=true + digest delta = the note landed (admitted through
		# the EXTERNAL note rule — note is no longer embedded, so no builtin could fake this).
		local out pre post
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		pre="${out##*digest=}"; pre="${pre%% *}"
		out="$("$MH" control observe --addr "$addr" --principal "$principal" --token-file "$tok" \
			--type note.write_candidate.observed --external-id n1 \
			--payload '{"narrative":{"text":"note stands up via config alone"}}')"
		case "$out" in *ticked=true*) ;; *) echo "note observe: $out"; exit 1 ;; esac
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		post="${out##*digest=}"; post="${post%% *}"
		[ -n "$pre" ] && [ -n "$post" ] && [ "$pre" != "$post" ] || { echo "note write did not change the scoped digest (pre=$pre post=$post)"; exit 1; }

		# 阶段二(P1 降级后):第四能力 decision —— 外部包 spec 文件 + KindCatalog/SchemaGuard
		# 各一行 kind 注册,零新增行为代码。
		out="$("$MH" control observe --addr "$addr" --principal "$principal" --token-file "$tok" \
			--type decision.write_candidate.observed --external-id d1 \
			--payload '{"narrative":{"text":"decision stands up from a spec file"}}')"
		case "$out" in *ticked=true*) ;; *) echo "decision observe: $out"; exit 1 ;; esac
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		post2="${out##*digest=}"; post2="${post2%% *}"
		[ -n "$post2" ] && [ "$post2" != "$post" ] || { echo "decision write did not change the scoped digest"; exit 1; }

		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
	) || fail "note flow failed (see $WORK/run-note.log)"
	sleep 0.3
	echo "    note+decision external packages OK"
}

# run_external_goal proves stage 5 on the product path: an event package that never had a standard kind
# registration in code (goal) stands up from a pure external package directory
# (.mnemon/loops/goal/capability.json) + the SAME config.loops/binding edit the note/decision
# external packages use — admission-equal rights. Includes the governed pull CONTENT leg (the
# goal text arrives via the pull verb, not only a digest delta) and the negative path: a
# malformed second package REFUSES `local run` boot, naming its path on stderr.
run_external_goal() {
	local principal="codex@project" addr="http://127.0.0.1:8787"
	CUR_HOST="external-goal"
	local proj="$WORK/proj-external-goal"
	mkdir -p "$proj"
	echo "=== E2E external goal event package ==="
	(
		cd "$proj"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal "$principal" --control-url "$addr" >/dev/null

		# The external package: directory presence = event package declaration.
		mkdir -p .mnemon/loops/goal
		cat >.mnemon/loops/goal/capability.json <<-'JSONEOF'
		{
		  "schema_version": 2,
		  "name": "goal",
		  "observed_type": "goal.write_candidate.observed",
		  "proposed_type": "goal.write.proposed",
		  "resource_kind": "goal",
		  "items_field": "items",
		  "fields": [
		    {
		      "section": "narrative",
		      "name": "statement",
		      "validators": [
		        {"id": "required", "params": {"missing_style": "empty"}},
		        {"id": "safety:unsafe"}
		      ]
		    }
		  ],
		  "render": {
		    "content": {"member": "bullet-list", "params": {"title": "# Goals", "field": "statement"}},
		    "static": {"statement": "project"}
		  }
		}
		JSONEOF

		# The enablement edit — EXACTLY isomorphic to the note/decision external packages:
		# config.loops + binding scope/types (config.loops stays the product-path authority).
		python3 - <<-'PYEOF'
		import json
		cfg = json.load(open(".mnemon/harness/local/config.json"))
		cfg["loops"].append("goal")
		json.dump(cfg, open(".mnemon/harness/local/config.json", "w"), indent=2)
		doc = json.load(open(".mnemon/harness/channel/bindings.json"))
		b = doc["bindings"][0]
		b["allowed_observed_types"].append("goal.write_candidate.observed")
		b["subscription_scope"].append({"kind": "goal", "id": "project"})
		json.dump(doc, open(".mnemon/harness/channel/bindings.json", "w"), indent=2)
		PYEOF

		"$MH" local run >"$WORK/run-external-goal.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"
		local up=0 i
		for i in $(seq 1 60); do
			if "$MH" control status --addr "$addr" --principal "$principal" --token-file "$tok" >/dev/null 2>&1; then
				up=1
				break
			fi
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-external-goal.log"; exit 1; }

		# observe -> synchronous tick admits through the EXTERNAL rule (goal is not embedded, so
		# there is no builtin fallback that could fake this) -> scoped digest delta.
		local out pre post
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		pre="${out##*digest=}"; pre="${pre%% *}"
		out="$("$MH" control observe --addr "$addr" --principal "$principal" --token-file "$tok" \
			--type goal.write_candidate.observed --external-id g1 \
			--payload '{"narrative":{"statement":"ship stage five"}}')"
		case "$out" in *ticked=true*) ;; *) echo "goal observe: $out"; exit 1 ;; esac
		out="$("$MH" control pull --addr "$addr" --principal "$principal" --token-file "$tok")"
		post="${out##*digest=}"; post="${post%% *}"
		[ -n "$pre" ] && [ -n "$post" ] && [ "$pre" != "$post" ] || { echo "goal write did not change the scoped digest (pre=$pre post=$post)"; exit 1; }

		# Governed pull CONTENT leg: the goal statement itself arrives via the pull verb
		# (control pull --json emits the scoped event view subjects + fields).
		"$MH" control pull --json --addr "$addr" --principal "$principal" --token-file "$tok" \
			| grep -q "ship stage five" || { echo "goal content did not arrive via the governed pull verb"; exit 1; }

		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
		sleep 0.3

		# NEGATIVE: a malformed second package must REFUSE boot (directory presence is contract;
		# split streams — the "ready" banner goes to stdout, the refusal names the path on stderr).
		# Background launch + bounded poll: a foreground run would HANG the suite if fail-closed
		# ever regressed into a serving process, so the refusal must arrive within ~6s or the
		# process is killed and the leg fails.
		mkdir -p .mnemon/loops/bad
		printf '{nope' >.mnemon/loops/bad/capability.json
		"$MH" local run >"$WORK/external-bad.out.log" 2>"$WORK/external-bad.err.log" &
		local badpid=$! refused=0
		for i in $(seq 1 60); do
			if ! kill -0 "$badpid" 2>/dev/null; then
				refused=1
				break
			fi
			sleep 0.1
		done
		if [ "$refused" != 1 ]; then
			kill "$badpid" 2>/dev/null || true
			wait "$badpid" 2>/dev/null || true
			echo "local run still alive after 6s with a malformed external package (fail-closed regressed)"; exit 1
		fi
		if wait "$badpid"; then
			echo "local run must exit non-zero with a malformed external package"; exit 1
		fi
		grep -q ".mnemon/loops/bad" "$WORK/external-bad.err.log" || { echo "boot refusal must name the bad package path on stderr"; cat "$WORK/external-bad.err.log"; exit 1; }
		rm -rf .mnemon/loops/bad

		# NEGATIVE (loop-package-v2): an external package may carry host assets, but the hook-fragment
		# CODE face stays embedded-only — a package shipping hooks/fragments/ must REFUSE boot, naming
		# its path. Same bounded-poll pattern (a fail-closed regression must not hang the suite).
		mkdir -p .mnemon/loops/frag/hooks/fragments
		cp .mnemon/loops/goal/capability.json .mnemon/loops/frag/capability.json
		sed -i.bak 's/goal/frag/g' .mnemon/loops/frag/capability.json && rm -f .mnemon/loops/frag/capability.json.bak
		printf 'echo pwned\n' >.mnemon/loops/frag/hooks/fragments/x.sh
		"$MH" local run >"$WORK/external-frag.out.log" 2>"$WORK/external-frag.err.log" &
		local fragpid=$! fragrefused=0
		for i in $(seq 1 60); do
			if ! kill -0 "$fragpid" 2>/dev/null; then fragrefused=1; break; fi
			sleep 0.1
		done
		if [ "$fragrefused" != 1 ]; then
			kill "$fragpid" 2>/dev/null || true; wait "$fragpid" 2>/dev/null || true
			echo "local run still alive with an external hooks/fragments/ package (code-face gate regressed)"; exit 1
		fi
		if wait "$fragpid"; then echo "local run must exit non-zero with an external fragments package"; exit 1; fi
		grep -q "hooks/fragments/" "$WORK/external-frag.err.log" || { echo "boot refusal must name the forbidden fragments face"; cat "$WORK/external-frag.err.log"; exit 1; }
		rm -rf .mnemon/loops/frag
	) || fail "external goal flow failed (see $WORK/run-external-goal.log)"
	sleep 0.3
	echo "    external goal package OK"
}

# run_foo_external proves an external package added via `loop add` can be enabled on the R1 setup
# path as event package scope without projecting host assets.
run_foo_external() {
	CUR_HOST="foo-external"
	local proj="$WORK/proj-foo"
	mkdir -p "$proj"
	echo "=== E2E external package setup scope (foo) ==="
	(
		cd "$proj"
		"$MH" setup --host codex --principal codex@project --control-url http://127.0.0.1:8787 >/dev/null

		# Author writes a package DIRECTORY, then registers it via the product front door
		# (`loop add`) — the minimal-onboarding path (P2): copy under the canonical name + validate
		# through the same fail-closed boot resolution. No hand-placement into .mnemon/loops.
		mkdir -p src/foo/skills/foo-set
		cat >src/foo/capability.json <<-'JSONEOF'
		{"schema_version":2,"name":"foo","observed_type":"foo.write_candidate.observed",
		"proposed_type":"foo.write.proposed","resource_kind":"foo","items_field":"items",
		"fields":[{"section":"narrative","name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}}]}],
		"render":{"content":{"member":"bullet-list","params":{"title":"# Foo","field":"text"}}}}
		JSONEOF
		cat >src/foo/loop.json <<-'JSONEOF'
		{"schema_version":2,"name":"foo",
		"surfaces":{"projection":[],"observation":[]},
		"assets":{"guide":"GUIDE.md","env":"env.sh","skills":["skills/foo-set/SKILL.md"],"subagents":[]}}
		JSONEOF
		printf '# Foo\n\nA declarative external loop package.\n' >src/foo/GUIDE.md
		printf '#!/usr/bin/env bash\n' >src/foo/env.sh
		printf 'Use this to record a foo. Reject vague entries.\n' >src/foo/skills/foo-set/SKILL.md
		"$MH" loop add src/foo >"$WORK/foo-add.log" 2>&1 || { echo "loop add foo failed"; cat "$WORK/foo-add.log"; exit 1; }
		[ -f .mnemon/loops/foo/capability.json ] || { echo "loop add did not place foo under .mnemon/loops"; exit 1; }
		[ -f .mnemon/loops/foo/skills/foo-set/SKILL.md ] || { echo "loop add did not copy the package subtree"; exit 1; }

		# Enable foo for the host. R1 setup grants event package scope and keeps host assets static.
		"$MH" setup --host codex --loop foo --principal codex@project --control-url http://127.0.0.1:8787 >"$WORK/foo-codex.log" 2>&1 \
			|| { echo "setup --loop foo (codex) failed"; cat "$WORK/foo-codex.log"; exit 1; }

		[ ! -e .codex/mnemon-foo/GUIDE.md ] || { echo "foo GUIDE must not be projected to codex runtime surface"; exit 1; }
		[ ! -e .codex/skills/foo-set/SKILL.md ] || { echo "foo skill must not be projected to codex"; exit 1; }
		grep -q "foo.write_candidate.observed" .mnemon/harness/channel/bindings.json \
			|| { echo "foo grant missing from binding"; exit 1; }

		# Discoverability (PD7): the generic mnemon-observe skill is generated from the live catalog,
		# so a freshly-added external kind appears in its mechanism section without any per-kind code.
		"$MH" loop observe-skill | grep -q "foo.write_candidate.observed" \
			|| { echo "observe-skill did not reflect the external foo kind"; exit 1; }
		"$MH" loop packages | grep -q "^foo " || { echo "loop packages missing foo"; exit 1; }

		"$MH" local run >"$WORK/run-foo.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		local up=0 i out
		for i in $(seq 1 60); do
			"$MH" control status --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" >/dev/null 2>&1 && { up=1; break; }
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-foo.log"; exit 1; }
		out="$("$MH" control observe --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" \
			--type foo.write_candidate.observed --external-id foo1 --payload '{"narrative":{"text":"foo governed by external package"}}')"
		case "$out" in *ticked=true*) ;; *) echo "foo observe: $out"; exit 1 ;; esac
		out="$("$MH" control pull --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok")"
		case "$out" in *event_subjects=1*) ;; *) echo "foo pull: $out"; exit 1 ;; esac
		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
	) || fail "foo external setup failed"
	sleep 0.3
	echo "    external package setup scope (foo) OK"
}

# Both hosts run sequentially (the server is stopped between them). codex stays on the default
# port (covering the bare default path); claude-code deliberately runs on a NON-default port to
# pin the stage-0 promise that a bare `local run` listens where setup's --control-url pointed.

# write_journal_pkg installs an EXTERNAL, declared-kind event package ("journal") into a
# project's .mnemon/loops. journal uses the generic entry list shape (items_field "entries",
# entry-list render) and opts into Remote Workspace import via the closed-set entry-dedup strategy
# — a kind whose name appears NOWHERE in the platform code. It is the PD6 proof object: that a novel kind
# syncs end-to-end (produce -> hub accept -> pull -> import) purely by declaring sync.importable in
# its descriptor, exercising the descriptor-derived produce surface (RuntimeConfig.SyncableKinds),
# the grant-scope hub accept, and the catalog-derived import dispatch.
write_journal_pkg() {
	local dir="$1"
	mkdir -p "$dir/.mnemon/loops/journal"
	cat >"$dir/.mnemon/loops/journal/capability.json" <<-'JSONEOF'
	{"schema_version":2,"name":"journal","observed_type":"journal.write_candidate.observed",
	"proposed_type":"journal.write.proposed","resource_kind":"journal","items_field":"entries",
	"fields":[{"section":"narrative","name":"content","validators":[{"id":"required","params":{"missing_style":"empty"}},{"id":"safety:secret"},{"id":"safety:injection"}]},
	{"section":"rule","name":"source","validators":[{"id":"required","params":{"missing_style":"missing"}}]},
	{"section":"rule","name":"confidence","validators":[{"id":"required","params":{"missing_style":"missing"}}]}],
	"render":{"content":{"member":"entry-list"}},
	"sync":{"importable":true,"merge":"entry-dedup"}}
	JSONEOF
	cat >"$dir/.mnemon/loops/journal/loop.json" <<-'JSONEOF'
	{"schema_version":2,"name":"journal","surfaces":{"projection":[],"observation":[]},
	"assets":{"guide":"GUIDE.md","env":"env.sh","skills":[],"subagents":[]}}
	JSONEOF
	printf '# Journal\n\nA declared external loop that syncs across replicas.\n' >"$dir/.mnemon/loops/journal/GUIDE.md"
	printf '#!/usr/bin/env bash\n' >"$dir/.mnemon/loops/journal/env.sh"
}

# run_sync_pair proves the stage-6 Remote MVP on the product path: two replicas (A, B) sync
# through a standalone mnemon-hub over TLS — A writes, the in-process sync worker pushes, B's
# worker pulls and the content arrives via B's governed pull (attribution carried end to end).
# It carries three kinds: embedded progress_digest, external journal, and embedded assignment. The
# journal round-trip is the PD6 proof that the descriptor-derived sync path is kind-agnostic (no kind
# literal anywhere on the produce/accept/import surfaces), while assignment proves generic item-dedup.
# Offline leg pins I13 (hub down = local fully functional); the bad-token leg pins authn on the
# wire. Conflict adjudication (hub idempotency + B-side import conflict) is pinned at the Go
# integration layer (mnemonhub tests, sync_import_test.go) per the v1.1 redefinition.
run_sync_pair() {
	CUR_HOST="sync-pair"
	echo "=== E2E sync pair via mnemon-hub (TLS) ==="
	local hubdir="$WORK/hub" tlsdir="$WORK/synctls"
	mkdir -p "$hubdir" "$tlsdir"

	go build -o "$WORK/mnemon-hub" ./harness/cmd/mnemon-hub

	"$WORK/mnemon-hub" --dev-selfsigned "$tlsdir" >/dev/null
	[ -f "$tlsdir/cert.pem" ] && [ -f "$tlsdir/key.pem" ] || fail "dev-selfsigned did not write cert/key"

	# hub credentials: two replicas, distinct principals (multi-replica acceptance).
	printf '%s\n' "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666" >"$hubdir/replica-a.token"
	printf '%s\n' "9999aaaa8888bbbb7777cccc6666dddd5555eeee4444ffff" >"$hubdir/replica-b.token"
	chmod 600 "$hubdir/replica-a.token" "$hubdir/replica-b.token"
	cat >"$hubdir/replicas.json" <<-'JSON'
	{
	  "schema_version": 1,
	  "replicas": [
	    {"principal": "replica-a@hub", "credential_ref": "replica-a.token",
	     "scopes": [{"kind": "progress_digest", "id": "project"}, {"kind": "journal", "id": "project"}, {"kind": "assignment", "id": "project"}]},
	    {"principal": "replica-b@hub", "credential_ref": "replica-b.token",
	     "scopes": [{"kind": "progress_digest", "id": "project"}, {"kind": "journal", "id": "project"}, {"kind": "assignment", "id": "project"}]}
	  ]
	}
	JSON
	chmod 600 "$hubdir/replicas.json"

	"$WORK/mnemon-hub" --addr 127.0.0.1:9787 --store "$hubdir/hub.db" --replicas "$hubdir/replicas.json" \
		--tls-cert "$tlsdir/cert.pem" --tls-key "$tlsdir/key.pem" >"$WORK/mnemon-hub.log" 2>&1 &
	local hubpid=$!
	sleep 0.5
	kill -0 "$hubpid" 2>/dev/null || { cat "$WORK/mnemon-hub.log"; fail "mnemon-hub did not start"; }

	local proja="$WORK/proj-sync-a" projb="$WORK/proj-sync-b"
	mkdir -p "$proja" "$projb"
	write_journal_pkg "$proja"
	write_journal_pkg "$projb"
	local apid="" bpid=""
	(
		cd "$proja"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal codex@project --control-url http://127.0.0.1:8787 >/dev/null
		"$MH" setup --host codex --loop journal --principal codex@project --control-url http://127.0.0.1:8787 >/dev/null
		"$MH" sync connect hub --remote-url https://127.0.0.1:9787 \
			--token-file "$hubdir/replica-a.token" --ca-file "$tlsdir/cert.pem" >/dev/null
		"$MH" local run --sync-interval 100ms >"$WORK/run-sync-a.log" 2>&1 &
		echo $! >"$WORK/sync-a.pid"
		local up=0 i
		for i in $(seq 1 60); do
			"$MH" control status --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" >/dev/null 2>&1 && { up=1; break; }
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-sync-a.log"; exit 1; }
		"$MH" control observe --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id sp1 \
			--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"sync pair payload from replica A"}}' >/dev/null
		# journal (external declared kind): the PD6 kind-agnostic produce surface emits a synced event
		# for it exactly because its descriptor declares sync.importable — no kind literal in code.
		"$MH" control observe --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" \
			--type journal.write_candidate.observed --external-id jp1 \
			--payload '{"rule":{"source":"user","confidence":"high"},"narrative":{"content":"journal entry from replica A"}}' >/dev/null
		# assignment (embedded coordination kind, item-dedup merge): the §577 generic append-merge
		# syncs a kind whose items carry arbitrary fields (scope/ttl/assignee/work/feedback), preserving them all.
		"$MH" control observe --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" \
			--type assignment.write_candidate.observed --external-id ap1 \
			--payload '{"rule":{"scope":"assignment from replica A","ttl":"2h","assignee":"codex@impl"},"narrative":{"expected_work":"act on assignment from replica A","expected_feedback":"progress_digest with result or blocker"},"refs":{"evidence_refs":["ticket-7"]}}' >/dev/null
	) || fail "replica A flow failed (see $WORK/run-sync-a.log / $WORK/mnemon-hub.log)"
	apid="$(cat "$WORK/sync-a.pid")"

	(
		cd "$projb"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal codex@project --control-url http://127.0.0.1:8899 >/dev/null
		"$MH" setup --host codex --loop journal --principal codex@project --control-url http://127.0.0.1:8899 >/dev/null
		"$MH" sync connect hub --remote-url https://127.0.0.1:9787 \
			--token-file "$hubdir/replica-b.token" --ca-file "$tlsdir/cert.pem" >/dev/null
		"$MH" local run --sync-interval 100ms >"$WORK/run-sync-b.log" 2>&1 &
		echo $! >"$WORK/sync-b.pid"
		local up=0 i seen=0 jseen=0 aseen=0
		for i in $(seq 1 60); do
			"$MH" control status --addr http://127.0.0.1:8899 --principal codex@project --token-file "$tok" >/dev/null 2>&1 && { up=1; break; }
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-sync-b.log"; exit 1; }
		# A worker pushes -> hub -> B worker pulls -> import re-enters intake -> governed pull sees it.
		# progress_digest (item-dedup) + journal (external, entry-dedup) + assignment (coordination, item-dedup)
		# must all arrive — three kinds, three descriptor-selected merge strategies, no kind literal.
		for i in $(seq 1 100); do
			local bpull
			bpull="$("$MH" control pull --json --addr http://127.0.0.1:8899 --principal codex@project --token-file "$tok" 2>/dev/null)"
			case "$bpull" in *"sync pair payload from replica A"*) seen=1 ;; esac
			case "$bpull" in *"journal entry from replica A"*) jseen=1 ;; esac
			case "$bpull" in *"assignment from replica A"*) aseen=1 ;; esac
			[ "$seen" = 1 ] && [ "$jseen" = 1 ] && [ "$aseen" = 1 ] && break
			sleep 0.2
		done
		# Diagnosable-flake margin (LOW-11): assert the hub actually RECEIVED A's push, separately
		# from B's pull arriving. A flake then reads as "push never arrived" (received=0) vs "pull
		# never ran" (received>=1 but B unseen) instead of one opaque timeout. /sync/status accepts
		# GET (frozen verb-method map); replica-a's token authorizes it over the pinned TLS cert.
		local hubstatus
		hubstatus="$(curl -sS --cacert "$tlsdir/cert.pem" \
			-H "Authorization: Bearer $(tr -d '\n' <"$hubdir/replica-a.token")" \
			https://127.0.0.1:9787/sync/status 2>/dev/null)"
		case "$hubstatus" in
			*'"hub_events_received":0'*|'') echo "hub never received A's push (status: ${hubstatus:-<empty>})"; tail -5 "$WORK/run-sync-b.log"; exit 1 ;;
			*'"hub_events_received":'*) ;;
			*) echo "unexpected hub status: $hubstatus"; exit 1 ;;
		esac
		[ "$seen" = 1 ] || { echo "B never saw A's progress event within 20s (hub received the push: $hubstatus -> pull side failed)"; tail -5 "$WORK/run-sync-b.log"; exit 1; }
		[ "$jseen" = 1 ] || { echo "B never saw A's external journal event within 20s (descriptor-derived sync path failed for a declared kind)"; tail -5 "$WORK/run-sync-b.log"; exit 1; }
		[ "$aseen" = 1 ] || { echo "B never saw A's assignment event within 20s (item-dedup coordination sync failed)"; tail -5 "$WORK/run-sync-b.log"; exit 1; }
		# attribution: the import preserves A's entries VERBATIM (faithful provenance) and the
		# write itself is attributed to the sync importer; the full origin chain (replica id,
		# decision id) lives in B's event log + decisions, pinned by sync_import Go tests.
		"$MH" control pull --json --addr http://127.0.0.1:8899 --principal codex@project --token-file "$tok" | grep -q '"sync@local"' \
			|| { echo "imported event subject lacks sync@local attribution"; exit 1; }
	) || fail "replica B flow failed (see $WORK/run-sync-b.log / $WORK/mnemon-hub.log)"
	bpid="$(cat "$WORK/sync-b.pid")"

	# offline leg (I13): hub down, A stays fully functional on the product path.
	kill "$hubpid" 2>/dev/null; wait "$hubpid" 2>/dev/null || true
	(
		cd "$proja"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		out="$("$MH" control observe --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id sp-offline \
			--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"offline write while hub is down"}}')"
		case "$out" in *ticked=true*) ;; *) echo "offline observe: $out"; exit 1 ;; esac
		"$MH" control pull --addr http://127.0.0.1:8787 --principal codex@project --token-file "$tok" >/dev/null
	) || fail "I13 offline leg failed"

	# authn leg: with A's server stopped (lock released), a manual push with the WRONG token is refused.
	{ kill "$apid" 2>/dev/null; wait "$apid"; } 2>/dev/null || true
	{ kill "$bpid" 2>/dev/null; wait "$bpid"; } 2>/dev/null || true
	"$WORK/mnemon-hub" --addr 127.0.0.1:9787 --store "$hubdir/hub.db" --replicas "$hubdir/replicas.json" \
		--tls-cert "$tlsdir/cert.pem" --tls-key "$tlsdir/key.pem" >>"$WORK/mnemon-hub.log" 2>&1 &
	hubpid=$!
	sleep 0.5
	(
		cd "$proja"
		# sp-offline is still pending (hub was down when it was written), so the push really
		# sends a request. The stored credential is what the client uses - corrupt it for the
		# negative, restore for the positive (true product-path authn probe).
		# connect stored the absolute --token-file path as credential_ref; mnemon-hub loaded the
		# token into memory at boot, so editing the file flips only the CLIENT side.
		local cred="$hubdir/replica-a.token"
		cp "$cred" "$WORK/cred.bak"
		printf '%s\n' "000000000000000000000000000000000000000000000000" >"$cred"
		if "$MH" sync push --once >/dev/null 2>&1; then
			echo "unknown-token push must be refused"; exit 1
		fi
		cp "$WORK/cred.bak" "$cred"
		"$MH" sync push --once >/dev/null 2>&1 || { echo "right-token push must succeed"; exit 1; }
	) || fail "authn leg failed"
	kill "$hubpid" 2>/dev/null; wait "$hubpid" 2>/dev/null || true
	rm -f "$PIDFILE"
	echo "    sync pair via mnemon-hub OK"
}

# run_daemon proves the local governance daemon lifecycle (PD8 / P2 acceptance "守护进程生命周期 e2e"):
# `mnemond up` detaches a serving process (pidfile + log under .mnemon/harness/local), status/logs
# reflect it, the DETACHED daemon governs a real observe over the channel, and `down` stops it and
# cleans the pidfile. The bare/foreground serve face (`local run`) is unchanged and proven elsewhere.
run_daemon() {
	CUR_HOST="daemon"
	local proj="$WORK/proj-daemon" addr="127.0.0.1:8788"
	mkdir -p "$proj"
	echo "=== E2E mnemond daemon lifecycle ==="
	go build -o "$WORK/mnemond" ./harness/cmd/mnemond
	(
		cd "$proj"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal codex@project --control-url "http://$addr" >/dev/null

		"$WORK/mnemond" status --root . | grep -q "stopped" || { echo "status before up must be stopped"; exit 1; }
		"$WORK/mnemond" up --root . --addr "$addr" >"$WORK/daemon-up.log" 2>&1 \
			|| { echo "mnemond up failed"; cat "$WORK/daemon-up.log"; exit 1; }
		# register the detached pid for the cleanup trap (own session, not a $WORK-tracked child)
		cp .mnemon/harness/local/mnemond.pid "$WORK/daemon.pid" 2>/dev/null || true
		"$WORK/mnemond" status --root . | grep -q "running" \
			|| { echo "status after up must be running"; "$WORK/mnemond" logs --root .; exit 1; }
		"$WORK/mnemond" logs --root . | grep -q "Local Mnemon: ready" \
			|| { echo "logs must show the serve banner"; exit 1; }
		# a second up over a live daemon must refuse
		if "$WORK/mnemond" up --root . --addr "$addr" >/dev/null 2>&1; then
			echo "a second up over a live daemon must refuse"; exit 1
		fi

		# the DETACHED daemon governs a real observe over the channel
		local out
		out="$("$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id d1 \
			--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"daemon governs this"}}')"
		case "$out" in *ticked=true*) ;; *) echo "daemon observe: $out"; exit 1 ;; esac

		"$WORK/mnemond" down --root . >/dev/null || { echo "mnemond down failed"; exit 1; }
		"$WORK/mnemond" status --root . | grep -q "stopped" || { echo "status after down must be stopped"; exit 1; }
		[ ! -f .mnemon/harness/local/mnemond.pid ] || { echo "down must remove the pidfile"; exit 1; }
	) || fail "daemon lifecycle failed (see $WORK/daemon-up.log)"
	rm -f "$WORK/daemon.pid"
	echo "    mnemond daemon lifecycle OK"
}

# run_coordination proves the AgentTeam coordination package is default-enabled (P3b): `setup --host
# codex` with NO --loop wires a host that governs project_intent/assignment/progress_digest out of the
# box — the §3.7 row-A 普通使用者 flow. No coordination kind is named anywhere on the setup line.
run_coordination() {
	CUR_HOST="coordination"
	local proj="$WORK/proj-coord" addr="127.0.0.1:8790"
	mkdir -p "$proj"
	echo "=== E2E coordination kinds default-enabled (no --loop) ==="
	(
		cd "$proj"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal codex@project --control-url "http://$addr" >/dev/null
		"$MH" local run >"$WORK/run-coord.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"
		local up=0 i
		for i in $(seq 1 60); do
			"$MH" control status --addr "http://$addr" --principal codex@project --token-file "$tok" >/dev/null 2>&1 && { up=1; break; }
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-coord.log"; exit 1; }
		# all three coordination kinds govern (observe → admit) with no --loop having named them
		local out
		# project_intent + assignment are mid-risk (P3c): the candidate must carry evidence.
		out="$("$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type project_intent.write_candidate.observed --external-id ci1 --payload '{"narrative":{"statement":"ship the AgentTeam beta","evidence_summary":"roadmap-q3"},"refs":{"evidence_refs":["roadmap-q3"]}}')"
		case "$out" in *ticked=true*) ;; *) echo "project_intent observe: $out"; exit 1 ;; esac
		out="$("$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type assignment.write_candidate.observed --external-id ci2 --payload '{"rule":{"scope":"fix event view","ttl":"2h","assignee":"codex@impl"},"narrative":{"expected_work":"fix event view","expected_feedback":"progress_digest with result or blocker"},"refs":{"evidence_refs":["ticket-123"]}}')"
		case "$out" in *ticked=true*) ;; *) echo "assignment observe: $out"; exit 1 ;; esac
		# mid-risk gate: an assignment WITHOUT evidence is denied (event subject count stays at the 2 above).
		"$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type assignment.write_candidate.observed --external-id ci2b --payload '{"rule":{"scope":"no evidence","ttl":"1h","assignee":"codex@impl"},"narrative":{"expected_work":"attempt no-evidence work","expected_feedback":"progress_digest with result or blocker"}}' >/dev/null
		out="$("$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type progress_digest.write_candidate.observed --external-id ci3 --payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"event view 80 percent done"}}')"
		case "$out" in *ticked=true*) ;; *) echo "progress_digest observe: $out"; exit 1 ;; esac
		# all three governed event subjects are pullable in the default coordination scope
		out="$("$MH" control pull --addr "http://$addr" --principal codex@project --token-file "$tok")"
		case "$out" in *event_subjects=3*) ;; *) echo "coordination pull (want event_subjects=3): $out"; exit 1 ;; esac
		# the status FIELD section (P3d, tower seed) reports the coordination entry counts: each
		# admitted kind has one entry (the evidence-less assignment was denied, so assignment=1 not 2).
		out="$("$MH" control status --addr "http://$addr" --principal codex@project --token-file "$tok")"
		case "$out" in *"Field: agent profile=0, assignment=1, progress digest=1, project intent=1, teamwork signal=0"*) ;; *) echo "status FIELD wrong: $out"; exit 1 ;; esac
		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
	) || fail "coordination flow failed (see $WORK/run-coord.log)"
	sleep 0.3
	echo "    coordination kinds default-enabled OK"
}

# run_subscription proves the P4 context-budget acceptance ("packet 大小受预算约束"): a host endpoint
# DECLARES budget=digest-only in its binding; after several progress events its render context packet
# carries only the most-recent entry — older entries are dropped by the LOCAL budget transform
# (never a hub-side reduction). The authoritative pull still reports the event subject present: budget
# bounds PRESENTATION, not AUTHORITY (A4). The closed-set guard lives at the binding boundary.
run_subscription() {
	CUR_HOST="subscription"
	local proj="$WORK/proj-sub" addr="127.0.0.1:8791"
	mkdir -p "$proj"
	echo "=== E2E subscription budget (digest-only context packet) ==="
	(
		cd "$proj"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal codex@project --control-url "http://$addr" >/dev/null
		# the endpoint declares its context-budget tier: digest-only (latest entry only)
		python3 - <<-'PYEOF'
		import json
		p = ".mnemon/harness/channel/bindings.json"
		doc = json.load(open(p))
		doc["bindings"][0]["budget"] = "digest-only"
		json.dump(doc, open(p, "w"), indent=2)
		PYEOF
		"$MH" local run >"$WORK/run-sub.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"
		local up=0 i out
		for i in $(seq 1 60); do
			"$MH" control status --addr "http://$addr" --principal codex@project --token-file "$tok" >/dev/null 2>&1 && { up=1; break; }
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-sub.log"; exit 1; }
		# three distinct progress events -> three admitted entries (full authority)
		local n
		for n in 1 2 3; do
			out="$("$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
				--type progress_digest.write_candidate.observed --external-id "sub$n" \
				--payload '{"rule":{"feedback_kind":"progress"},"narrative":{"summary":"budget entry '"$n"'"}}')"
			case "$out" in *ticked=true*) ;; *) echo "sub observe $n: $out"; exit 1 ;; esac
		done
		# the context packet is budgeted to digest-only: the newest entry present, older ones dropped.
		out="$("$MH" control render --addr "http://$addr" --principal codex@project --token-file "$tok" --intent context.packet)"
		case "$out" in *"budget entry 3"*) ;; *) echo "digest-only context missing newest entry: $out"; exit 1 ;; esac
		case "$out" in *"budget entry 1"*|*"budget entry 2"*) echo "digest-only context leaked older entries: $out"; exit 1 ;; esac
		# AUTHORITY preserved (A4): the un-budgeted pull still reports the progress event subject present —
		# budget shrank the context packet, never what was admitted/stored.
		out="$("$MH" control pull --addr "http://$addr" --principal codex@project --token-file "$tok")"
		case "$out" in *event_subjects=1*) ;; *) echo "authority pull (want event_subjects=1): $out"; exit 1 ;; esac
		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
	) || fail "subscription flow failed (see $WORK/run-sub.log)"
	sleep 0.3
	echo "    subscription budget OK"
}

# run_tower proves the Agent Control Tower (P6): after the daemon admits a project_intent (GOAL) and
# an assignment (FIELD), `tower --dump` renders the four read-only pages (GOAL/FIELD/INBOX/LEDGER) with
# the governed data. The Tower opens the store directly (cross-actor reads the per-actor channel can't
# serve), so the daemon is stopped first (single-writer, S11). READ-ONLY: --dump never writes or Ticks.
run_tower() {
	CUR_HOST="tower"
	local proj="$WORK/proj-tower" addr="127.0.0.1:8792"
	mkdir -p "$proj"
	echo "=== E2E Control Tower (tower --dump) ==="
	(
		cd "$proj"
		local tok=".mnemon/harness/channel/credentials/codex-project.token"
		"$MH" setup --host codex --principal codex@project --control-url "http://$addr" >/dev/null
		"$MH" local run >"$WORK/run-tower.log" 2>&1 &
		local runpid=$!
		echo "$runpid" >"$PIDFILE"
		local up=0 i out
		for i in $(seq 1 60); do
			"$MH" control status --addr "http://$addr" --principal codex@project --token-file "$tok" >/dev/null 2>&1 && { up=1; break; }
			sleep 0.1
		done
		[ "$up" = 1 ] || { cat "$WORK/run-tower.log"; exit 1; }
		# seed GOAL (project_intent, mid-risk -> needs evidence) + FIELD (assignment with lease TTL)
		"$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type project_intent.write_candidate.observed --external-id ti1 --payload '{"narrative":{"statement":"ship the AgentTeam beta","evidence_summary":"roadmap"},"refs":{"evidence_refs":["roadmap"]}}' >/dev/null
		"$MH" control observe --addr "http://$addr" --principal codex@project --token-file "$tok" \
			--type assignment.write_candidate.observed --external-id ta1 --payload '{"rule":{"scope":"fix event view","ttl":"2h","assignee":"codex@impl"},"narrative":{"expected_work":"fix event view","expected_feedback":"progress_digest with result or blocker"},"refs":{"evidence_refs":["ticket"]}}' >/dev/null
		# stop the daemon so the Tower can open the store (single-writer, S11)
		{ kill "$runpid" 2>/dev/null; wait "$runpid"; } 2>/dev/null || true
		rm -f "$PIDFILE"
		# the Tower renders the four pages with the governed data
		out="$("$MH" tower --dump 2>&1)"
		local title
		for title in "# GOAL" "# FIELD" "# INBOX" "# LEDGER"; do
			case "$out" in *"$title"*) ;; *) echo "tower missing page $title:"; echo "$out"; exit 1 ;; esac
		done
		case "$out" in *"ship the AgentTeam beta"*) ;; *) echo "tower GOAL missing the project intent:"; echo "$out"; exit 1 ;; esac
		case "$out" in *"fix event view"*) ;; *) echo "tower FIELD missing the assignment:"; echo "$out"; exit 1 ;; esac
		case "$out" in *"codex@project"*) ;; *) echo "tower FIELD missing the agent:"; echo "$out"; exit 1 ;; esac
	) || fail "tower flow failed (see $WORK/run-tower.log)"
	sleep 0.3
	echo "    Control Tower OK"
}

run_host codex codex@project 8787 .codex
run_host claude-code claude@project 8899 .claude
run_observe_skill codex codex@project
run_observe_skill claude-code claude@project
run_note
run_external_goal
run_foo_external
run_sync_pair
run_daemon
run_coordination
run_subscription
run_tower

echo "E2E PASS (codex + claude-code; progress + observe-skill + note-external-package + external-goal + foo-scope + sync-pair[progress+journal+assignment] + daemon + coordination + subscription + tower)"
