#!/bin/sh
set -eu

cue='[mnemon:wake] Managed work is pending. Use the Mnemon Harness skill to process one Event.'

emit_result() {
    id=$1
    result=$2
    jq -cn --argjson id "$id" --argjson result "$result" '{id:$id,result:$result}'
}

hook_inventory() {
    cwd=$1
    config="$cwd/.codex/hooks.json"
    hook="$cwd/.codex/hooks/mnemon-harness/hook.sh"
    if [ ! -f "$config" ] || [ ! -x "$hook" ] ||
       ! jq -e --arg command "$hook" '
          [.hooks.UserPromptSubmit[]?.hooks[]? |
            select(.command == $command and .statusMessage == "Checking Mnemon Teamwork" and
                   .timeout == 10 and .type == "command")] | length == 1
       ' "$config" >/dev/null 2>&1; then
        jq -cn --arg cwd "$cwd" --arg path "$config" \
          '{data:[{cwd:$cwd,errors:[{path:$path,message:"managed project Hook is absent or changed"}],warnings:[],hooks:[]}]}'
        return
    fi
    hash="sha256:$(sha256sum "$hook" | awk '{print $1}')"
    jq -cn --arg cwd "$cwd" --arg command "$hook" --arg hash "$hash" --arg source "$config" '
      {data:[{cwd:$cwd,errors:[],warnings:[],hooks:[{
        command:$command,currentHash:$hash,displayOrder:1,enabled:true,
        eventName:"userPromptSubmit",handlerType:"command",isManaged:false,
        key:"project:userPromptSubmit:mnemon-harness",matcher:null,pluginId:null,
        source:"project",sourcePath:$source,statusMessage:"Checking Mnemon Teamwork",
        timeoutSec:10,trustStatus:"trusted"
      }]}]}'
}

skill_inventory() {
    cwd=$1
    skill="$cwd/.agents/skills/mnemon-harness/SKILL.md"
    if [ ! -f "$skill" ] ||
       ! grep -q '^name: mnemon-harness$' "$skill" ||
       ! grep -q '^description: .' "$skill"; then
        jq -cn --arg cwd "$cwd" --arg path "$skill" \
          '{data:[{cwd:$cwd,errors:[{path:$path,message:"managed project Skill is absent or changed"}],skills:[]}]}'
        return
    fi
    jq -cn --arg cwd "$cwd" --arg path "$skill" '
      {data:[{cwd:$cwd,errors:[],skills:[{
        dependencies:null,description:"Process one mnemond-managed Teamwork event.",
        enabled:true,interface:null,name:"mnemon-harness",path:$path,scope:"repo",
        shortDescription:null
      }]}]}'
}

run_hook() {
    cwd=$1
    hook="$cwd/.codex/hooks/mnemon-harness/hook.sh"
    test -x "$hook"
    "$hook"
}

app_server() {
    thread_id="thread-scripted-$$"
    turn_id="turn-scripted-$$"
    hook_id="hook-scripted-$$"
    while IFS= read -r request; do
        method=$(printf '%s\n' "$request" | jq -er '.method')
        id=$(printf '%s\n' "$request" | jq -c '.id // empty')
        case "$method" in
            initialize)
                result=$(jq -cn --arg home "${CODEX_HOME:-/home/r5/.codex}" \
                  '{codexHome:$home,platformFamily:"unix",platformOs:"linux",userAgent:"mnemon-r5-scripted"}')
                emit_result "$id" "$result"
                ;;
            initialized)
                ;;
            hooks/list)
                cwd=$(printf '%s\n' "$request" | jq -er '.params.cwds[0]')
                result=$(hook_inventory "$cwd")
                emit_result "$id" "$result"
                ;;
            skills/list)
                cwd=$(printf '%s\n' "$request" | jq -er '.params.cwds[0]')
                result=$(skill_inventory "$cwd")
                emit_result "$id" "$result"
                ;;
            thread/start)
                cwd=$(printf '%s\n' "$request" | jq -er '.params.cwd')
                now=$(date +%s)
                result=$(jq -cn --arg cwd "$cwd" --arg thread "$thread_id" --argjson now "$now" '
                  {approvalPolicy:"never",approvalsReviewer:"user",cwd:$cwd,
                   model:"scripted-r5",modelProvider:"mnemon-scripted",
                   sandbox:{type:"workspaceWrite",writableRoots:[],networkAccess:false,
                            excludeTmpdirEnvVar:false,excludeSlashTmp:false},
                   thread:{cliVersion:"r5-scripted",createdAt:$now,cwd:$cwd,ephemeral:true,
                           id:$thread,modelProvider:"mnemon-scripted",preview:"",sessionId:$thread,
                           source:"scripted-runtime",status:{type:"idle"},turns:[],updatedAt:$now}}')
                emit_result "$id" "$result"
                ;;
            turn/start)
                cwd=$(printf '%s\n' "$request" | jq -er '.params.cwd')
                result=$(jq -cn --arg turn "$turn_id" '{turn:{id:$turn,status:"inProgress",items:[]}}')
                emit_result "$id" "$result"
                started=$(date +%s%3N)
                source="$cwd/.codex/hooks.json"
                jq -cn --arg thread "$thread_id" --arg turn "$turn_id" --arg hook "$hook_id" \
                  --arg source "$source" --argjson started "$started" '
                  {method:"hook/started",params:{threadId:$thread,turnId:$turn,run:{
                    id:$hook,displayOrder:1,entries:[],eventName:"userPromptSubmit",
                    executionMode:"sync",handlerType:"command",scope:"turn",source:"project",
                    sourcePath:$source,startedAt:$started,status:"running",
                    statusMessage:"Checking Mnemon Teamwork"}}}'
                hook_status=0
                hook_output=$(run_hook "$cwd") || hook_status=$?
                completed=$(date +%s%3N)
                duration=$((completed - started))
                if [ "$hook_status" -eq 0 ] && [ "$hook_output" = "$cue" ]; then
                    jq -cn --arg thread "$thread_id" --arg turn "$turn_id" --arg hook "$hook_id" \
                      --arg source "$source" --arg cue "$cue" --argjson started "$started" \
                      --argjson completed "$completed" --argjson duration "$duration" '
                      {method:"hook/completed",params:{threadId:$thread,turnId:$turn,run:{
                        id:$hook,displayOrder:1,entries:[{kind:"context",text:$cue}],
                        eventName:"userPromptSubmit",executionMode:"sync",handlerType:"command",
                        scope:"turn",source:"project",sourcePath:$source,startedAt:$started,
                        completedAt:$completed,durationMs:$duration,status:"completed",
                        statusMessage:"Checking Mnemon Teamwork"}}}'
                    if /opt/r5/bin/scripted-agent-turn --managed; then
                        turn_status=completed
                    else
                        turn_status=failed
                    fi
                else
                    turn_status=failed
                fi
                jq -cn --arg thread "$thread_id" --arg turn "$turn_id" --arg status "$turn_status" '
                  {method:"turn/completed",params:{threadId:$thread,
                    turn:{id:$turn,status:$status,items:[]}}}'
                ;;
            turn/interrupt)
                if [ -n "$id" ]; then
                    emit_result "$id" '{}'
                fi
                jq -cn --arg thread "$thread_id" --arg turn "$turn_id" '
                  {method:"turn/completed",params:{threadId:$thread,
                    turn:{id:$turn,status:"interrupted",items:[]}}}'
                ;;
            *)
                if [ -n "$id" ]; then
                    jq -cn --argjson id "$id" '{id:$id,error:{code:-32601,message:"unsupported scripted method"}}'
                fi
                ;;
        esac
    done
}

case "${1:-}" in
    --version|version)
        test "$#" -eq 1
        printf '%s\n' 'codex-cli 0.0.0-scripted-r5'
        ;;
    app-server)
        if [ "${2:-}" = "--help" ] && [ "$#" -eq 2 ]; then
            printf '%s\n' 'Usage: codex app-server [OPTIONS]'
        elif [ "${2:-}" = "--stdio" ] && [ "$#" -eq 2 ]; then
            app_server
        else
            exit 2
        fi
        ;;
    exec)
        test "$#" -eq 2
        test "$2" = '-'
        install -d -m 0700 .r5/runtime
        prompt=.r5/runtime/user-prompt.txt
        umask 077
        sed -n '1,200p' >"$prompt"
        # An ordinary user turn executes the installed Hook too. Fresh entry
        # has no pending managed Event, so an empty successful result is exact.
        run_hook "$(pwd)" >.r5/runtime/user-hook.txt
        /opt/r5/bin/scripted-agent-turn --user <"$prompt"
        ;;
    *)
        exit 2
        ;;
esac
