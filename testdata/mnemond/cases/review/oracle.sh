#!/usr/bin/env bash

r7_run_case() {
  local case_dir=$1 view initial_implementer receipt peer subject artifact
  local playbook_capture first_capture rework_capture revision_capture acceptance_capture
  local playbook_handle first_handle rework_handle revision_handle acceptance_handle intent

  view=$(r7_fresh_current implementer)
  test "$(printf '%s' "$view" | jq -r '.current // "none"')" = none || \
    r7_fail "implementer did not begin with an empty View"
  initial_implementer=$view

  # Observe an empty reviewer View before the request. Its next Hook must
  # rotate that empty Current and reveal later-arriving work.
  view=$(r7_fresh_current reviewer)
  test "$(printf '%s' "$view" | jq -r '.current // "none"')" = none || \
    r7_fail "reviewer did not begin with an empty View"

  cd "$case_dir"
  playbook_capture=$(r7_capture implementer playbook.md)
  first_capture=$(r7_capture implementer artifacts/candidate-v1.txt)
  playbook_handle=$(printf '%s' "$playbook_capture" | jq -r .handle)
  first_handle=$(printf '%s' "$first_capture" | jq -r .handle)
  peer=$(r7_remote_alias "$initial_implementer" reviewer)
  test -n "$peer" || r7_fail "reviewer target was absent"
  intent=$(jq -cn --arg peer "$peer" --arg playbook "$playbook_handle" --arg candidate "$first_handle" \
    '{kind:"review.request",payload:"review the bounded candidate",consequence:"handling.create",successors:[{self:true},{alias:$peer}],artifacts:[{kind:"candidate",handle:$playbook},{kind:"candidate",handle:$candidate}]}')
  receipt=$(r7_submit implementer "$intent")
  r7_expect_accepted "$receipt" "initial review request"

  view=$(r7_next_current implementer)
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  intent=$(jq -cn --arg subject "$subject" \
    '{kind:"review.wait",payload:"remote review remains independently pending",consequence:"handling.resolve.unresolved",subject_handling:$subject}')
  receipt=$(r7_submit implementer "$intent")
  r7_expect_accepted "$receipt" "initial local anchor disposition"

  r7_restart_node reviewer
  view=$(r7_next_current reviewer)
  r7_assert_view_artifacts_match_files reviewer "$view" "$case_dir/playbook.md" \
    "$case_dir/artifacts/candidate-v1.txt"
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  peer=$(r7_remote_alias "$view" implementer)
  rework_capture=$(r7_capture reviewer "$case_dir/artifacts/rework.txt")
  rework_handle=$(printf '%s' "$rework_capture" | jq -r .handle)
  intent=$(jq -cn --arg subject "$subject" --arg peer "$peer" --arg artifact "$rework_handle" \
    '{kind:"review.rework",payload:"the first candidate needs one bounded revision",consequence:"handling.advance",subject_handling:$subject,successors:[{alias:$peer}],artifacts:[{kind:"candidate",handle:$artifact}]}')
  receipt=$(r7_submit reviewer "$intent")
  r7_expect_accepted "$receipt" "rework response"

  view=$(r7_next_current reviewer)
  artifact=$(printf '%s' "$view" | jq -r '.current.facts.artifacts[0].handle')
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  intent=$(jq -cn --arg subject "$subject" --arg artifact "$artifact" \
    '{kind:"review.done",payload:"rework was durably sent",consequence:"handling.resolve.completed",subject_handling:$subject,artifacts:[{kind:"view_handle",handle:$artifact}]}')
  receipt=$(r7_submit reviewer "$intent")
  r7_expect_accepted "$receipt" "reviewer rework completion"

  view=$(r7_next_current implementer)
  r7_assert_view_artifacts_match_files implementer "$view" "$case_dir/artifacts/rework.txt"
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  peer=$(r7_remote_alias "$view" reviewer)
  playbook_capture=$(r7_capture implementer "$case_dir/playbook.md")
  revision_capture=$(r7_capture implementer "$case_dir/artifacts/candidate-v2.txt")
  playbook_handle=$(printf '%s' "$playbook_capture" | jq -r .handle)
  revision_handle=$(printf '%s' "$revision_capture" | jq -r .handle)
  intent=$(jq -cn --arg subject "$subject" --arg peer "$peer" --arg playbook "$playbook_handle" --arg candidate "$revision_handle" \
    '{kind:"review.revision",payload:"review the revised candidate",consequence:"handling.advance",subject_handling:$subject,successors:[{alias:$peer}],artifacts:[{kind:"candidate",handle:$playbook},{kind:"candidate",handle:$candidate}]}')
  receipt=$(r7_submit implementer "$intent")
  r7_expect_accepted "$receipt" "revised candidate"

  view=$(r7_next_current implementer)
  artifact=$(printf '%s' "$view" | jq -r '.current.facts.artifacts[0].handle')
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  intent=$(jq -cn --arg subject "$subject" --arg artifact "$artifact" \
    '{kind:"review.done",payload:"revision was durably sent",consequence:"handling.resolve.completed",subject_handling:$subject,artifacts:[{kind:"view_handle",handle:$artifact}]}')
  receipt=$(r7_submit implementer "$intent")
  r7_expect_accepted "$receipt" "implementer revision completion"

  view=$(r7_next_current reviewer)
  r7_assert_view_artifacts_match_files reviewer "$view" "$case_dir/playbook.md" \
    "$case_dir/artifacts/candidate-v2.txt"
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  peer=$(r7_remote_alias "$view" implementer)
  acceptance_capture=$(r7_capture reviewer "$case_dir/artifacts/acceptance.txt")
  acceptance_handle=$(printf '%s' "$acceptance_capture" | jq -r .handle)
  intent=$(jq -cn --arg subject "$subject" --arg peer "$peer" --arg artifact "$acceptance_handle" \
    '{kind:"review.accept",payload:"the revised candidate is accepted",consequence:"handling.advance",subject_handling:$subject,successors:[{alias:$peer}],artifacts:[{kind:"candidate",handle:$artifact}]}')
  receipt=$(r7_submit reviewer "$intent")
  r7_expect_accepted "$receipt" "review acceptance"

  view=$(r7_next_current reviewer)
  artifact=$(printf '%s' "$view" | jq -r '.current.facts.artifacts[0].handle')
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  intent=$(jq -cn --arg subject "$subject" --arg artifact "$artifact" \
    '{kind:"review.done",payload:"acceptance was durably sent",consequence:"handling.resolve.completed",subject_handling:$subject,artifacts:[{kind:"view_handle",handle:$artifact}]}')
  receipt=$(r7_submit reviewer "$intent")
  r7_expect_accepted "$receipt" "reviewer acceptance completion"

  view=$(r7_next_current implementer)
  r7_assert_view_artifacts_match_files implementer "$view" "$case_dir/artifacts/acceptance.txt"
  subject=$(printf '%s' "$view" | jq -r .current.facts.handle)
  artifact=$(printf '%s' "$view" | jq -r '.current.facts.artifacts[0].handle')
  intent=$(jq -cn --arg subject "$subject" --arg artifact "$artifact" \
    '{kind:"review.done",payload:"accepted review result was verified",consequence:"handling.resolve.completed",subject_handling:$subject,artifacts:[{kind:"view_handle",handle:$artifact}]}')
  receipt=$(r7_submit implementer "$intent")
  r7_expect_accepted "$receipt" "implementer final completion"
}
