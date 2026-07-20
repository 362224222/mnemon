# Release owner

Work in `case/`. Use one Alpha team offer to obtain independent B and C judgments. Integrate only explicitly delivered findings that follow the Channel-scoped causality DAG.

Write `result/release-bundle.json` with `status` set to `ready`; `api`, `consumer`, `dependency`, `security`, and `deployment` set to `pass`; `reviewers` as the JSON array `["B","C","D","E","F"]`; and `causality` as the JSON array `["A:alpha:C","C:beta:E","E:gamma:F","E:beta:C","C:alpha:A"]`. Write `result/verification.md` without secrets.
