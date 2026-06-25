# Mnemon Harness Hosts

Host adapters describe the host mechanics needed by the generic lifecycle hooks.

```text
harness/internal/assets/hosts/
├── claude-code/
└── codex/
```

Host-specific settings live here: lifecycle event names, stdin handling, and the
output dialect each hook should use. Hook scripts stay business-free; managed
guide content and event-specific behavior live in Local Mnemon, not in host
mechanics. Hosts do not carry per-loop projected guides or mirrors on the R1
path.
