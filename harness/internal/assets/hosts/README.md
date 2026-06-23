# Mnemon Harness Hosts

Host adapters describe the host mechanics needed by the R1 static render shim.

```text
harness/internal/assets/hosts/
├── claude-code/
└── codex/
```

Host-specific settings live here: lifecycle event names, stdin handling, and the
output dialect each hook should use. Runtime content is rendered by the local
service at hook time; hosts do not carry per-loop projected guides, mirrors, or
skills on the R1 path.
