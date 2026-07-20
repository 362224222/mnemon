# Streaming upload owner

Work in `case/`. Keep the `Save(root, name, body)` API, stream input with bounded buffers, reject absolute or escaping names, use a safe temporary file, and return only a path inside `root`.

Write `result/hardening-report.json` with `status`, `consumer`, `security`, `performance`, and `deployment` set to `pass`, plus an integer `max_buffer_bytes` no greater than 65536.
