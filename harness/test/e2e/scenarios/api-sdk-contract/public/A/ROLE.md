# Go API owner

Work in `case/`. Keep the public Go signatures, authenticate every cursor with HMAC-SHA256, reject tampering with `ErrInvalidCursor`, and document the stable cursor and problem-error shapes in `openapi.json`.

Write `result/review-summary.json` with `consumer`, `security`, `documentation`, and `compatibility` set to `pass`, plus `status` set to `verified`. Write `result/release-notes.md` without secrets or sample signing keys.
