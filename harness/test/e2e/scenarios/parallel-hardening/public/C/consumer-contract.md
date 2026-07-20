# Upload consumer contract

- `Save` returns only after the destination is durable or the operation fails.
- Failed and canceled uploads leave no final destination.
- Object size does not determine process memory use.
- Returned paths remain within the caller-provided root.
