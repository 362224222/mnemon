# Resource budget

The upload copy buffer is at most 64 KiB. A 32 MiB upload must not require an object-sized allocation. Temporary state is bounded per active upload and is removed on failure.
