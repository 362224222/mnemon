# Deployment policy

No privileged runtime or host mount is permitted. The service writes only beneath its configured upload root, uses owner-only file modes, exposes bounded failure behavior, and leaves no partial final object after restart.
