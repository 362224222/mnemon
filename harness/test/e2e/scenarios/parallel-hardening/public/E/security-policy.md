# Upload security policy

An untrusted name is one clean relative path element. The implementation creates a private temporary file inside the trusted root, streams and syncs it, then atomically publishes it. Errors remove partial state. Logs and results contain no body bytes or credentials.
