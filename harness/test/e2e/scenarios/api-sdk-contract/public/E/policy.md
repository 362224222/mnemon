# Cursor policy

The server must authenticate the complete cursor payload with HMAC-SHA256 and compare authentication bytes safely. A malformed, forged, or key-mismatched cursor returns the same public problem shape and never exposes signing material.
