package daemon

import "encoding/json"

// Wire fields are frozen in canonical order. These private DTOs never enter
// agency because HTTP is a replaceable local boundary.
type attachmentWire struct {
	Attachment string `json:"attachment"`
	Credential string `json:"credential"`
	ExpiresAt  string `json:"expires_at"`
	Schema     string `json:"schema"`
	Version    int    `json:"version"`
}

type attachmentBeginWire struct {
	BoundaryDigest string `json:"boundary_digest"`
}

type attachmentEndWire struct {
	ReleasedClaim bool   `json:"released_claim"`
	Replayed      bool   `json:"replayed"`
	Schema        string `json:"schema"`
	Status        string `json:"status"`
	Version       int    `json:"version"`
}

type candidateWire struct {
	Digest string `json:"digest"`
	Handle string `json:"handle"`
}

type submitWire struct {
	Candidates []candidateWire `json:"candidates,omitempty"`
	Intent     json.RawMessage `json:"intent"`
}

type artifactRequestWire struct {
	Content string `json:"content_base64"`
}

type artifactReadRequestWire struct {
	Handle string `json:"handle"`
}

type artifactResponseWire struct {
	ByteSize int64  `json:"byte_size"`
	Digest   string `json:"digest"`
	Handle   string `json:"handle"`
	Schema   string `json:"schema"`
	Version  int    `json:"version"`
}

type statusWire struct {
	Schema  string `json:"schema"`
	Status  string `json:"status"`
	Version int    `json:"version"`
}
