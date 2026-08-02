package node

import (
	"context"
	"time"
)

// AgencyService is the transport-neutral R7 local action surface. It exposes
// only the View -> Intent -> Receipt loop and the machine mechanics needed to
// enter that loop. It contains no R5 Profile, Channel, Work, or Teamwork
// semantics.
//
// Parse and binding failures have not formed an AdmissionRequest and are
// therefore input errors, not durable rejected Receipts. Capture is owner-local
// CAS I/O; bytes become an accepted fact only when a later admission re-reads
// and verifies them.
type AgencyService interface {
	AgencyAttach(context.Context) (AgencyAttachment, error)
	AgencyCurrent(context.Context, AgencyAuthority) (AgencyView, error)
	AgencySubmit(context.Context, AgencyAuthority, AgencySubmission) (AgencyReceipt, error)
	AgencyCapture(context.Context, []byte) (AgencyArtifactCapture, error)
	AgencyStatus(context.Context) (AgencyStatusSnapshot, error)
}

// AgencyAttachment is private machine material. A transport may return it to
// the local CLI, but the CLI must never project it into an Agent View, Intent,
// Receipt, argument, or diagnostic.
type AgencyAttachment struct {
	ID         string
	Credential []byte
	ExpiresAt  time.Time
}

// AgencyCandidateBinding is private CLI evidence pairing one Agent-visible
// candidate handle with the exact CAS digest captured for it.
type AgencyCandidateBinding struct {
	Handle string
	Digest string
}

// AgencyArtifactCapture is private CLI evidence. Handle is safe to show to an
// Agent; Digest and ByteSize remain machine-held binding material.
type AgencyArtifactCapture struct {
	Handle   string
	Digest   string
	ByteSize int64
}

// AgencyView and AgencyReceipt contain only their bounded, canonical Agent
// projections. Machine authority has no field through which it could cross
// this seam.
type AgencyView struct{ canonical []byte }
type AgencyReceipt struct{ canonical []byte }

func (view AgencyView) CanonicalJSON() []byte       { return append([]byte(nil), view.canonical...) }
func (receipt AgencyReceipt) CanonicalJSON() []byte { return append([]byte(nil), receipt.canonical...) }

// AgencyStatusSnapshot is intentionally not a domain projection. Ready means
// only that the local authority and CAS ports were constructed and this
// service remains callable. It is not a deep health check or completion
// evidence.
type AgencyStatusSnapshot struct {
	Ready bool
}
