package peer

import (
	"errors"
	"fmt"
	"time"
)

var ErrGossipIngress = errors.New("Mnemon Gossip ingress")

const (
	gossipIngressFastRetry  = 250 * time.Millisecond
	gossipIngressStoreRetry = time.Second
)

// GossipIngressDiagnosticCode is a closed operational result. It deliberately
// carries no Store error, publication body or transport address: callers can
// make retry/readiness decisions without leaking untrusted or durable bytes.
type GossipIngressDiagnosticCode string

const (
	GossipIngressDiagnosticPressure    GossipIngressDiagnosticCode = "pressure"
	GossipIngressDiagnosticAuthority   GossipIngressDiagnosticCode = "authority"
	GossipIngressDiagnosticQuarantine  GossipIngressDiagnosticCode = "quarantine"
	GossipIngressDiagnosticConflict    GossipIngressDiagnosticCode = "conflict"
	GossipIngressDiagnosticPublication GossipIngressDiagnosticCode = "invalid_publication"
	GossipIngressDiagnosticSession     GossipIngressDiagnosticCode = "session_unavailable"
	GossipIngressDiagnosticStore       GossipIngressDiagnosticCode = "store_unavailable"
)

func (code GossipIngressDiagnosticCode) Valid() bool {
	switch code {
	case GossipIngressDiagnosticPressure, GossipIngressDiagnosticAuthority,
		GossipIngressDiagnosticQuarantine, GossipIngressDiagnosticConflict,
		GossipIngressDiagnosticPublication, GossipIngressDiagnosticSession,
		GossipIngressDiagnosticStore:
		return true
	default:
		return false
	}
}

type GossipIngressDiagnostic struct {
	code       GossipIngressDiagnosticCode
	retryable  bool
	retryAfter time.Duration
}

func (diagnostic GossipIngressDiagnostic) Code() GossipIngressDiagnosticCode {
	return diagnostic.code
}

func (diagnostic GossipIngressDiagnostic) Retryable() bool { return diagnostic.retryable }

func (diagnostic GossipIngressDiagnostic) RetryAfter() time.Duration {
	return diagnostic.retryAfter
}

// GossipIngressFailure is returned only when the serial receive loop must be
// restarted or retired. Quarantine and a durably recorded conflict are exposed
// through Snapshot and do not stop unrelated origins on the same Channel.
type GossipIngressFailure struct{ diagnostic GossipIngressDiagnostic }

func (failure *GossipIngressFailure) Error() string {
	if failure == nil {
		return ErrGossipIngress.Error()
	}
	return fmt.Sprintf("%s: %s", ErrGossipIngress, failure.diagnostic.code)
}

func (failure *GossipIngressFailure) Unwrap() error { return ErrGossipIngress }

func (failure *GossipIngressFailure) Code() GossipIngressDiagnosticCode {
	if failure == nil {
		return ""
	}
	return failure.diagnostic.Code()
}

func (failure *GossipIngressFailure) Retryable() bool {
	return failure != nil && failure.diagnostic.Retryable()
}

func (failure *GossipIngressFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.diagnostic.RetryAfter()
}

func newGossipIngressDiagnostic(code GossipIngressDiagnosticCode) GossipIngressDiagnostic {
	switch code {
	case GossipIngressDiagnosticPressure, GossipIngressDiagnosticAuthority,
		GossipIngressDiagnosticSession:
		return GossipIngressDiagnostic{code: code, retryable: true,
			retryAfter: gossipIngressFastRetry}
	case GossipIngressDiagnosticStore:
		return GossipIngressDiagnostic{code: code, retryable: true,
			retryAfter: gossipIngressStoreRetry}
	case GossipIngressDiagnosticQuarantine, GossipIngressDiagnosticConflict,
		GossipIngressDiagnosticPublication:
		return GossipIngressDiagnostic{code: code}
	default:
		return GossipIngressDiagnostic{code: GossipIngressDiagnosticStore,
			retryable: true, retryAfter: gossipIngressStoreRetry}
	}
}

func newGossipIngressFailure(code GossipIngressDiagnosticCode) *GossipIngressFailure {
	return &GossipIngressFailure{diagnostic: newGossipIngressDiagnostic(code)}
}
