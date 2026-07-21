package peer

import (
	"errors"
	"testing"
)

func TestGossipIngressDiagnosticClassifiesClosedCodes(t *testing.T) {
	tests := map[GossipIngressDiagnosticCode]bool{
		GossipIngressDiagnosticPressure:    true,
		GossipIngressDiagnosticAuthority:   true,
		GossipIngressDiagnosticQuarantine:  false,
		GossipIngressDiagnosticConflict:    false,
		GossipIngressDiagnosticPublication: false,
		GossipIngressDiagnosticSession:     true,
		GossipIngressDiagnosticStore:       true,
	}
	for code, retryable := range tests {
		diagnostic := newGossipIngressDiagnostic(code)
		if diagnostic.Code() != code || diagnostic.Retryable() != retryable ||
			retryable && diagnostic.RetryAfter() <= 0 ||
			!retryable && diagnostic.RetryAfter() != 0 {
			t.Fatalf("diagnostic %q = %#v", code, diagnostic)
		}
	}
}

func TestGossipIngressFailureWrapsClosedAuthority(t *testing.T) {
	failure := newGossipIngressFailure(GossipIngressDiagnosticConflict)
	if !errors.Is(failure, ErrGossipIngress) || failure.Code() != GossipIngressDiagnosticConflict ||
		failure.Retryable() {
		t.Fatalf("gossip failure = %#v", failure)
	}
	fallback := newGossipIngressFailure("unknown")
	if fallback.Code() != GossipIngressDiagnosticStore || !fallback.Retryable() {
		t.Fatalf("fallback gossip failure = %#v", fallback)
	}
}
