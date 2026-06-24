package channel

import (
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

// ServerAPI is the edge<->server boundary (D5). Production HTTP/gRPC+mTLS is a thin adapter over it
// (httpapi.go); the in-process implementation is *runtime.ControlServer. It grows by phase: Ingest
// (P0), PullEventView (P2). The channel owns this port; the runtime satisfies it structurally, so
// channel never imports runtime.
type ServerAPI interface {
	Ingest(principal contract.ActorID, env contract.ObservationEnvelope) (seq int64, dup bool, err error)
	PullEventView(principal contract.ActorID, sub contract.Subscription) (eventview.EventView, error)
}
