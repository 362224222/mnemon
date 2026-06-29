package driver

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/activationtrace"
)

const (
	ManagedTurnTraceSourceCodexAppServer = activationtrace.SourceCodexAppServer
	managedTurnTraceTextLimit            = activationtrace.TextLimit
)

type ManagedTurnTraceEvent = activationtrace.Event
type ManagedTurnTraceSink = activationtrace.Sink
type ManagedTurnTraceSinkFunc = activationtrace.SinkFunc

type ManagedTurnClientWithTrace interface {
	StartTurnWithTrace(ctx context.Context, query string, sink ManagedTurnTraceSink) (ManagedTurnResult, error)
}

func ManagedTurnTraceEventsFromCodexNotifications(principal string, notifications []map[string]any) []ManagedTurnTraceEvent {
	return activationtrace.EventsFromCodexNotifications(principal, notifications)
}
