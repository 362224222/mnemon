package agent

import (
	"context"
	"errors"
	"time"
)

const (
	// codexPipeDrainMax bounds pipe EOF delivery after process exit is already
	// proven. It stays independent of signal grace so a short observation
	// grace cannot starve the drain.
	codexPipeDrainMax = 5 * time.Second
)

type CodexWakeAdapterOptions struct {
	Executable       string
	Workspace        string
	Environment      []string
	Starter          CodexProcessStarter
	Identity         CodexProcessIdentityProbe
	Clock            CodexAdapterClock
	Terminator       CodexProcessTerminator
	VerifyProjection func(context.Context) error
	InterruptGrace   time.Duration
	ExitGrace        time.Duration
	SignalGrace      time.Duration
	PipeDrainGrace   time.Duration
}

func normalizeCodexAdapterDeadlines(options *CodexWakeAdapterOptions) error {
	if options.InterruptGrace == 0 {
		options.InterruptGrace = 2 * time.Second
	}
	if options.ExitGrace == 0 {
		options.ExitGrace = 2 * time.Second
	}
	if options.SignalGrace == 0 {
		options.SignalGrace = time.Second
	}
	if options.PipeDrainGrace == 0 {
		options.PipeDrainGrace = codexPipeDrainMax
	}
	for _, deadline := range []time.Duration{options.InterruptGrace, options.ExitGrace,
		options.SignalGrace, options.PipeDrainGrace} {
		if deadline < time.Millisecond || deadline > 30*time.Second {
			return errors.New("cleanup deadlines must be 1ms..30s")
		}
	}
	return nil
}
