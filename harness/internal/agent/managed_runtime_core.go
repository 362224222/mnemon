package agent

import (
	"context"
	"errors"
	"path/filepath"
	"time"
)

// managedRuntimeCore is the provider-neutral ownership boundary for one
// short-lived managed Runtime process. Provider adapters own their protocol;
// this core owns executable authority, exact process identity and cleanup.
type managedRuntimeCore struct {
	executable       string
	workspace        string
	environment      []string
	starter          CodexProcessStarter
	identity         CodexProcessIdentityProbe
	clock            CodexAdapterClock
	terminator       CodexProcessTerminator
	verifyProjection func(context.Context) error
	interruptGrace   time.Duration
	exitGrace        time.Duration
	signalGrace      time.Duration
	pipeDrainGrace   time.Duration
}

func newManagedRuntimeCore(options CodexWakeAdapterOptions) (*managedRuntimeCore, string, error) {
	if options.Executable == "" || !filepath.IsAbs(options.Executable) ||
		filepath.Clean(options.Executable) != options.Executable {
		return nil, "configure", errors.New("executable must be absolute and clean")
	}
	if options.Workspace == "" || !filepath.IsAbs(options.Workspace) ||
		filepath.Clean(options.Workspace) != options.Workspace {
		return nil, "configure", errors.New("workspace must be absolute and clean")
	}
	environment, err := validateCodexBaseEnvironment(options.Environment)
	if err != nil {
		return nil, "configure", err
	}
	if options.VerifyProjection == nil {
		return nil, "configure", errors.New("projection verifier is required")
	}
	if options.Identity == nil || options.Terminator == nil {
		if err := checkSystemRuntimeProcessSupport(); err != nil {
			return nil, "runtime readiness", err
		}
	}
	setManagedRuntimeDefaults(&options)
	if err := normalizeCodexAdapterDeadlines(&options); err != nil {
		return nil, "configure", err
	}
	return &managedRuntimeCore{executable: options.Executable, workspace: options.Workspace,
		environment: environment, starter: options.Starter, identity: options.Identity,
		clock: options.Clock, terminator: options.Terminator,
		verifyProjection: options.VerifyProjection,
		interruptGrace:   options.InterruptGrace, exitGrace: options.ExitGrace,
		signalGrace: options.SignalGrace, pipeDrainGrace: options.PipeDrainGrace}, "", nil
}

func setManagedRuntimeDefaults(options *CodexWakeAdapterOptions) {
	if options.Starter == nil {
		options.Starter = execCodexProcessStarter{}
	}
	if options.Identity == nil {
		options.Identity = systemCodexProcessIdentityProbe{}
	}
	if options.Clock == nil {
		options.Clock = wallCodexAdapterClock{}
	}
	if options.Terminator == nil {
		options.Terminator = systemCodexProcessTerminator{}
	}
}
