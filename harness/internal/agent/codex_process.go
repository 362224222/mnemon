package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type codexProcessExit struct {
	exited  bool
	method  string
	signals []string
}

func (adapter *CodexWakeAdapter) closeRegisteredProcess(process CodexProcess,
	runtimeIDs model.JSON,
) (codexProcessExit, error) {
	exit := codexProcessExit{signals: make([]string, 0)}
	if process == nil {
		return exit, errors.New("registered process is missing")
	}
	for _, stream := range []io.Closer{process.Stdin(), process.Stdout(), process.Stderr()} {
		if stream != nil {
			_ = stream.Close()
		}
	}
	// Exact Runtime identity has already been validated. Retain the direct
	// child unreaped while the terminator proves the process group's exit;
	// otherwise Wait could release its PID/PGID before group authority is used.
	terminateCtx, cancel := context.WithTimeout(context.Background(),
		codexTerminationMax+adapter.signalGrace)
	signals, err := adapter.terminator.Terminate(terminateCtx, runtimeIDs)
	cancel()
	if err != nil {
		return exit, err
	}
	if len(signals) != 0 {
		if err := validateCodexTerminationSignals(signals); err != nil {
			return exit, err
		}
		exit.signals = append(exit.signals, signals...)
	}
	exit.method = "wait_without_signal"
	if len(exit.signals) != 0 {
		exit.method = "signal_assisted"
	}
	err = process.Wait()
	exit.exited = true
	if err != nil {
		var exitErr *exec.ExitError
		if len(exit.signals) == 0 || !errors.As(err, &exitErr) {
			return exit, err
		}
	}
	return exit, nil
}

func (adapter *CodexWakeAdapter) closeUnregisteredProcess(process CodexProcess) (codexProcessExit, error) {
	if process == nil {
		return codexProcessExit{exited: true, method: "not_started", signals: []string{}}, nil
	}
	for _, stream := range []io.Closer{process.Stdin(), process.Stdout(), process.Stderr()} {
		if stream != nil {
			_ = stream.Close()
		}
	}
	exit := codexProcessExit{method: "wait_without_signal", signals: make([]string, 0)}
	// Identity capture failed before the first protocol byte was sent, so this
	// direct child cannot hold managed work. Signal only the exact os.Process
	// handle—not its unvalidated numeric group—and do so before Wait can reap
	// and release the PID. This closes the only safe cleanup window for an
	// unregistered child that ignores stdio EOF.
	if err := process.Signal(syscall.SIGKILL); err == nil {
		exit.method = "signal_assisted"
		exit.signals = []string{"SIGKILL"}
	} else if !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return exit, fmt.Errorf("kill exact unregistered child: %w", err)
	}
	err := process.Wait()
	exit.exited = true
	if err != nil {
		var exitErr *exec.ExitError
		if len(exit.signals) == 0 || !errors.As(err, &exitErr) {
			return exit, err
		}
	}
	return exit, nil
}

func (session *codexProtocolSession) close(interrupt bool) (codexProcessExit, error) {
	var exit codexProcessExit
	var result error
	session.closeOnce.Do(func() {
		exit, result = session.closeOwnedProcess(interrupt)
	})
	return exit, result
}

func (session *codexProtocolSession) closeOwnedProcess(interrupt bool) (codexProcessExit, error) {
	defer func() {
		close(session.stopReader)
		_ = session.stdout.Close()
		_ = session.stderr.Close()
	}()
	result := session.interruptOwnedTurn(interrupt)
	_ = session.stdin.Close()
	protocolExited, err := session.waitForProtocolExit(session.adapter.exitGrace)
	if err != nil {
		result = errors.Join(result, codexAdapterError("observe exit", err))
	}
	exit, proved, proofErr := session.proveOwnedProcessExit(protocolExited)
	result = errors.Join(result, proofErr)
	if !proved {
		return exit, result
	}
	exit, waitErr := session.waitOwnedProcess(exit)
	return exit, errors.Join(result, waitErr)
}

func (session *codexProtocolSession) interruptOwnedTurn(interrupt bool) error {
	if !interrupt || session.threadID == "" || session.turnID == "" {
		return nil
	}
	err := session.sendForCleanup(map[string]any{"id": 5, "method": "turn/interrupt",
		"params": map[string]any{"threadId": session.threadID, "turnId": session.turnID}},
		session.adapter.interruptGrace)
	if err == nil {
		err = session.waitForInterruptedTurn(session.adapter.interruptGrace)
	}
	if err != nil {
		return codexAdapterError("interrupt", err)
	}
	return nil
}

func (session *codexProtocolSession) proveOwnedProcessExit(protocolExited bool) (
	codexProcessExit, bool, error,
) {
	exit := codexProcessExit{signals: make([]string, 0)}
	var result error
	// Wait has deliberately not started: the direct child remains the exact
	// process-group identity anchor until observation or termination proves exit.
	if protocolExited {
		observed, err := session.observeRuntimeExit(session.adapter.signalGrace)
		if err != nil {
			result = codexAdapterError("observe Runtime exit", err)
		} else if observed {
			exit.method = "wait_without_signal"
			return exit, true, result
		}
	}
	terminationBudget := codexTerminationMax + session.adapter.signalGrace
	terminateCtx, cancel := context.WithTimeout(context.Background(), terminationBudget)
	signals, err := session.adapter.terminator.Terminate(terminateCtx, session.runtimeIDs)
	cancel()
	if err != nil {
		return exit, false, errors.Join(result, codexAdapterError("terminate", err))
	}
	if len(signals) != 0 {
		if err := validateCodexTerminationSignals(signals); err != nil {
			return exit, false, errors.Join(result, codexAdapterError("terminate", err))
		}
	}
	exit.signals = append(exit.signals, signals...)
	return exit, true, result
}

func (session *codexProtocolSession) waitOwnedProcess(exit codexProcessExit) (codexProcessExit, error) {
	var result error
	if err := session.waitForProcessPipeDrain(session.adapter.pipeDrainGrace); err != nil {
		result = codexAdapterError("pipe drain", err)
	}
	waitErr := session.process.Wait()
	exit.exited = true
	if exit.method == "" {
		exit.method = "wait_without_signal"
	}
	if len(exit.signals) != 0 {
		exit.method = "signal_assisted"
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if len(exit.signals) == 0 || !errors.As(waitErr, &exitErr) {
			result = errors.Join(result, codexAdapterError("wait", waitErr))
		}
	}
	if len(exit.signals) != 0 {
		result = errors.Join(result, codexAdapterError("cleanup",
			errors.New("process required signal-assisted shutdown")))
	}
	return exit, result
}

func (session *codexProtocolSession) waitForProcessPipeDrain(duration time.Duration) error {
	timer := session.adapter.clock.After(duration)
	stdoutDone, stderrDone := session.stdoutDone, session.stderrDone
	var result error
	forcedClose := false
	for stdoutDone != nil || stderrDone != nil {
		select {
		case <-session.stderrOverflow:
			result = errors.Join(result, errors.New("Codex stderr exceeded its bound"))
		case event := <-session.events:
			result = errors.Join(result, session.retainCleanupEvent(event))
		case <-stdoutDone:
			stdoutDone = nil
		case <-stderrDone:
			stderrDone = nil
		case <-timer:
			if forcedClose {
				result = errors.Join(result,
					errors.New("Codex process pipe readers did not stop after close"))
				timer = nil
				continue
			}
			_ = session.stdout.Close()
			_ = session.stderr.Close()
			result = errors.Join(result, errors.New("Codex process pipes did not drain after exit proof"))
			forcedClose = true
			timer = session.adapter.clock.After(duration)
		}
	}
	if session.stderrExceeded.Load() {
		result = errors.Join(result, errors.New("Codex stderr exceeded its bound"))
	}
	return result
}

func (session *codexProtocolSession) retainCleanupEvent(event codexProtocolEvent) error {
	if event.err != nil {
		if !errors.Is(event.err, io.EOF) {
			return event.err
		}
		return nil
	}
	session.pending = append(session.pending, event.envelope)
	if len(session.pending) > codexProtocolMessageMax {
		return errors.New("cleanup notifications exceeded their bound")
	}
	return nil
}
