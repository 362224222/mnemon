package agent

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type claudeStreamEvent struct {
	line []byte
	err  error
}

type claudeStreamSession struct {
	adapter      *ClaudeWakeAdapter
	process      CodexProcess
	runtimeIDs   model.JSON
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	events       chan claudeStreamEvent
	stop         chan struct{}
	stopOnce     sync.Once
	stdoutDone   chan struct{}
	stderrDone   chan struct{}
	stderrBytes  atomic.Int64
	stderrExcess atomic.Bool
}

func newClaudeStreamSession(adapter *ClaudeWakeAdapter,
	process CodexProcess,
) (*claudeStreamSession, error) {
	if adapter == nil || process == nil || process.Stdin() == nil || process.Stdout() == nil ||
		process.Stderr() == nil {
		return nil, errors.New("Claude process did not expose three owned pipes")
	}
	session := &claudeStreamSession{adapter: adapter, process: process,
		stdin: process.Stdin(), stdout: process.Stdout(), stderr: process.Stderr(),
		events: make(chan claudeStreamEvent, 32), stop: make(chan struct{}),
		stdoutDone: make(chan struct{}), stderrDone: make(chan struct{})}
	go session.readStdout()
	go session.readStderr()
	return session, nil
}

func (session *claudeStreamSession) sendPrompt() error {
	written, writeErr := io.WriteString(session.stdin, claudeManagedPrompt)
	closeErr := session.stdin.Close()
	if writeErr != nil || written != len(claudeManagedPrompt) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		return errors.Join(writeErr, closeErr)
	}
	return closeErr
}

func (session *claudeStreamSession) next(ctx context.Context) ([]byte, error) {
	select {
	case event := <-session.events:
		return event.line, event.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (session *claudeStreamSession) finishOutput(ctx context.Context) error {
	select {
	case <-session.stderrDone:
		if session.stderrWasClean() {
			return nil
		}
		return errors.New("Claude emitted bounded stderr diagnostics")
	case <-session.adapter.clock.After(session.adapter.exitGrace):
		return errors.New("Claude stderr did not close after stdout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (session *claudeStreamSession) stderrWasClean() bool {
	return session.stderrBytes.Load() == 0 && !session.stderrExcess.Load()
}

func (session *claudeStreamSession) readStdout() {
	defer close(session.stdoutDone)
	scanner := bufio.NewScanner(session.stdout)
	scanner.Buffer(make([]byte, 4096), claudeStreamLineMax+1)
	total, messages := 0, 0
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		total += len(line) + 1
		messages++
		if len(line) == 0 || total > claudeStreamTotalMax || messages > claudeStreamEventsMax {
			session.sendEvent(claudeStreamEvent{err: errors.New("Claude stream exceeded its bound")})
			return
		}
		if !session.sendEvent(claudeStreamEvent{line: line}) {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		session.sendEvent(claudeStreamEvent{err: err})
		return
	}
	session.sendEvent(claudeStreamEvent{err: io.EOF})
}

func (session *claudeStreamSession) readStderr() {
	defer close(session.stderrDone)
	buffer := make([]byte, 4096)
	for {
		read, err := session.stderr.Read(buffer)
		if read > 0 && session.stderrBytes.Add(int64(read)) > claudeStderrMax {
			session.stderrExcess.Store(true)
		}
		if err != nil {
			return
		}
	}
}

func (session *claudeStreamSession) sendEvent(event claudeStreamEvent) bool {
	select {
	case session.events <- event:
		return true
	case <-session.stop:
		return false
	}
}

func (session *claudeStreamSession) closeReaders() error {
	session.stopOnce.Do(func() {
		close(session.stop)
		_ = session.stdout.Close()
		_ = session.stderr.Close()
	})
	var result error
	if err := session.waitReader(session.stdoutDone); err != nil {
		result = errors.Join(result, errors.New("Claude stdout reader did not stop"))
	}
	if err := session.waitReader(session.stderrDone); err != nil {
		result = errors.Join(result, errors.New("Claude stderr reader did not stop"))
	}
	if session.stderrExcess.Load() {
		result = errors.Join(result, errors.New("Claude stderr exceeded its bound"))
	}
	return result
}

func (session *claudeStreamSession) waitReader(done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-session.adapter.clock.After(session.adapter.pipeDrainGrace):
		return errors.New("reader wait exceeded its bound")
	}
}
