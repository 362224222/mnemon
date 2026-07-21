package node

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestAdmittedWakeStoreEntersAndReleasesEveryOperationIndependently(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	for _, test := range wakeStoreOperationTests() {
		t.Run(test.name, func(t *testing.T) {
			admission := &wakeAdmissionSpy{}
			admitted := &admittedWakeStore{store: st, admission: admission}
			_ = test.call(admitted)
			snapshot := admission.snapshot()
			if snapshot.workerEntries != 1 || snapshot.releases != 1 || snapshot.active != 0 ||
				snapshot.maxActive != 1 || snapshot.routeEntries != 0 {
				t.Fatalf("admission snapshot = %#v", snapshot)
			}
		})
	}
}

func TestAdmittedWakeStoreRejectsEveryOperationBeforeStore(t *testing.T) {
	t.Parallel()
	rejected := errors.New("worker admission rejected")
	for _, test := range wakeStoreOperationTests() {
		t.Run(test.name, func(t *testing.T) {
			admission := &wakeAdmissionSpy{enterErr: rejected}
			// A nil Store makes an accidental call observable as a panic. Every
			// operation must return the admission error before reaching it.
			admitted := &admittedWakeStore{admission: admission}
			if err := test.call(admitted); !errors.Is(err, rejected) ||
				!errors.Is(err, agent.ErrWakeStoreNotInvoked) {
				t.Fatalf("operation error = %v, want %v", err, rejected)
			}
			snapshot := admission.snapshot()
			if snapshot.workerEntries != 1 || snapshot.releases != 0 || snapshot.active != 0 ||
				snapshot.routeEntries != 0 {
				t.Fatalf("rejected admission snapshot = %#v", snapshot)
			}
		})
	}
}

func TestAdmittedWakeStoreProvesRetainedSealPreventedStoreCall(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	admission := newControllerAdmissionGate()
	generation, err := admission.seal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer admission.reopen(generation)
	admitted := &admittedWakeStore{store: st, admission: admission}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, callErr := admitted.PreclaimAgentWake(ctx, store.AgentWakePreclaimSpec{})
		result <- callErr
	}()
	select {
	case callErr := <-result:
		t.Fatalf("retained seal admitted Store call: %v", callErr)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case callErr := <-result:
		if !errors.Is(callErr, agent.ErrWakeStoreNotInvoked) ||
			!errors.Is(callErr, ErrManagedAdmission) || !errors.Is(callErr, context.Canceled) {
			t.Fatalf("retained-seal error = %v", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("retained seal did not release cancelled worker admission")
	}
}

func TestWakeAttachmentFilesystemWorkStaysOutsideAdmission(t *testing.T) {
	t.Parallel()
	t.Run("scan failure precedes first Store entry", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		admission := &wakeAdmissionSpy{}
		preparer, err := agent.NewWakeAttachmentPreparer(
			&admittedWakeStore{admission: admission},
			agent.WakeAttachmentOptions{Attachments: newAgentAttachmentFilesystem(
				filepath.Join(t.TempDir(), "missing", "node")),
				AssetRevision: fixture.revision, Clock: wakeFixedClock{fixture.profile.UpdatedAt()}},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := preparer.Prepare(context.Background(), fixture.profile); err == nil {
			t.Fatal("Prepare() succeeded without a Node state directory")
		}
		if snapshot := admission.snapshot(); snapshot.workerEntries != 0 || snapshot.active != 0 {
			t.Fatalf("filesystem scan entered admission: %#v", snapshot)
		}
	})

	t.Run("staging runs between Store entries", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		admission := &wakeAdmissionSpy{}
		entropy := &wakeBoundaryReader{source: bytes.NewReader(bytes.Repeat([]byte{0xa5}, 80)),
			admission: admission}
		preparer, err := agent.NewWakeAttachmentPreparer(
			&admittedWakeStore{store: st, admission: admission},
			agent.WakeAttachmentOptions{Attachments: newAgentAttachmentFilesystem(fixture.nodeState),
				AssetRevision: fixture.revision,
				Clock:         wakeFixedClock{fixture.profile.UpdatedAt()}, Random: entropy},
		)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := preparer.Prepare(context.Background(), fixture.profile)
		if err != nil || prepared.Status() != store.AgentClaimNone {
			t.Fatalf("Prepare() = (%#v, %v)", prepared, err)
		}
		readerCalls, readerViolated := entropy.snapshot()
		if readerCalls == 0 || readerViolated {
			t.Fatalf("staging admission observation = calls %d, violated %t", readerCalls, readerViolated)
		}
		if snapshot := admission.snapshot(); snapshot.workerEntries != 2 || snapshot.releases != 2 ||
			snapshot.active != 0 || snapshot.maxActive != 1 {
			t.Fatalf("preparer admission snapshot = %#v", snapshot)
		}
	})
}

func TestManagedWakeWorkerDoesNotHoldAdmissionAcrossAdapterRun(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	insertWakeBoundaryHandling(t, st.Path(), fixture.profile.UpdatedAt())

	admission := &wakeAdmissionSpy{}
	adapter := &wakeBoundaryAdapter{admission: admission, entered: make(chan int, 1)}
	worker, err := newManagedWakeWorker(st, fixture.nodeState, fixture.profile,
		wakeFixedClock{fixture.profile.UpdatedAt()}, fixture.install, adapter, admission)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	select {
	case active := <-adapter.entered:
		if active != 0 {
			t.Fatalf("adapter.Run began with %d admitted Store calls", active)
		}
	case err := <-done:
		t.Fatalf("worker stopped before adapter.Run: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach adapter.Run")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, agent.ErrWakeWorker) {
			t.Fatalf("worker error = %v, want fail-closed adapter result", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after adapter cancellation")
	}
	if snapshot := admission.snapshot(); snapshot.workerEntries != 3 || snapshot.releases != 3 ||
		snapshot.active != 0 || snapshot.maxActive != 1 {
		t.Fatalf("adapter boundary admission snapshot = %#v", snapshot)
	}
}

type wakeStoreOperationTest struct {
	name string
	call func(*admittedWakeStore) error
}

func wakeStoreOperationTests() []wakeStoreOperationTest {
	return []wakeStoreOperationTest{
		{name: "PreclaimAgentWake", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.PreclaimAgentWake(context.Background(), store.AgentWakePreclaimSpec{})
			return err
		}},
		{name: "ListReapableAgentRunAttachments", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.ListReapableAgentRunAttachments(context.Background(),
				store.AgentAttachmentCleanupSpec{})
			return err
		}},
		{name: "ListIncompleteManagedAgentRuns", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.ListIncompleteManagedAgentRuns(context.Background())
			return err
		}},
		{name: "AbandonUnregisteredAgentRun", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.AbandonUnregisteredAgentRun(context.Background(),
				store.AgentUnregisteredRunSpec{})
			return err
		}},
		{name: "SettleOrphanedAgentRuntime", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.SettleOrphanedAgentRuntime(context.Background(), store.AgentRuntimeOrphanSpec{})
			return err
		}},
		{name: "RecordAgentRuntimeLaunch", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.RecordAgentRuntimeLaunch(context.Background(), store.AgentRuntimeLaunchSpec{})
			return err
		}},
		{name: "RecordAgentWakeDelivery", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.RecordAgentWakeDelivery(context.Background(), store.AgentWakeDeliverySpec{})
			return err
		}},
		{name: "FinishAgentRuntime", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.FinishAgentRuntime(context.Background(), store.AgentRuntimeFinishSpec{})
			return err
		}},
		{name: "FailAgentRuntime", call: func(admitted *admittedWakeStore) error {
			_, err := admitted.FailAgentRuntime(context.Background(), store.AgentRuntimeFailureSpec{})
			return err
		}},
	}
}

type wakeAdmissionSnapshot struct {
	routeEntries  int
	workerEntries int
	releases      int
	active        int
	maxActive     int
}

type wakeAdmissionSpy struct {
	mu sync.Mutex
	wakeAdmissionSnapshot
	enterErr error
}

func (admission *wakeAdmissionSpy) Enter(context.Context) (func(), error) {
	admission.mu.Lock()
	admission.routeEntries++
	admission.mu.Unlock()
	return nil, errors.New("route admission used by wake Store")
}

func (admission *wakeAdmissionSpy) EnterWorker(context.Context) (func(), error) {
	admission.mu.Lock()
	admission.workerEntries++
	if admission.enterErr != nil {
		err := admission.enterErr
		admission.mu.Unlock()
		return nil, err
	}
	admission.active++
	if admission.active > admission.maxActive {
		admission.maxActive = admission.active
	}
	admission.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			admission.mu.Lock()
			admission.releases++
			admission.active--
			admission.mu.Unlock()
		})
	}, nil
}

func (admission *wakeAdmissionSpy) snapshot() wakeAdmissionSnapshot {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.wakeAdmissionSnapshot
}

type wakeBoundaryReader struct {
	mu        sync.Mutex
	source    io.Reader
	admission *wakeAdmissionSpy
	calls     int
	violated  bool
}

func (reader *wakeBoundaryReader) Read(buffer []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.calls++
	if reader.admission.snapshot().active != 0 {
		reader.violated = true
	}
	return reader.source.Read(buffer)
}

func (reader *wakeBoundaryReader) snapshot() (int, bool) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.calls, reader.violated
}

type wakeBoundaryAdapter struct {
	admission *wakeAdmissionSpy
	entered   chan int
}

func (adapter *wakeBoundaryAdapter) Run(ctx context.Context,
	_ agent.CodexWakeRequest,
) (agent.CodexWakeResult, error) {
	adapter.entered <- adapter.admission.snapshot().active
	<-ctx.Done()
	return agent.CodexWakeResult{}, ctx.Err()
}

type wakeFixedClock struct{ at time.Time }

func (clock wakeFixedClock) Now() time.Time { return clock.at }

// insertWakeBoundaryHandling seeds the one queue row needed to reach the
// adapter boundary. Store intentionally exposes no test-only enqueue API, so
// the fixture connection performs this isolated insert while the production
// Store is idle. The worker still performs every mutation through Store.
func insertWakeBoundaryHandling(t *testing.T, databasePath string, at time.Time) {
	t.Helper()
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatal(err)
	}
	encodedAt := at.Round(0).UTC().Format("2006-01-02T15:04:05.000000000Z")
	if _, err := db.Exec(`INSERT INTO agent_handlings(
		handling_id,profile_id,event_id,status,priority,available_at,attempts,recovery_count,
		created_at,updated_at) VALUES(?,?,?,'pending',1,?,0,0,?,?)`,
		"handling-wake-adapter-boundary", model.TeamworkProfileID().String(),
		"event-wake-adapter-boundary", encodedAt, encodedAt, encodedAt); err != nil {
		t.Fatal(err)
	}
}

var _ ManagedAdmission = (*wakeAdmissionSpy)(nil)
var _ agent.WakeWorkerAdapter = (*wakeBoundaryAdapter)(nil)
