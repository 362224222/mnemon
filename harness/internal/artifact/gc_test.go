package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestGCReconcilesEveryDurableCrashState(t *testing.T) {
	tests := []struct {
		name       string
		queueState GCQueueState
		casState   CASTombstoneState
		fatal      GCFatalCode
	}{
		{name: "queued final only", queueState: GCQueueQueued, casState: CASTombstoneFinalOnly},
		{name: "queued linked", queueState: GCQueueQueued, casState: CASTombstoneFinalAndTrash},
		{name: "queued tombstone only", queueState: GCQueueQueued, casState: CASTombstoneTrashOnly},
		{name: "renamed tombstone only", queueState: GCQueueRenamed, casState: CASTombstoneTrashOnly},
		{name: "renamed absent", queueState: GCQueueRenamed, casState: CASTombstoneAbsent},
		{name: "queued absent fails closed", queueState: GCQueueQueued,
			casState: CASTombstoneAbsent, fatal: GCFatalCASInvariant},
		{name: "renamed final only fails closed", queueState: GCQueueRenamed,
			casState: CASTombstoneFinalOnly, fatal: GCFatalCASInvariant},
		{name: "renamed linked fails closed", queueState: GCQueueRenamed,
			casState: CASTombstoneFinalAndTrash, fatal: GCFatalCASInvariant},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cas := gcOpenCAS(t)
			content := []byte(fmt.Sprintf("gc crash matrix object %d", index))
			digest := model.Sum(content)
			token := gcToken(byte(index + 1))
			if _, err := cas.Put(digest, content); err != nil {
				t.Fatal(err)
			}
			gcArrangeCASState(t, cas, digest, token, test.casState)
			store := newGCTestStore()
			store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest, Token: token},
				SizeBytes: uint64(len(content)), State: test.queueState})
			worker := gcNewWorker(t, store, cas, GCOptions{})
			err := worker.ReconcileStartup(context.Background())
			if test.fatal != GCFatalNone {
				if !errors.Is(err, ErrGCInvariant) || worker.Snapshot().FatalCode != test.fatal {
					t.Fatalf("ReconcileStartup error/snapshot = (%v, %#v), want %q", err,
						worker.Snapshot(), test.fatal)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if store.queueLength() != 0 {
				t.Fatalf("queue length = %d, want zero", store.queueLength())
			}
			status, err := cas.InspectTombstone(digest, token)
			if err != nil || status.State != CASTombstoneAbsent || !status.Closed {
				t.Fatalf("settled CAS state = (%#v, %v)", status, err)
			}
			snapshot := worker.Snapshot()
			if snapshot.StartupReconciliations != 1 || snapshot.QueueItemsCompleted != 1 ||
				snapshot.FatalCode != GCFatalNone {
				t.Fatalf("startup snapshot = %#v", snapshot)
			}
		})
	}
}

func TestGCRecoversStoreResponseLossAcrossRestart(t *testing.T) {
	t.Run("prepare", func(t *testing.T) {
		cas := gcOpenCAS(t)
		now := gcTestNow()
		digest := gcPutOld(t, cas, []byte("prepare response loss"), now)
		store := newGCTestStore()
		store.prepareResponseLoss = true
		first := gcNewWorker(t, store, cas, GCOptions{Clock: func() time.Time { return now }})
		if err := first.RunCycle(context.Background()); !errors.Is(err, ErrGCInvariant) ||
			first.Snapshot().FatalCode != GCFatalStoreFailure {
			t.Fatalf("first cycle = (%v, %#v)", err, first.Snapshot())
		}
		if store.queueLength() != 1 || store.scan.After != digest || store.scan.Done != true {
			t.Fatalf("committed prepare state = queue %d scan %#v", store.queueLength(), store.scan)
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, digest)); err != nil {
			t.Fatalf("prepare response loss altered final object: %v", err)
		}
		restarted := gcNewWorker(t, store, cas, GCOptions{Clock: func() time.Time { return now }})
		if err := restarted.ReconcileStartup(context.Background()); err != nil {
			t.Fatal(err)
		}
		gcRequireCollected(t, store, cas, digest, store.completedToken(digest))
	})

	t.Run("mark renamed", func(t *testing.T) {
		cas := gcOpenCAS(t)
		content := []byte("mark response loss")
		digest := model.Sum(content)
		token := gcToken(41)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest, Token: token},
			SizeBytes: uint64(len(content)), State: GCQueueQueued})
		store.markResponseLoss = true
		first := gcNewWorker(t, store, cas, GCOptions{})
		if err := first.ReconcileStartup(context.Background()); !errors.Is(err, ErrGCInvariant) {
			t.Fatalf("first reconciliation error = %v", err)
		}
		item, found := store.queueItem(GCQueueIdentity{Digest: digest, Token: token})
		status, statusErr := cas.InspectTombstone(digest, token)
		if !found || item.State != GCQueueRenamed || statusErr != nil ||
			status.State != CASTombstoneTrashOnly {
			t.Fatalf("mark response-loss boundary = (%#v, %t, %#v, %v)", item, found, status, statusErr)
		}
		restarted := gcNewWorker(t, store, cas, GCOptions{})
		if err := restarted.ReconcileStartup(context.Background()); err != nil {
			t.Fatal(err)
		}
		gcRequireCollected(t, store, cas, digest, token)
	})

	t.Run("complete", func(t *testing.T) {
		cas := gcOpenCAS(t)
		content := []byte("complete response loss")
		digest := model.Sum(content)
		token := gcToken(51)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.Tombstone(digest, token); err != nil {
			t.Fatal(err)
		}
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest, Token: token},
			SizeBytes: uint64(len(content)), State: GCQueueRenamed})
		store.completeResponseLoss = true
		first := gcNewWorker(t, store, cas, GCOptions{})
		if err := first.ReconcileStartup(context.Background()); !errors.Is(err, ErrGCInvariant) {
			t.Fatalf("first reconciliation error = %v", err)
		}
		gcRequireCollected(t, store, cas, digest, token)
		restarted := gcNewWorker(t, store, cas, GCOptions{})
		if err := restarted.ReconcileStartup(context.Background()); err != nil {
			t.Fatal(err)
		}
		if restarted.Snapshot().QueueItemsCompleted != 0 {
			t.Fatalf("restart invented completion = %#v", restarted.Snapshot())
		}
	})
}

func TestGCBoundsPagesBytesQueueAndAdvancesProtectedCursor(t *testing.T) {
	t.Run("protected prefix", func(t *testing.T) {
		cas := gcOpenCAS(t)
		now := gcTestNow()
		digests := make([]model.Digest, 0, 3)
		for index := 0; index < 3; index++ {
			digests = append(digests, gcPutOld(t, cas,
				[]byte(fmt.Sprintf("protected-prefix-%d", index)), now))
		}
		sort.Slice(digests, func(left, right int) bool {
			return digests[left].String() < digests[right].String()
		})
		store := newGCTestStore()
		store.protected[digests[0]] = true
		store.protected[digests[1]] = true
		worker := gcNewWorker(t, store, cas, GCOptions{
			Clock: func() time.Time { return now }, MaxExamined: 2, MaxQueued: 1,
		})
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.scan.After != digests[1] || store.scan.Done || store.queueLength() != 0 {
			t.Fatalf("protected page cursor/queue = (%#v, %d)", store.scan, store.queueLength())
		}
		if snapshot := worker.Snapshot(); snapshot.ObjectsExamined != 2 ||
			snapshot.ObjectsProtected != 2 || snapshot.ObjectsQueued != 0 {
			t.Fatalf("protected snapshot = %#v", snapshot)
		}
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !store.scan.Done || store.scan.After != digests[2] {
			t.Fatalf("terminal scan = %#v", store.scan)
		}
		for _, digest := range digests[:2] {
			if _, err := os.Lstat(gcObjectPath(t, cas, digest)); err != nil {
				t.Fatalf("protected object %s was removed: %v", digest, err)
			}
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, digests[2])); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan object remains: %v", err)
		}
	})

	t.Run("byte prefix", func(t *testing.T) {
		cas := gcOpenCAS(t)
		now := gcTestNow()
		contentA := bytes.Repeat([]byte{0x61}, 3<<20)
		contentB := bytes.Repeat([]byte{0x62}, 3<<20)
		digests := []model.Digest{gcPutOld(t, cas, contentA, now), gcPutOld(t, cas, contentB, now)}
		store := newGCTestStore()
		worker := gcNewWorker(t, store, cas, GCOptions{
			Clock: func() time.Time { return now }, MaxExamined: 2, MaxQueued: 2,
			MaxBytes: maxCASObjectSize,
		})
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if snapshot := worker.Snapshot(); snapshot.ObjectsExamined != 1 ||
			snapshot.ObjectsQueued != 1 || snapshot.ObjectBytesQueued != 3<<20 {
			t.Fatalf("first byte-bounded snapshot = %#v", snapshot)
		}
		if len(store.prepareSpecs) != 1 || len(store.prepareSpecs[0].Candidates) != 1 ||
			store.prepareSpecs[0].PageDone {
			t.Fatalf("first byte-bounded prepare = %#v", store.prepareSpecs)
		}
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if snapshot := worker.Snapshot(); snapshot.ObjectsExamined != 2 ||
			snapshot.ObjectsQueued != 2 || snapshot.ObjectBytesQueued != 6<<20 {
			t.Fatalf("second byte-bounded snapshot = %#v", snapshot)
		}
		for _, digest := range digests {
			if _, err := os.Lstat(gcObjectPath(t, cas, digest)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("byte-bounded orphan remains: %v", err)
			}
		}
	})
}

func TestGCBatchesDurableQueueByItemsAndBytes(t *testing.T) {
	newFixture := func(t *testing.T) (*CAS, *gcTestStore) {
		t.Helper()
		cas := gcOpenCAS(t)
		store := newGCTestStore()
		for index := 0; index < 3; index++ {
			content := bytes.Repeat([]byte{byte(0x31 + index)}, 3<<20)
			digest := model.Sum(content)
			token := gcToken(byte(101 + index))
			if _, err := cas.Put(digest, content); err != nil {
				t.Fatal(err)
			}
			store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest, Token: token},
				SizeBytes: uint64(len(content)), State: GCQueueQueued})
		}
		return cas, store
	}

	t.Run("periodic cycle", func(t *testing.T) {
		cas, store := newFixture(t)
		worker := gcNewWorker(t, store, cas, GCOptions{
			MaxExamined: 3, MaxQueued: 2, MaxBytes: maxCASObjectSize, MaxQueue: 4,
		})
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.queueLength() != 2 || worker.Snapshot().QueueItemsCompleted != 1 ||
			len(store.sweepSpecs) != 0 {
			t.Fatalf("first periodic batch = queue %d snapshot %#v sweeps %d",
				store.queueLength(), worker.Snapshot(), len(store.sweepSpecs))
		}
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.queueLength() != 1 || worker.Snapshot().QueueItemsCompleted != 2 ||
			len(store.sweepSpecs) != 0 {
			t.Fatalf("second periodic batch = queue %d snapshot %#v sweeps %d",
				store.queueLength(), worker.Snapshot(), len(store.sweepSpecs))
		}
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.queueLength() != 0 || worker.Snapshot().QueueItemsCompleted != 3 ||
			len(store.sweepSpecs) != 1 {
			t.Fatalf("terminal periodic batch = queue %d snapshot %#v sweeps %d",
				store.queueLength(), worker.Snapshot(), len(store.sweepSpecs))
		}
	})

	t.Run("startup drains bounded batches with one trusted time", func(t *testing.T) {
		cas, store := newFixture(t)
		now := gcTestNow()
		worker := gcNewWorker(t, store, cas, GCOptions{
			Clock: func() time.Time { return now }, MaxExamined: 3, MaxQueued: 2,
			MaxBytes: maxCASObjectSize, MaxQueue: 4,
		})
		if err := worker.ReconcileStartup(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.queueLength() != 0 || worker.Snapshot().QueueItemsCompleted != 3 ||
			worker.Snapshot().Reconciliations != 3 || worker.Snapshot().StartupReconciliations != 1 {
			t.Fatalf("startup batches = queue %d snapshot %#v", store.queueLength(), worker.Snapshot())
		}
		for _, spec := range append(append([]GCQueueTransitionSpec{}, store.markSpecs...),
			store.completeSpecs...) {
			if spec.At != now {
				t.Fatalf("startup transition used %s, want one trusted %s", spec.At, now)
			}
		}
	})

	t.Run("residual bytes defer a new scan candidate", func(t *testing.T) {
		cas := gcOpenCAS(t)
		now := gcTestNow()
		queuedContent := bytes.Repeat([]byte{0x71}, 3<<20)
		queuedDigest := model.Sum(queuedContent)
		queuedToken := gcToken(111)
		if _, err := cas.Put(queuedDigest, queuedContent); err != nil {
			t.Fatal(err)
		}
		orphanDigest := gcPutOld(t, cas, bytes.Repeat([]byte{0x72}, 2<<20), now)
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: queuedDigest, Token: queuedToken},
			SizeBytes: uint64(len(queuedContent)), State: GCQueueQueued})
		worker := gcNewWorker(t, store, cas, GCOptions{
			Clock: func() time.Time { return now }, MaxExamined: 2, MaxQueued: 2,
			MaxBytes: maxCASObjectSize,
		})
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, orphanDigest)); err != nil {
			t.Fatalf("orphan did not wait for a fresh byte budget: %v", err)
		}
		if len(store.prepareSpecs) != 0 || store.scan.After != (model.Digest{}) {
			t.Fatalf("residual budget advanced durable scan: specs=%d cursor=%#v",
				len(store.prepareSpecs), store.scan)
		}
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, orphanDigest)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fresh byte budget did not collect orphan: %v", err)
		}
	})

	t.Run("queue staging and prepare share the cycle byte budget", func(t *testing.T) {
		cas := gcOpenCAS(t)
		now := gcTestNow()
		queuedContent := bytes.Repeat([]byte{0x73}, 2<<20)
		queuedDigest := model.Sum(queuedContent)
		queuedToken := gcToken(121)
		if _, err := cas.Put(queuedDigest, queuedContent); err != nil {
			t.Fatal(err)
		}
		orphanDigest := gcPutOld(t, cas, bytes.Repeat([]byte{0x74}, 2<<20), now)
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: queuedDigest, Token: queuedToken},
			SizeBytes: uint64(len(queuedContent)), State: GCQueueQueued})
		store.sweepResult = GCStagingSweepResult{Examined: 1, Swept: 1, SweptBytes: 2 << 20}
		worker := gcNewWorker(t, store, cas, GCOptions{
			Clock: func() time.Time { return now }, MaxExamined: 3, MaxQueued: 2,
			MaxBytes: maxCASObjectSize,
		})
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(store.sweepSpecs) != 1 || store.sweepSpecs[0].MaxItems != 2 ||
			store.sweepSpecs[0].MaxBytes != 2<<20 || len(store.prepareSpecs) != 0 {
			t.Fatalf("shared byte budget = sweep %#v prepare %d",
				store.sweepSpecs, len(store.prepareSpecs))
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, orphanDigest)); err != nil {
			t.Fatalf("orphan did not wait after the cycle byte budget was spent: %v", err)
		}
		store.mu.Lock()
		store.sweepResult = GCStagingSweepResult{}
		store.mu.Unlock()
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, orphanDigest)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fresh cycle did not collect deferred orphan: %v", err)
		}
	})

	t.Run("queue staging and prepare share the cycle item budget", func(t *testing.T) {
		cas := gcOpenCAS(t)
		now := gcTestNow()
		queuedContent := []byte("item-budget queued object")
		queuedDigest := model.Sum(queuedContent)
		queuedToken := gcToken(125)
		if _, err := cas.Put(queuedDigest, queuedContent); err != nil {
			t.Fatal(err)
		}
		orphanDigest := gcPutOld(t, cas, []byte("item-budget deferred orphan"), now)
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: queuedDigest, Token: queuedToken},
			SizeBytes: uint64(len(queuedContent)), State: GCQueueQueued})
		store.sweepResult = GCStagingSweepResult{Examined: 1}
		worker := gcNewWorker(t, store, cas, GCOptions{
			Clock: func() time.Time { return now }, MaxExamined: 2, MaxQueued: 2,
		})
		if err := worker.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(store.prepareSpecs) != 0 {
			t.Fatalf("prepare ran after cycle item budget was spent: %#v", store.prepareSpecs)
		}
		if _, err := os.Lstat(gcObjectPath(t, cas, orphanDigest)); err != nil {
			t.Fatalf("orphan did not wait after the cycle item budget was spent: %v", err)
		}
	})
}

func TestGCPrunesOnlyOldRecognizableTempsAndSweepsThroughStore(t *testing.T) {
	cas := gcOpenCAS(t)
	now := gcTestNow()
	oldTemp := filepath.Join(cas.temp, "cas-11111111111111111111111111111111.tmp")
	newTemp := filepath.Join(cas.temp, "cas-22222222222222222222222222222222.tmp")
	for _, path := range []string{oldTemp, newTemp} {
		if err := os.WriteFile(path, []byte("bounded temp"), casObjectMode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(oldTemp, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newTemp, now, now); err != nil {
		t.Fatal(err)
	}
	store := newGCTestStore()
	store.sweepResult = GCStagingSweepResult{Examined: 3, Swept: 2, SweptBytes: 42}
	worker := gcNewWorker(t, store, cas, GCOptions{Clock: func() time.Time { return now }, MaxTemps: 1})
	if err := worker.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(oldTemp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old recognizable temp remains: %v", err)
	}
	if _, err := os.Lstat(newTemp); err != nil {
		t.Fatalf("fresh temp was removed: %v", err)
	}
	if len(store.sweepSpecs) != 1 || store.sweepSpecs[0].Cutoff != now.Add(-time.Hour) ||
		store.sweepSpecs[0].MaxItems != gcDefaultMaxExamined ||
		store.sweepSpecs[0].MaxBytes != gcDefaultMaxBytes {
		t.Fatalf("staging sweep spec = %#v", store.sweepSpecs)
	}
	if snapshot := worker.Snapshot(); snapshot.TempsPruned != 1 ||
		snapshot.StagingExamined != 3 || snapshot.StagingSwept != 2 ||
		snapshot.StagingBytesSwept != 42 {
		t.Fatalf("prune/sweep snapshot = %#v", snapshot)
	}
}

func TestGCFailsClosedForForeignTombstoneAndQueueOverflow(t *testing.T) {
	t.Run("foreign token", func(t *testing.T) {
		cas := gcOpenCAS(t)
		content := []byte("foreign durable tombstone")
		digest := model.Sum(content)
		queueToken := gcToken(71)
		foreignToken := gcToken(72)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.Tombstone(digest, foreignToken); err != nil {
			t.Fatal(err)
		}
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest, Token: queueToken},
			SizeBytes: uint64(len(content)), State: GCQueueQueued})
		worker := gcNewWorker(t, store, cas, GCOptions{})
		err := worker.ReconcileStartup(context.Background())
		if !errors.Is(err, ErrGCInvariant) || worker.Snapshot().FatalCode != GCFatalCASInvariant {
			t.Fatalf("foreign tombstone = (%v, %#v)", err, worker.Snapshot())
		}
	})

	t.Run("queue overflow", func(t *testing.T) {
		cas := gcOpenCAS(t)
		store := newGCTestStore()
		for index := 0; index < 3; index++ {
			content := []byte(fmt.Sprintf("overflow-%d", index))
			digest := model.Sum(content)
			if _, err := cas.Put(digest, content); err != nil {
				t.Fatal(err)
			}
			store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest,
				Token: gcToken(byte(80 + index))}, SizeBytes: uint64(len(content)), State: GCQueueQueued})
		}
		worker := gcNewWorker(t, store, cas, GCOptions{MaxExamined: 2, MaxQueued: 1, MaxQueue: 2})
		err := worker.ReconcileStartup(context.Background())
		if !errors.Is(err, ErrGCInvariant) || worker.Snapshot().FatalCode != GCFatalStoreInvariant {
			t.Fatalf("queue overflow = (%v, %#v)", err, worker.Snapshot())
		}
	})
}

func TestGCExclusiveFenceAndCancellationLeaveReplayableBoundaries(t *testing.T) {
	t.Run("exclusive blocks use", func(t *testing.T) {
		cas := gcOpenCAS(t)
		store := newGCTestStore()
		store.sweepStarted = make(chan struct{}, 1)
		store.sweepRelease = make(chan struct{})
		worker := gcNewWorker(t, store, cas, GCOptions{})
		cycleDone := make(chan error, 1)
		go func() { cycleDone <- worker.RunCycle(context.Background()) }()
		select {
		case <-store.sweepStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("collector did not enter Store sweep while exclusive")
		}
		useAcquired := make(chan *CASLease, 1)
		go func() {
			lease, err := cas.AcquireUse()
			if err != nil {
				useAcquired <- nil
				return
			}
			useAcquired <- lease
		}()
		select {
		case lease := <-useAcquired:
			if lease != nil {
				lease.Release()
			}
			t.Fatal("CAS use entered while GC held the exclusive lifecycle fence")
		case <-time.After(75 * time.Millisecond):
		}
		close(store.sweepRelease)
		if err := <-cycleDone; err != nil {
			t.Fatal(err)
		}
		select {
		case lease := <-useAcquired:
			if lease == nil {
				t.Fatal("AcquireUse failed after collection")
			}
			lease.Release()
		case <-time.After(2 * time.Second):
			t.Fatal("CAS use remained blocked after collection")
		}
	})

	t.Run("trusted clock is sampled after exclusive acquisition", func(t *testing.T) {
		cas := gcOpenCAS(t)
		use, err := cas.AcquireUse()
		if err != nil {
			t.Fatal(err)
		}
		clockCalled := make(chan struct{}, 1)
		worker := gcNewWorker(t, newGCTestStore(), cas, GCOptions{Clock: func() time.Time {
			select {
			case clockCalled <- struct{}{}:
			default:
			}
			return gcTestNow()
		}})
		done := make(chan error, 1)
		go func() { done <- worker.RunCycle(context.Background()) }()
		select {
		case <-clockCalled:
			use.Release()
			t.Fatal("trusted clock was sampled before the exclusive CAS fence")
		case <-time.After(75 * time.Millisecond):
		}
		use.Release()
		select {
		case <-clockCalled:
		case <-time.After(2 * time.Second):
			t.Fatal("trusted clock was not sampled after exclusive acquisition")
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cancel after tombstone", func(t *testing.T) {
		cas := gcOpenCAS(t)
		content := []byte("cancel at replayable GC boundary")
		digest := model.Sum(content)
		token := gcToken(91)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		store := newGCTestStore()
		store.addQueue(GCQueueItem{Identity: GCQueueIdentity{Digest: digest, Token: token},
			SizeBytes: uint64(len(content)), State: GCQueueQueued})
		ctx, cancel := context.WithCancel(context.Background())
		store.cancelBeforeMark = cancel
		worker := gcNewWorker(t, store, cas, GCOptions{})
		if err := worker.ReconcileStartup(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled reconciliation error = %v", err)
		}
		item, found := store.queueItem(GCQueueIdentity{Digest: digest, Token: token})
		status, err := cas.InspectTombstone(digest, token)
		if !found || item.State != GCQueueQueued || err != nil || status.State != CASTombstoneTrashOnly {
			t.Fatalf("cancel boundary = (%#v, %t, %#v, %v)", item, found, status, err)
		}
		if snapshot := worker.Snapshot(); snapshot.FatalCode != GCFatalNone || snapshot.State != GCIdle {
			t.Fatalf("cancellation was reported as fatal: %#v", snapshot)
		}
		restarted := gcNewWorker(t, store, cas, GCOptions{})
		if err := restarted.ReconcileStartup(context.Background()); err != nil {
			t.Fatal(err)
		}
		gcRequireCollected(t, store, cas, digest, token)
	})
}

func TestGCFatalInvariantWinsConcurrentCancellation(t *testing.T) {
	for _, name := range []string{"run cycle", "periodic run"} {
		t.Run(name, func(t *testing.T) {
			cas := gcOpenCAS(t)
			store := newGCTestStore()
			ctx, cancel := context.WithCancel(context.Background())
			store.cancelDuringSweep = cancel
			store.sweepError = ErrGCStoreInvariant
			worker := gcNewWorker(t, store, cas, GCOptions{})
			var err error
			if name == "periodic run" {
				err = worker.Run(ctx)
			} else {
				err = worker.RunCycle(ctx)
			}
			if !errors.Is(err, ErrGCInvariant) || errors.Is(err, context.Canceled) ||
				worker.Snapshot().State != GCFailed ||
				worker.Snapshot().FatalCode != GCFatalStoreInvariant {
				t.Fatalf("simultaneous fatal/cancel = (%v, %#v)", err, worker.Snapshot())
			}
		})
	}

	t.Run("startup reconciliation", func(t *testing.T) {
		cas := gcOpenCAS(t)
		store := newGCTestStore()
		ctx, cancel := context.WithCancel(context.Background())
		store.cancelDuringList = cancel
		store.listError = ErrGCStoreInvariant
		worker := gcNewWorker(t, store, cas, GCOptions{})
		err := worker.ReconcileStartup(ctx)
		if !errors.Is(err, ErrGCInvariant) || errors.Is(err, context.Canceled) ||
			worker.Snapshot().State != GCFailed ||
			worker.Snapshot().FatalCode != GCFatalStoreInvariant {
			t.Fatalf("startup simultaneous fatal/cancel = (%v, %#v)", err, worker.Snapshot())
		}
	})
}

func TestGCFatalErrorDoesNotExposeStoreCause(t *testing.T) {
	const secret = "/private/node/.trash/sha256-secret.deadbeef-token"
	store := newGCTestStore()
	store.sweepError = errors.New(secret)
	worker := gcNewWorker(t, store, gcOpenCAS(t), GCOptions{})
	err := worker.RunCycle(context.Background())
	if !errors.Is(err, ErrGCInvariant) || worker.Snapshot().FatalCode != GCFatalStoreFailure {
		t.Fatalf("fatal Store error = (%v, %#v)", err, worker.Snapshot())
	}
	for _, projection := range []string{err.Error(), fmt.Sprintf("%#v", err),
		fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", worker.Snapshot())} {
		if strings.Contains(projection, secret) {
			t.Fatalf("fatal projection exposed Store cause: %q", projection)
		}
	}
}

func TestGCRunIsSingleUseAndStopsCleanly(t *testing.T) {
	cas := gcOpenCAS(t)
	store := newGCTestStore()
	store.sweepStarted = make(chan struct{}, 2)
	worker := gcNewWorker(t, store, cas, GCOptions{Period: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-store.sweepStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic worker did not run its immediate cycle")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("periodic worker did not stop")
	}
	if snapshot := worker.Snapshot(); snapshot.State != GCStopped || snapshot.Cycles != 1 ||
		snapshot.FatalCode != GCFatalNone {
		t.Fatalf("stopped snapshot = %#v", snapshot)
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrGCRunning) {
		t.Fatalf("second Run error = %v", err)
	}
}

func TestGCRejectsInvalidOptionsClockAndStoreResults(t *testing.T) {
	cas := gcOpenCAS(t)
	store := newGCTestStore()
	if _, err := NewGCWorker(GCOptions{Store: store, CAS: cas, MaxBytes: 1}); !errors.Is(err, ErrGC) {
		t.Fatalf("undersized byte budget error = %v", err)
	}
	worker := gcNewWorker(t, store, cas, GCOptions{Clock: func() time.Time { return time.Time{} }})
	if err := worker.RunCycle(context.Background()); !errors.Is(err, ErrGCInvariant) ||
		worker.Snapshot().FatalCode != GCFatalWorkerInvariant {
		t.Fatalf("invalid clock = (%v, %#v)", err, worker.Snapshot())
	}

	badSweep := newGCTestStore()
	badSweep.sweepResult = GCStagingSweepResult{Examined: gcDefaultMaxExamined + 1}
	worker = gcNewWorker(t, badSweep, gcOpenCAS(t), GCOptions{})
	if err := worker.RunCycle(context.Background()); !errors.Is(err, ErrGCInvariant) ||
		worker.Snapshot().FatalCode != GCFatalStoreInvariant {
		t.Fatalf("invalid Store result = (%v, %#v)", err, worker.Snapshot())
	}

	zeroRandom := bytes.NewReader(make([]byte, 32*gcRandomTokenAttempts))
	now := gcTestNow()
	zeroCAS := gcOpenCAS(t)
	gcPutOld(t, zeroCAS, []byte("zero token candidate"), now)
	worker = gcNewWorker(t, newGCTestStore(), zeroCAS, GCOptions{
		Clock: func() time.Time { return now }, Random: zeroRandom,
	})
	if err := worker.RunCycle(context.Background()); !errors.Is(err, ErrGCInvariant) ||
		worker.Snapshot().FatalCode != GCFatalWorkerInvariant {
		t.Fatalf("zero random source = (%v, %#v)", err, worker.Snapshot())
	}

	wrongQueue := newGCTestStore()
	wrongQueue.wrongPreparedToken = true
	wrongCAS := gcOpenCAS(t)
	wrongDigest := gcPutOld(t, wrongCAS, []byte("wrong durable prepare token"), now)
	worker = gcNewWorker(t, wrongQueue, wrongCAS, GCOptions{Clock: func() time.Time { return now }})
	if err := worker.RunCycle(context.Background()); !errors.Is(err, ErrGCInvariant) ||
		worker.Snapshot().FatalCode != GCFatalStoreInvariant {
		t.Fatalf("wrong prepared token = (%v, %#v)", err, worker.Snapshot())
	}
	if _, err := os.Lstat(gcObjectPath(t, wrongCAS, wrongDigest)); err != nil {
		t.Fatalf("worker touched object before exact-token validation: %v", err)
	}
}

type gcTestStore struct {
	mu sync.Mutex

	scan        GCScanCursor
	scanPresent bool
	protected   map[model.Digest]bool
	queue       map[string]GCQueueItem
	completed   map[string]gcTestCompletion

	prepareSpecs  []GCPrepareSpec
	sweepSpecs    []GCStagingSweepSpec
	markSpecs     []GCQueueTransitionSpec
	completeSpecs []GCQueueTransitionSpec
	sweepResult   GCStagingSweepResult

	prepareResponseLoss  bool
	markResponseLoss     bool
	completeResponseLoss bool
	wrongPreparedToken   bool
	cancelBeforeMark     context.CancelFunc
	cancelDuringSweep    context.CancelFunc
	cancelDuringList     context.CancelFunc
	sweepError           error
	listError            error
	sweepStarted         chan struct{}
	sweepRelease         chan struct{}
}

func newGCTestStore() *gcTestStore {
	return &gcTestStore{protected: make(map[model.Digest]bool), queue: make(map[string]GCQueueItem),
		completed: make(map[string]gcTestCompletion)}
}

type gcTestCompletion struct {
	identity GCQueueIdentity
	at       time.Time
}

func (store *gcTestStore) OpenArtifactGCScan(ctx context.Context, spec GCScanSpec) (GCScanCursor, error) {
	if err := ctx.Err(); err != nil {
		return GCScanCursor{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.scanPresent || store.scan.Done {
		store.scan = GCScanCursor{Cutoff: spec.InitializeCutoff}
		store.scanPresent = true
	}
	return store.scan, nil
}

func (store *gcTestStore) PrepareArtifactGC(ctx context.Context, spec GCPrepareSpec) (GCPrepareResult, error) {
	if err := ctx.Err(); err != nil {
		return GCPrepareResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.scanPresent || store.scan != spec.Current {
		return GCPrepareResult{}, fmt.Errorf("%w: scan cursor changed", ErrGCStoreInvariant)
	}
	copySpec := spec
	copySpec.Candidates = append([]GCCandidate(nil), spec.Candidates...)
	store.prepareSpecs = append(store.prepareSpecs, copySpec)
	result := GCPrepareResult{Next: store.scan}
	for _, candidate := range spec.Candidates {
		if result.Queued >= spec.MaxQueued || len(store.queue) >= spec.MaxQueue {
			break
		}
		result.Examined++
		result.Next.After = candidate.Digest
		if store.protected[candidate.Digest] {
			result.Protected++
			continue
		}
		token := candidate.Token
		if store.wrongPreparedToken {
			token[0] ^= 0xff
		}
		item := GCQueueItem{Identity: GCQueueIdentity{Digest: candidate.Digest, Token: token},
			SizeBytes: candidate.SizeBytes, ModifiedAt: candidate.ModifiedAt,
			QueuedAt: spec.At, State: GCQueueQueued}
		store.queue[gcQueueKey(item.Identity)] = item
		result.Queued++
		result.QueuedBytes += candidate.SizeBytes
	}
	result.Next.Done = spec.PageDone && result.Examined == len(spec.Candidates)
	store.scan = result.Next
	if store.prepareResponseLoss {
		store.prepareResponseLoss = false
		return result, errors.New("injected prepare response loss")
	}
	return result, nil
}

func (store *gcTestStore) SweepArtifactGCStaging(ctx context.Context,
	spec GCStagingSweepSpec,
) (GCStagingSweepResult, error) {
	if store.sweepStarted != nil {
		select {
		case store.sweepStarted <- struct{}{}:
		default:
		}
	}
	if store.sweepRelease != nil {
		select {
		case <-store.sweepRelease:
		case <-ctx.Done():
			return GCStagingSweepResult{}, ctx.Err()
		}
	}
	store.mu.Lock()
	cancel := store.cancelDuringSweep
	store.cancelDuringSweep = nil
	sweepErr := store.sweepError
	store.sweepError = nil
	store.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if sweepErr != nil {
		return GCStagingSweepResult{}, sweepErr
	}
	if err := ctx.Err(); err != nil {
		return GCStagingSweepResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.sweepSpecs = append(store.sweepSpecs, spec)
	return store.sweepResult, nil
}

func (store *gcTestStore) ListArtifactGCQueue(ctx context.Context, limit int) ([]GCQueueItem, error) {
	store.mu.Lock()
	cancel := store.cancelDuringList
	store.cancelDuringList = nil
	listErr := store.listError
	store.listError = nil
	store.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if listErr != nil {
		return nil, listErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	items := make([]GCQueueItem, 0, len(store.queue))
	for _, item := range store.queue {
		items = append(items, item)
	}
	sort.Slice(items, func(left, right int) bool {
		return gcQueueIdentityLess(items[left].Identity, items[right].Identity)
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return append([]GCQueueItem(nil), items...), nil
}

func (store *gcTestStore) GetArtifactGCQueue(ctx context.Context,
	identity GCQueueIdentity,
) (GCQueueItem, bool, error) {
	if err := ctx.Err(); err != nil {
		return GCQueueItem{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	item, found := store.queue[gcQueueKey(identity)]
	return item, found, nil
}

func (store *gcTestStore) MarkArtifactGCRenamed(ctx context.Context,
	spec GCQueueTransitionSpec,
) (GCQueueTransitionResult, error) {
	store.mu.Lock()
	cancel := store.cancelBeforeMark
	store.cancelBeforeMark = nil
	store.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if err := ctx.Err(); err != nil {
		return GCQueueTransitionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.markSpecs = append(store.markSpecs, spec)
	key := gcQueueKey(spec.Identity)
	item, found := store.queue[key]
	if !found {
		return GCQueueTransitionResult{}, fmt.Errorf("%w: queue item missing", ErrGCStoreInvariant)
	}
	replayed := item.State == GCQueueRenamed
	if item.State != GCQueueQueued && !replayed {
		return GCQueueTransitionResult{}, fmt.Errorf("%w: invalid queue transition", ErrGCStoreInvariant)
	}
	item.State = GCQueueRenamed
	item.RenamedAt = spec.At
	store.queue[key] = item
	result := GCQueueTransitionResult{State: GCQueueRenamed, Replayed: replayed, At: item.RenamedAt}
	if store.markResponseLoss {
		store.markResponseLoss = false
		return result, errors.New("injected mark response loss")
	}
	return result, nil
}

func (store *gcTestStore) CompleteArtifactGC(ctx context.Context,
	spec GCQueueTransitionSpec,
) (GCQueueTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return GCQueueTransitionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.completeSpecs = append(store.completeSpecs, spec)
	key := gcQueueKey(spec.Identity)
	item, found := store.queue[key]
	if !found {
		if completed, present := store.completed[key]; present && completed.identity == spec.Identity {
			return GCQueueTransitionResult{Completed: true, Replayed: true, At: completed.at}, nil
		}
		return GCQueueTransitionResult{}, fmt.Errorf("%w: queue item missing", ErrGCStoreInvariant)
	}
	if item.State != GCQueueRenamed {
		return GCQueueTransitionResult{}, fmt.Errorf("%w: queue item is not renamed", ErrGCStoreInvariant)
	}
	delete(store.queue, key)
	store.completed[key] = gcTestCompletion{identity: spec.Identity, at: spec.At}
	result := GCQueueTransitionResult{Completed: true, At: spec.At}
	if store.completeResponseLoss {
		store.completeResponseLoss = false
		return result, errors.New("injected complete response loss")
	}
	return result, nil
}

func (store *gcTestStore) addQueue(item GCQueueItem) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if item.ModifiedAt.IsZero() {
		item.ModifiedAt = gcTestNow().Add(-2 * time.Hour)
	}
	if item.QueuedAt.IsZero() {
		item.QueuedAt = gcTestNow().Add(-30 * time.Minute)
	}
	if item.State == GCQueueRenamed && item.RenamedAt.IsZero() {
		item.RenamedAt = gcTestNow().Add(-20 * time.Minute)
	}
	store.queue[gcQueueKey(item.Identity)] = item
}

func (store *gcTestStore) queueLength() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.queue)
}

func (store *gcTestStore) queueItem(identity GCQueueIdentity) (GCQueueItem, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	item, found := store.queue[gcQueueKey(identity)]
	return item, found
}

func (store *gcTestStore) completedToken(digest model.Digest) [32]byte {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, completed := range store.completed {
		if completed.identity.Digest == digest {
			return completed.identity.Token
		}
	}
	return [32]byte{}
}

func gcNewWorker(t *testing.T, store GCStore, cas *CAS, override GCOptions) *GCWorker {
	t.Helper()
	options := GCOptions{
		Store: store, CAS: cas, Clock: func() time.Time { return gcTestNow() },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x5a}, 32*1024)),
		Period: 10 * time.Millisecond, ObjectTTL: time.Hour, StagingTTL: time.Hour,
		TempTTL: time.Hour, MaxExamined: gcDefaultMaxExamined, MaxQueued: gcDefaultMaxQueued,
		MaxBytes: gcDefaultMaxBytes, MaxTemps: gcDefaultMaxTemps, MaxQueue: gcDefaultMaxQueue,
	}
	if override.Clock != nil {
		options.Clock = override.Clock
	}
	if override.Random != nil {
		options.Random = override.Random
	}
	if override.Period != 0 {
		options.Period = override.Period
	}
	if override.ObjectTTL != 0 {
		options.ObjectTTL = override.ObjectTTL
	}
	if override.StagingTTL != 0 {
		options.StagingTTL = override.StagingTTL
	}
	if override.TempTTL != 0 {
		options.TempTTL = override.TempTTL
	}
	if override.MaxExamined != 0 {
		options.MaxExamined = override.MaxExamined
	}
	if override.MaxQueued != 0 {
		options.MaxQueued = override.MaxQueued
	}
	if override.MaxBytes != 0 {
		options.MaxBytes = override.MaxBytes
	}
	if override.MaxTemps != 0 {
		options.MaxTemps = override.MaxTemps
	}
	if override.MaxQueue != 0 {
		options.MaxQueue = override.MaxQueue
	}
	worker, err := NewGCWorker(options)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func gcOpenCAS(t *testing.T) *CAS {
	t.Helper()
	cas, err := NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}

func gcTestNow() time.Time {
	return time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
}

func gcPutOld(t *testing.T, cas *CAS, content []byte, now time.Time) model.Digest {
	t.Helper()
	digest := model.Sum(content)
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	modified := now.Add(-2 * time.Hour)
	if err := os.Chtimes(gcObjectPath(t, cas, digest), modified, modified); err != nil {
		t.Fatal(err)
	}
	return digest
}

func gcObjectPath(t *testing.T, cas *CAS, digest model.Digest) string {
	t.Helper()
	path, err := cas.objectPath(digest, false)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func gcToken(seed byte) [32]byte {
	var token [32]byte
	for index := range token {
		token[index] = seed + byte(index)
	}
	if token == ([32]byte{}) {
		panic("test generated zero GC token")
	}
	return token
}

func gcArrangeCASState(t *testing.T, cas *CAS, digest model.Digest, token [32]byte,
	state CASTombstoneState,
) {
	t.Helper()
	switch state {
	case CASTombstoneFinalOnly:
	case CASTombstoneFinalAndTrash:
		if err := os.Link(gcObjectPath(t, cas, digest), cas.tombstonePath(digest, token)); err != nil {
			t.Fatal(err)
		}
	case CASTombstoneTrashOnly:
		if _, err := cas.Tombstone(digest, token); err != nil {
			t.Fatal(err)
		}
	case CASTombstoneAbsent:
		if _, err := cas.Tombstone(digest, token); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.PurgeTombstone(digest, token); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported test CAS state %q", state)
	}
}

func gcRequireCollected(t *testing.T, store *gcTestStore, cas *CAS,
	digest model.Digest, token [32]byte,
) {
	t.Helper()
	if token == ([32]byte{}) {
		t.Fatal("completed queue did not retain its exact nonzero token evidence")
	}
	if store.queueLength() != 0 {
		t.Fatalf("queue length = %d, want zero", store.queueLength())
	}
	status, err := cas.InspectTombstone(digest, token)
	if err != nil || status.State != CASTombstoneAbsent || !status.Closed {
		t.Fatalf("collected state = (%#v, %v)", status, err)
	}
}

var _ GCStore = (*gcTestStore)(nil)
var _ io.Reader = (*bytes.Reader)(nil)
