package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestArtifactGCScanPrepareCursorQueueAndResponseReplay(t *testing.T) {
	st := openTestStore(t)
	now := artifactGCStoreTestTime()
	cutoff := now.Add(-time.Hour)
	cursor, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
		InitializeCutoff: cutoff, At: now,
	})
	if err != nil || cursor != (artifactdomain.GCScanCursor{Cutoff: cutoff}) {
		t.Fatalf("open scan = (%#v, %v)", cursor, err)
	}

	manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
	manifestDigest := model.Sum(manifest.Bytes())
	var lower, upper model.Digest
	for index := 0; lower.IsZero() || upper.IsZero(); index++ {
		digest := model.Sum([]byte(fmt.Sprintf("artifact-gc-order-%d", index)))
		if digest.String() < manifestDigest.String() && lower.IsZero() {
			lower = digest
		}
		if digest.String() > manifestDigest.String() && upper.IsZero() {
			upper = digest
		}
	}
	candidates := []artifactdomain.GCCandidate{
		{Digest: lower, SizeBytes: 1, ModifiedAt: now.Add(-2 * time.Hour), Token: artifactGCStoreToken(1)},
		{Digest: manifestDigest, SizeBytes: 2, ModifiedAt: now.Add(-2 * time.Hour), Token: artifactGCStoreToken(2)},
		{Digest: upper, SizeBytes: 3, ModifiedAt: now.Add(-2 * time.Hour), Token: artifactGCStoreToken(3)},
	}
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), VerifiedArtifactRoot{
		RootDigest: model.Sum([]byte("artifact-gc-protected-root")), Manifest: manifest, ManifestDigest: manifestDigest,
		CreatedAt: now.Add(-2 * time.Hour), VerifiedAt: now.Add(-2 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	spec := artifactdomain.GCPrepareSpec{Current: cursor, Candidates: candidates,
		MaxQueued: 1, MaxQueue: 1, At: now}
	prepared, err := st.PrepareArtifactGC(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Examined != 2 || prepared.Queued != 1 || prepared.Protected != 1 ||
		prepared.Next.After != candidates[1].Digest || prepared.Next.Done ||
		prepared.QueuedBytes != candidates[0].SizeBytes {
		t.Fatalf("prepared prefix = %#v", prepared)
	}
	replayed, err := st.PrepareArtifactGC(context.Background(), spec)
	if err != nil || replayed != prepared {
		t.Fatalf("prepare response replay = (%#v, %v), want %#v", replayed, err, prepared)
	}
	queueFullSpec := artifactdomain.GCPrepareSpec{Current: prepared.Next,
		Candidates: candidates[2:], MaxQueued: 1, MaxQueue: 1, At: now}
	queueFull, err := st.PrepareArtifactGC(context.Background(), queueFullSpec)
	if err != nil || queueFull.Examined != 0 || queueFull.Next != prepared.Next {
		t.Fatalf("queue-full prepare = (%#v, %v)", queueFull, err)
	}
	queueFullReplay, err := st.PrepareArtifactGC(context.Background(), queueFullSpec)
	if err != nil || queueFullReplay != queueFull {
		t.Fatalf("queue-full prepare replay = (%#v, %v), want %#v", queueFullReplay, err, queueFull)
	}
	items, err := st.ListArtifactGCQueue(context.Background(), artifactGCMaxQueueList)
	if err != nil || len(items) != 1 || items[0].Identity.Digest != candidates[0].Digest ||
		items[0].Identity.Token != candidates[0].Token || items[0].ModifiedAt != candidates[0].ModifiedAt ||
		items[0].QueuedAt != now || items[0].State != artifactdomain.GCQueueQueued {
		t.Fatalf("durable queue = (%#v, %v)", items, err)
	}

	mark, err := st.MarkArtifactGCRenamed(context.Background(), artifactdomain.GCQueueTransitionSpec{
		Identity: items[0].Identity, At: now,
	})
	if err != nil || mark.State != artifactdomain.GCQueueRenamed || mark.Completed || mark.Replayed || mark.At != now {
		t.Fatalf("mark = (%#v, %v)", mark, err)
	}
	markReplay, err := st.MarkArtifactGCRenamed(context.Background(), artifactdomain.GCQueueTransitionSpec{
		Identity: items[0].Identity, At: now,
	})
	if err != nil || !markReplay.Replayed || markReplay.At != now {
		t.Fatalf("mark replay = (%#v, %v)", markReplay, err)
	}
	completed, err := st.CompleteArtifactGC(context.Background(), artifactdomain.GCQueueTransitionSpec{
		Identity: items[0].Identity, At: now,
	})
	if err != nil || !completed.Completed || completed.Replayed || completed.At != now {
		t.Fatalf("complete = (%#v, %v)", completed, err)
	}
	if _, err := st.db.Exec(`UPDATE artifact_gc_completion_receipts SET completed_at=?
		WHERE digest=? AND token=?`, storeTime(now.Add(time.Nanosecond)),
		items[0].Identity.Digest.String(), items[0].Identity.Token[:]); err == nil ||
		!strings.Contains(err.Error(), "completion receipt is immutable") {
		t.Fatalf("completion receipt update = %v", err)
	}
	bogusDigest := model.Sum([]byte("artifact-gc-bogus-completion-receipt"))
	bogusToken := artifactGCStoreUniqueToken("bogus-completion-receipt")
	if _, err := st.db.Exec(`INSERT INTO artifact_gc_completion_receipts(digest,token,completed_at)
		VALUES(?,?,?)`, bogusDigest.String(), bogusToken[:], storeTime(now)); err == nil ||
		!strings.Contains(err.Error(), "completion receipt requires renamed queue") {
		t.Fatalf("completion receipt forgery = %v", err)
	}

	path := st.Path()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	completeReplay, err := restarted.CompleteArtifactGC(context.Background(),
		artifactdomain.GCQueueTransitionSpec{Identity: items[0].Identity, At: now})
	if err != nil || !completeReplay.Completed || !completeReplay.Replayed || completeReplay.At != now {
		t.Fatalf("restart complete replay = (%#v, %v)", completeReplay, err)
	}
	if _, err := restarted.CompleteArtifactGC(context.Background(), artifactdomain.GCQueueTransitionSpec{
		Identity: items[0].Identity, At: now.Add(time.Nanosecond),
	}); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) {
		t.Fatalf("tampered completion replay error = %v", err)
	}

	current, err := restarted.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
		InitializeCutoff: cutoff, At: now,
	})
	if err != nil || current != prepared.Next {
		t.Fatalf("restart cursor = (%#v, %v), want %#v", current, err, prepared.Next)
	}
	terminalSpec := artifactdomain.GCPrepareSpec{Current: current, Candidates: candidates[2:],
		PageDone: true, MaxQueued: 1, MaxQueue: 1, At: now}
	terminal, err := restarted.PrepareArtifactGC(context.Background(), terminalSpec)
	if err != nil || !terminal.Next.Done || terminal.Next.After != candidates[2].Digest || terminal.Queued != 1 {
		t.Fatalf("terminal prepare = (%#v, %v)", terminal, err)
	}
	if _, err := restarted.db.Exec(`UPDATE artifact_gc_scan SET "after"=?,done=0
		WHERE singleton=1 AND done=1`, candidates[2].Digest.String()); err == nil ||
		!strings.Contains(err.Error(), "scan cursor cannot regress") {
		t.Fatalf("forged completed scan reset error = %v", err)
	}
	fresh, err := restarted.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
		InitializeCutoff: cutoff, At: now,
	})
	if err != nil || fresh != (artifactdomain.GCScanCursor{Cutoff: cutoff}) {
		t.Fatalf("closed fresh scan = (%#v, %v)", fresh, err)
	}
}

func TestArtifactGCSweepsOnlyExpiredUnacceptedStaging(t *testing.T) {
	t.Run("orphan verified closure and shared blocks", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		closure := artifactClosureFixture(t, "gc-sweep-shared")
		if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
			t.Fatal(err)
		}
		spec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 8, artifactGCMaxSweepBytes, now)
		result, err := st.SweepArtifactGCStaging(context.Background(), spec)
		if err != nil || result.Examined != 2 || result.Swept != 2 || result.SweptBytes != 11 {
			t.Fatalf("sweep = (%#v, %v)", result, err)
		}
		assertArtifactClosureCounts(t, st, 0, 0, 0)
	})

	t.Run("permanent pin and expired Inbox pin", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		first, firstRoot, _ := newArtifactSourceClosure(t, "gc-permanent-pin", []byte("one"), now.Add(-2*time.Hour))
		second, secondRoot, _ := newArtifactSourceClosure(t, "gc-expired-pin", []byte("two"), now.Add(-2*time.Hour))
		for _, closure := range []VerifiedArtifactClosure{first, second} {
			if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,created_at)
			VALUES(?,'retention','gc-permanent',?)`, firstRoot.RootDigest.String(), storeTime(now.Add(-90*time.Minute))); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(`DROP TRIGGER artifact_pins_inbox_owner_insert`); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,expires_at,created_at)
			VALUES(?,'inbox','gc-expired',?,?)`, secondRoot.RootDigest.String(),
			storeTime(now.Add(-time.Minute)), storeTime(now.Add(-2*time.Hour))); err != nil {
			t.Fatal(err)
		}
		spec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 8, artifactGCMaxSweepBytes, now)
		result, err := st.SweepArtifactGCStaging(context.Background(), spec)
		if err != nil || result.Examined != 2 || result.Swept != 1 || result.SweptBytes != secondRoot.TotalBytes {
			t.Fatalf("pin sweep = (%#v, %v)", result, err)
		}
		var firstPresent, secondPresent, expiredPins int
		_ = st.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM artifact_roots WHERE root_digest=?)`,
			firstRoot.RootDigest.String()).Scan(&firstPresent)
		_ = st.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM artifact_roots WHERE root_digest=?)`,
			secondRoot.RootDigest.String()).Scan(&secondPresent)
		_ = st.db.QueryRow(`SELECT COUNT(*) FROM artifact_pins WHERE owner_id='gc-expired'`).Scan(&expiredPins)
		if firstPresent != 1 || secondPresent != 0 || expiredPins != 0 {
			t.Fatalf("pin retention = first %d second %d expired pins %d", firstPresent, secondPresent, expiredPins)
		}
	})

	t.Run("accepted provenance is permanent", func(t *testing.T) {
		fixture := newArtifactSourceFixture(t)
		if _, err := fixture.store.db.Exec(`DROP TRIGGER artifact_provenance_event_insert`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`INSERT INTO artifact_provenance(root_digest,
			producer_event_id,producer_origin_peer_id,relation,created_at)
			VALUES(?,?,?,'replica',?)`, fixture.root.RootDigest.String(), fixture.event.ID().String(),
			fixture.event.Scope().OriginPeerID().String(), storeTime(fixture.event.AcceptedAt())); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`DELETE FROM artifact_pins WHERE root_digest=?`,
			fixture.root.RootDigest.String()); err != nil {
			t.Fatal(err)
		}
		now := fixture.root.CreatedAt.Add(3 * time.Hour)
		spec := artifactGCStoreStagingSpec(t, fixture.store, now.Add(-time.Hour), 8,
			artifactGCMaxSweepBytes, now)
		result, err := fixture.store.SweepArtifactGCStaging(context.Background(), spec)
		if err != nil || result.Swept != 1 {
			t.Fatalf("accepted sweep = (%#v, %v)", result, err)
		}
		var present int
		_ = fixture.store.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM artifact_roots WHERE root_digest=?)`,
			fixture.root.RootDigest.String()).Scan(&present)
		if present != 1 {
			t.Fatal("accepted provenance root was swept")
		}
	})
}

func TestArtifactGCOperationRetentionAndCommittedInvariant(t *testing.T) {
	t.Run("started and rejected TTL", func(t *testing.T) {
		fixture := newOperationCaptureFixture(t, "gc-operation-retention", 1)
		if _, err := fixture.store.CheckpointOperationCapture(context.Background(), fixture.operation.ID(),
			fixture.operation.LeaseOwner(), fixture.now, fixture.capture); err != nil {
			t.Fatal(err)
		}
		before := fixture.now.Add(30 * time.Minute)
		startedSpec := artifactGCStoreStagingSpec(t, fixture.store, fixture.now.Add(time.Nanosecond),
			2, artifactGCMaxSweepBytes, before)
		started, err := fixture.store.SweepArtifactGCStaging(context.Background(), startedSpec)
		if err != nil || started.Swept != 0 {
			t.Fatalf("started sweep = (%#v, %v)", started, err)
		}
		resultJSON, _ := model.NewJSON([]byte(`{"error":"invalid_action"}`))
		if _, err := fixture.store.RejectOperation(context.Background(), fixture.operation.ID(),
			fixture.operation.LeaseOwner(), fixture.now, resultJSON); err != nil {
			t.Fatal(err)
		}
		insideAt := fixture.now.Add(artifactGCStagingRetention - time.Nanosecond)
		insideSpec := artifactGCStoreStagingSpec(t, fixture.store, fixture.now.Add(time.Nanosecond),
			2, artifactGCMaxSweepBytes, insideAt)
		inside, err := fixture.store.SweepArtifactGCStaging(context.Background(), insideSpec)
		if err != nil || inside.Swept != 0 {
			t.Fatalf("inside rejected TTL = (%#v, %v)", inside, err)
		}
		boundaryAt := fixture.now.Add(artifactGCStagingRetention)
		boundarySpec := artifactGCStoreStagingSpec(t, fixture.store, fixture.now.Add(time.Nanosecond),
			2, artifactGCMaxSweepBytes, boundaryAt)
		boundary, err := fixture.store.SweepArtifactGCStaging(context.Background(), boundarySpec)
		if err != nil || boundary.Swept != 1 {
			t.Fatalf("rejected TTL boundary = (%#v, %v)", boundary, err)
		}
	})

	t.Run("committed capture without matching provenance is fatal", func(t *testing.T) {
		fixture := newOperationCaptureFixture(t, "gc-operation-corrupt", 1)
		if _, err := fixture.store.CheckpointOperationCapture(context.Background(), fixture.operation.ID(),
			fixture.operation.LeaseOwner(), fixture.now, fixture.capture); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`UPDATE operations SET status='committed',lease_owner=NULL,
			lease_until=NULL,result_json='{}',finished_at=? WHERE operation_id=?`, storeTime(fixture.now),
			fixture.operation.ID().String()); err != nil {
			t.Fatal(err)
		}
		cursor, err := fixture.store.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
			InitializeCutoff: fixture.now.Add(time.Hour), At: fixture.now.Add(2 * time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		unrelated := model.Sum([]byte("gc-unrelated-corrupt-candidate"))
		candidate := artifactdomain.GCCandidate{Digest: unrelated, SizeBytes: 1,
			ModifiedAt: fixture.now.Add(-time.Hour), Token: artifactGCStoreToken(77)}
		_, err = fixture.store.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
			Current: cursor, Candidates: []artifactdomain.GCCandidate{candidate}, PageDone: true,
			MaxQueued: 1, MaxQueue: 1, At: fixture.now.Add(2 * time.Hour),
		})
		if !errors.Is(err, artifactdomain.ErrGCStoreInvariant) ||
			!strings.Contains(err.Error(), "matching local provenance is missing") {
			t.Fatalf("committed corruption error = %v", err)
		}
		var queued int
		_ = fixture.store.db.QueryRow(`SELECT COUNT(*) FROM artifact_gc_queue`).Scan(&queued)
		if queued != 0 {
			t.Fatal("unrelated committed corruption allowed enqueue")
		}
		if _, err := fixture.store.db.Exec(`INSERT INTO artifact_blocks(block_digest,size_bytes,created_at)
			VALUES(?,1,?)`, unrelated.String(), storeTime(fixture.now.Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`INSERT INTO artifact_gc_queue(digest,token,state,size_bytes,
			modified_at,queued_at,renamed_at,updated_at) VALUES(?,?,'queued',1,?,?,NULL,?)`,
			unrelated.String(), candidate.Token[:], storeTime(candidate.ModifiedAt),
			storeTime(fixture.now.Add(2*time.Hour)), storeTime(fixture.now.Add(2*time.Hour))); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ListArtifactGCQueue(context.Background(), 2); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) {
			t.Fatalf("startup list ignored unrelated committed corruption: %v", err)
		}
		if _, err := fixture.store.MarkArtifactGCRenamed(context.Background(),
			artifactdomain.GCQueueTransitionSpec{Identity: artifactdomain.GCQueueIdentity{
				Digest: unrelated, Token: candidate.Token}, At: fixture.now.Add(2 * time.Hour)}); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) {
			t.Fatalf("mark ignored unrelated committed corruption: %v", err)
		}
		var blockPresent int
		_ = fixture.store.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM artifact_blocks WHERE block_digest=?)`,
			unrelated.String()).Scan(&blockPresent)
		if blockPresent != 1 {
			t.Fatal("failed-closed Mark deleted unrelated block metadata")
		}
	})
}

func TestArtifactGCQueueGuardsOwnerCreationAndTamper(t *testing.T) {
	st := openTestStore(t)
	now := artifactGCStoreTestTime()
	candidate := artifactGCStoreCandidates(now, 1)[0]
	cursor, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
		InitializeCutoff: now.Add(-time.Hour), At: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
		Current: cursor, Candidates: []artifactdomain.GCCandidate{candidate}, PageDone: false,
		MaxQueued: 1, MaxQueue: 1, At: now,
	}); err != nil {
		t.Fatal(err)
	}
	manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest,manifest_json,manifest_digest,
		total_bytes,state,created_at,verified_at) VALUES(?,?,?,0,'verified',?,?)`,
		model.Sum([]byte("queued-manifest-root")).String(), manifest.Bytes(), candidate.Digest.Bytes(),
		storeTime(now), storeTime(now)); err == nil || !strings.Contains(err.Error(), "GC queue owns manifest") {
		t.Fatalf("queued root creation error = %v", err)
	}
	tamperedToken := artifactGCStoreToken(88)
	if _, err := st.db.Exec(`UPDATE artifact_gc_queue SET token=? WHERE digest=?`,
		tamperedToken[:], candidate.Digest.String()); err == nil ||
		!strings.Contains(err.Error(), "queue identity is immutable") {
		t.Fatalf("queue identity tamper = %v", err)
	}
	if _, err := st.db.Exec(`DELETE FROM artifact_gc_queue WHERE digest=?`, candidate.Digest.String()); err == nil ||
		!strings.Contains(err.Error(), "completion authority is required") {
		t.Fatalf("queue delete tamper = %v", err)
	}
	if _, err := st.db.Exec(`UPDATE artifact_gc_scan SET "after"='' WHERE singleton=1`); err == nil ||
		!strings.Contains(err.Error(), "cursor cannot regress") {
		t.Fatalf("scan regression tamper = %v", err)
	}
	invalidTimeDigest := model.Sum([]byte("artifact-gc-invalid-calendar-time"))
	invalidTimeToken := artifactGCStoreUniqueToken("invalid-calendar-time")
	if _, err := st.db.Exec(`INSERT INTO artifact_gc_queue(digest,token,state,size_bytes,
		modified_at,queued_at,renamed_at,updated_at) VALUES(?,?,'queued',1,?,?,NULL,?)`,
		invalidTimeDigest.String(), invalidTimeToken[:], "2026-00-19T10:00:00.000000000Z",
		storeTime(now), storeTime(now)); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("invalid queue calendar time = %v", err)
	}
}

func TestArtifactGCCompletionReceiptsAreBoundedAndReplayExact(t *testing.T) {
	st := openTestStore(t)
	now := artifactGCStoreTestTime()
	type completion struct {
		identity artifactdomain.GCQueueIdentity
		at       time.Time
	}
	completed := make([]completion, artifactGCMaxCompletionReceipt+1)
	for index := range completed {
		digest, err := model.ParseDigest(fmt.Sprintf("sha256:%064x", len(completed)-index))
		if err != nil {
			t.Fatal(err)
		}
		token := artifactGCStoreUniqueToken(fmt.Sprintf("completion-bound-%d", index))
		at := now
		identity := artifactdomain.GCQueueIdentity{Digest: digest, Token: token}
		if _, err := st.db.Exec(`INSERT INTO artifact_gc_queue(digest,token,state,size_bytes,
			modified_at,queued_at,renamed_at,updated_at) VALUES(?,?,'renamed',0,?,?,?,?)`,
			digest.String(), token[:], storeTime(now.Add(-2*time.Hour)), storeTime(at),
			storeTime(at), storeTime(at)); err != nil {
			t.Fatalf("insert renamed queue %d: %v", index, err)
		}
		result, err := st.CompleteArtifactGC(context.Background(), artifactdomain.GCQueueTransitionSpec{
			Identity: identity, At: at,
		})
		if err != nil || !result.Completed || result.Replayed || result.At != at {
			t.Fatalf("complete %d = (%#v, %v)", index, result, err)
		}
		completed[index] = completion{identity: identity, at: at}
	}
	var receiptCount int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM artifact_gc_completion_receipts`).Scan(&receiptCount); err != nil ||
		receiptCount != artifactGCMaxCompletionReceipt {
		t.Fatalf("completion receipt count = (%d, %v), want %d", receiptCount, err,
			artifactGCMaxCompletionReceipt)
	}
	var firstSequence, lastSequence int64
	if err := st.db.QueryRow(`SELECT MIN(completion_seq),MAX(completion_seq)
		FROM artifact_gc_completion_receipts`).Scan(&firstSequence, &lastSequence); err != nil ||
		firstSequence != 2 || lastSequence != int64(len(completed)) {
		t.Fatalf("completion receipt commit sequence = (%d,%d,%v)",
			firstSequence, lastSequence, err)
	}
	recent := completed[len(completed)-1]
	replayed, err := st.CompleteArtifactGC(context.Background(), artifactdomain.GCQueueTransitionSpec{
		Identity: recent.identity, At: recent.at,
	})
	if err != nil || !replayed.Completed || !replayed.Replayed || replayed.At != recent.at {
		t.Fatalf("retained completion replay = (%#v, %v)", replayed, err)
	}
	oldest := completed[0]
	if _, err := st.CompleteArtifactGC(context.Background(), artifactdomain.GCQueueTransitionSpec{
		Identity: oldest.identity, At: oldest.at,
	}); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) ||
		!strings.Contains(err.Error(), "queue identity missing") {
		t.Fatalf("pruned completion replay = %v", err)
	}
}

func TestArtifactGCStagingCursorMakesProtectedPrefixAndLargeRootLive(t *testing.T) {
	t.Run("protected prefix restart and response replay", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
		manifestDigest := model.Sum(manifest.Bytes())
		roots := []model.Digest{model.Sum([]byte("gc-prefix-a")), model.Sum([]byte("gc-prefix-b")),
			model.Sum([]byte("gc-prefix-c"))}
		sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })
		for _, root := range roots {
			if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), VerifiedArtifactRoot{
				RootDigest: root, Manifest: manifest, ManifestDigest: manifestDigest,
				CreatedAt: now.Add(-2 * time.Hour), VerifiedAt: now.Add(-2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
		}
		for index, root := range roots[:2] {
			if _, err := st.db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,created_at)
				VALUES(?,'retention',?,?)`, root.String(), fmt.Sprintf("gc-prefix-pin-%d", index),
				storeTime(now.Add(-time.Hour))); err != nil {
				t.Fatal(err)
			}
		}
		spec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 2,
			artifactGCMaxSweepBytes, now)
		first, err := st.SweepArtifactGCStaging(context.Background(), spec)
		if err != nil || first.Examined != 2 || first.Swept != 0 {
			t.Fatalf("protected prefix sweep = (%#v, %v)", first, err)
		}
		replayed, err := st.SweepArtifactGCStaging(context.Background(), spec)
		if err != nil || replayed != first {
			t.Fatalf("staging response replay = (%#v, %v), want %#v", replayed, err, first)
		}
		var after string
		var done int
		if err := st.db.QueryRow(`SELECT "after",done FROM artifact_gc_staging_scan WHERE singleton=1`).
			Scan(&after, &done); err != nil || after != roots[1].String() || done != 0 {
			t.Fatalf("durable staging cursor = (%q,%d,%v)", after, done, err)
		}
		path := st.Path()
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		restarted, err := OpenExisting(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = restarted.Close() })
		restartReplay, err := restarted.SweepArtifactGCStaging(context.Background(), spec)
		if err != nil || restartReplay != first {
			t.Fatalf("restart staging response replay = (%#v, %v), want %#v", restartReplay, err, first)
		}
		secondSpec := artifactGCStoreStagingSpec(t, restarted, spec.Current.Cutoff,
			spec.MaxItems, spec.MaxBytes, now)
		second, err := restarted.SweepArtifactGCStaging(context.Background(), secondSpec)
		if err != nil || second.Examined != 1 || second.Swept != 1 {
			t.Fatalf("restart staging continuation = (%#v, %v)", second, err)
		}
		fresh, err := restarted.OpenArtifactGCStagingScan(context.Background(), artifactdomain.GCScanSpec{
			InitializeCutoff: spec.Current.Cutoff, At: now,
		})
		if err != nil || fresh.Generation != spec.Current.Generation+1 || fresh.Done || !fresh.After.IsZero() {
			t.Fatalf("constant-time fresh generation = (%#v, %v)", fresh, err)
		}
		var orphan int
		_ = restarted.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM artifact_roots WHERE root_digest=?)`,
			roots[2].String()).Scan(&orphan)
		if orphan != 0 {
			t.Fatal("protected prefix starved the later orphan")
		}
	})

	t.Run("root above physical queue default waits without cursor loss", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
		root := VerifiedArtifactRoot{RootDigest: model.Sum([]byte("gc-large-staging-root")),
			Manifest: manifest, ManifestDigest: model.Sum(manifest.Bytes()), TotalBytes: 17 << 20,
			CreatedAt: now.Add(-2 * time.Hour), VerifiedAt: now.Add(-2 * time.Hour)}
		if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		blockedSpec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 1, 16<<20, now)
		blocked, err := st.SweepArtifactGCStaging(context.Background(), blockedSpec)
		if err != nil || blocked.Examined != 0 || blocked.Swept != 0 {
			t.Fatalf("undersized staging budget = (%#v, %v)", blocked, err)
		}
		blockedReplay, err := st.SweepArtifactGCStaging(context.Background(), blockedSpec)
		if err != nil || blockedReplay != blocked {
			t.Fatalf("undersized staging replay = (%#v, %v), want %#v", blockedReplay, err, blocked)
		}
		var after string
		if err := st.db.QueryRow(`SELECT "after" FROM artifact_gc_staging_scan WHERE singleton=1`).Scan(&after); err != nil || after != "" {
			t.Fatalf("large root cursor advanced = (%q,%v)", after, err)
		}
		sweptSpec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 1,
			artifactGCMaxSweepBytes, now.Add(time.Nanosecond))
		swept, err := st.SweepArtifactGCStaging(context.Background(), sweptSpec)
		if err != nil || swept.Examined != 1 || swept.Swept != 1 || swept.SweptBytes != 17<<20 {
			t.Fatalf("large root sweep = (%#v, %v)", swept, err)
		}
	})

	t.Run("bounded expired pin cleanup cannot roll back or starve roots", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
		manifestDigest := model.Sum(manifest.Bytes())
		roots := []model.Digest{model.Sum([]byte("gc-expired-order-a")), model.Sum([]byte("gc-expired-order-b"))}
		sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })
		for _, root := range roots {
			if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), VerifiedArtifactRoot{
				RootDigest: root, Manifest: manifest, ManifestDigest: manifestDigest,
				CreatedAt: now.Add(-2 * time.Hour), VerifiedAt: now.Add(-2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.db.Exec(`DROP TRIGGER artifact_pins_inbox_owner_insert`); err != nil {
			t.Fatal(err)
		}
		// The later root has the earlier expiry, so the bounded global cleanup
		// removes its pin while the root scan first reaches the earlier root.
		for index, root := range roots {
			expires := now.Add(-time.Minute)
			if index == 1 {
				expires = now.Add(-2 * time.Minute)
			}
			if _, err := st.db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,
				expires_at,created_at) VALUES(?,'inbox',?,?,?)`, root.String(),
				fmt.Sprintf("gc-expired-order-%d", index), storeTime(expires),
				storeTime(now.Add(-2*time.Hour))); err != nil {
				t.Fatal(err)
			}
		}
		call := func(offset time.Duration) artifactdomain.GCStagingSweepResult {
			t.Helper()
			at := now.Add(offset)
			spec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 1,
				artifactGCMaxSweepBytes, at)
			result, err := st.SweepArtifactGCStaging(context.Background(), spec)
			if err != nil {
				t.Fatal(err)
			}
			return result
		}
		if first := call(0); first.Examined != 1 || first.Swept != 0 {
			t.Fatalf("residual expired pin first pass = %#v", first)
		}
		if second := call(time.Nanosecond); second.Examined != 1 || second.Swept != 1 {
			t.Fatalf("expired pin second pass = %#v", second)
		}
		if third := call(2 * time.Nanosecond); third.Examined != 1 || third.Swept != 1 {
			t.Fatalf("fresh pass did not recover earlier root = %#v", third)
		}
		var rootsLeft, pinsLeft int
		_ = st.db.QueryRow(`SELECT COUNT(*) FROM artifact_roots`).Scan(&rootsLeft)
		_ = st.db.QueryRow(`SELECT COUNT(*) FROM artifact_pins`).Scan(&pinsLeft)
		if rootsLeft != 0 || pinsLeft != 0 {
			t.Fatalf("expired cleanup leftovers roots=%d pins=%d", rootsLeft, pinsLeft)
		}
	})
}

func TestArtifactGCManifestRolesAndLaterCorruptOwner(t *testing.T) {
	t.Run("manifest-only metadata protects then becomes collectable", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
		manifestDigest := model.Sum(manifest.Bytes())
		root := VerifiedArtifactRoot{RootDigest: model.Sum([]byte("gc-manifest-only-root")),
			Manifest: manifest, ManifestDigest: manifestDigest, CreatedAt: now.Add(-2 * time.Hour),
			VerifiedAt: now.Add(-2 * time.Hour)}
		if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		cursor, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
			InitializeCutoff: now.Add(-time.Hour), At: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		candidate := artifactdomain.GCCandidate{Digest: manifestDigest, SizeBytes: uint64(len(manifest.Bytes())),
			ModifiedAt: now.Add(-2 * time.Hour), Token: artifactGCStoreToken(101)}
		protected, err := st.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
			Current: cursor, Candidates: []artifactdomain.GCCandidate{candidate}, PageDone: true,
			MaxQueued: 1, MaxQueue: 1, At: now,
		})
		if err != nil || protected.Protected != 1 || protected.Queued != 0 {
			t.Fatalf("manifest protection = (%#v, %v)", protected, err)
		}
		sweepSpec := artifactGCStoreStagingSpec(t, st, now.Add(-time.Hour), 1,
			artifactGCMaxSweepBytes, now.Add(time.Nanosecond))
		if _, err := st.SweepArtifactGCStaging(context.Background(), sweepSpec); err != nil {
			t.Fatal(err)
		}
		fresh, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
			InitializeCutoff: now.Add(-time.Hour), At: now.Add(2 * time.Nanosecond),
		})
		if err != nil {
			t.Fatal(err)
		}
		candidate.Token = artifactGCStoreToken(102)
		queued, err := st.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
			Current: fresh, Candidates: []artifactdomain.GCCandidate{candidate}, PageDone: true,
			MaxQueued: 1, MaxQueue: 1, At: now.Add(2 * time.Nanosecond),
		})
		if err != nil || queued.Queued != 1 || queued.Protected != 0 {
			t.Fatalf("orphan manifest prepare = (%#v, %v)", queued, err)
		}
	})

	t.Run("manifest block dual role audits every associated root", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		closureA, rootA, _ := newArtifactSourceClosure(t, "gc-dual-manifest", []byte("payload"), now.Add(-2*time.Hour))
		closureB, rootB, _ := newArtifactSourceClosure(t, "gc-dual-block", rootA.Manifest.Bytes(), now.Add(-2*time.Hour))
		for _, closure := range []VerifiedArtifactClosure{closureA, closureB} {
			if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
				t.Fatal(err)
			}
		}
		physical := rootA.ManifestDigest
		if closureB.Blocks[0].Digest != physical {
			t.Fatal("fixture did not create a manifest/block dual role")
		}
		early, corrupt := rootA, rootB
		if early.RootDigest.String() > corrupt.RootDigest.String() {
			early, corrupt = corrupt, early
		}
		if _, err := st.db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,created_at)
			VALUES(?,'retention','gc-dual-early',?)`, early.RootDigest.String(), storeTime(now)); err != nil {
			t.Fatal(err)
		}
		node, profile := bootstrapValues(t, "peer-gc-dual", "principal-gc-dual", "/workspace/gc-dual")
		_, _ = activateTestNode(t, st, node, profile)
		insertOperationAgentRun(t, st, profile, "run-gc-dual", "running", now)
		operation := startedOperation(t, "operation-gc-dual", "key-gc-dual", "request-gc-dual",
			"run-gc-dual", "owner-gc-dual", now, nil)
		if _, err := st.ReserveOperation(context.Background(), operation, now); err != nil {
			t.Fatal(err)
		}
		capture := operationCaptureJSON(t, []captureRoot{{RootDigest: corrupt.RootDigest,
			ManifestDigest: corrupt.ManifestDigest}})
		if _, err := st.CheckpointOperationCapture(context.Background(), operation.ID(), operation.LeaseOwner(),
			now, capture); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(`UPDATE operations SET status='committed',lease_owner=NULL,
			lease_until=NULL,result_json='{}',finished_at=? WHERE operation_id=?`, storeTime(now),
			operation.ID().String()); err != nil {
			t.Fatal(err)
		}
		cursor, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
			InitializeCutoff: now.Add(-time.Hour), At: now.Add(time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = st.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
			Current: cursor, Candidates: []artifactdomain.GCCandidate{{Digest: physical,
				SizeBytes: uint64(len(rootA.Manifest.Bytes())), ModifiedAt: now.Add(-2 * time.Hour),
				Token: artifactGCStoreToken(103)}}, PageDone: true, MaxQueued: 1, MaxQueue: 1,
			At: now.Add(time.Minute),
		})
		if !errors.Is(err, artifactdomain.ErrGCStoreInvariant) ||
			!strings.Contains(err.Error(), "matching local provenance is missing") {
			t.Fatalf("later corrupt dual-role owner error = %v", err)
		}
	})
}

func TestArtifactGCManifestOwnerRaceAndPinRevivalFence(t *testing.T) {
	for iteration := 0; iteration < 12; iteration++ {
		t.Run(fmt.Sprintf("manifest race %02d", iteration), func(t *testing.T) {
			st := openTestStore(t)
			now := artifactGCStoreTestTime()
			manifest, _ := model.NewJSON([]byte(fmt.Sprintf(`{"entries":[],"iteration":%d}`, iteration)))
			manifestDigest := model.Sum(manifest.Bytes())
			root := VerifiedArtifactRoot{RootDigest: model.Sum([]byte(fmt.Sprintf("gc-manifest-race-root-%d", iteration))),
				Manifest: manifest, ManifestDigest: manifestDigest, CreatedAt: now, VerifiedAt: now}
			cursor, err := st.OpenArtifactGCScan(context.Background(), artifactdomain.GCScanSpec{
				InitializeCutoff: now.Add(-time.Hour), At: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			candidate := artifactdomain.GCCandidate{Digest: manifestDigest, SizeBytes: uint64(len(manifest.Bytes())),
				ModifiedAt: now.Add(-2 * time.Hour), Token: artifactGCStoreToken(byte(120 + iteration))}
			start := make(chan struct{})
			var wait sync.WaitGroup
			wait.Add(2)
			var checkpointErr, prepareErr error
			var prepared artifactdomain.GCPrepareResult
			go func() {
				defer wait.Done()
				<-start
				_, checkpointErr = st.CheckpointVerifiedArtifactRoot(context.Background(), root)
			}()
			go func() {
				defer wait.Done()
				<-start
				prepared, prepareErr = st.PrepareArtifactGC(context.Background(), artifactdomain.GCPrepareSpec{
					Current: cursor, Candidates: []artifactdomain.GCCandidate{candidate}, PageDone: true,
					MaxQueued: 1, MaxQueue: 1, At: now,
				})
			}()
			close(start)
			wait.Wait()
			ownerWon := checkpointErr == nil && prepareErr == nil && prepared.Protected == 1 && prepared.Queued == 0
			queueWon := prepareErr == nil && prepared.Queued == 1 && prepared.Protected == 0 &&
				errors.Is(checkpointErr, artifactdomain.ErrGCStoreInvariant)
			if !ownerWon && !queueWon {
				t.Fatalf("manifest race double/zero win: checkpoint=%v prepare=(%#v,%v)",
					checkpointErr, prepared, prepareErr)
			}
		})
	}

	t.Run("queued closure rejects expiring pin revival", func(t *testing.T) {
		st := openTestStore(t)
		now := artifactGCStoreTestTime()
		closure, root, _ := newArtifactSourceClosure(t, "gc-pin-revival", []byte("pin"), now.Add(-2*time.Hour))
		if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(`DROP TRIGGER artifact_pins_inbox_owner_insert`); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,expires_at,created_at)
			VALUES(?,'inbox','gc-pin-revival',?,?)`, root.RootDigest.String(), storeTime(now.Add(-time.Minute)),
			storeTime(now.Add(-2*time.Hour))); err != nil {
			t.Fatal(err)
		}
		token := artifactGCStoreToken(201)
		queueSQL := `INSERT INTO artifact_gc_queue(digest,token,state,size_bytes,modified_at,
			queued_at,renamed_at,updated_at) VALUES(?,?,'queued',1,?,?,NULL,?)`
		if _, err := st.db.Exec(queueSQL, root.ManifestDigest.String(), token[:],
			storeTime(now.Add(-2*time.Hour)), storeTime(now), storeTime(now)); err == nil ||
			!strings.Contains(err.Error(), "still has durable ownership") {
			t.Fatalf("owner-first queue insert error = %v", err)
		}
		if _, err := st.db.Exec(`DROP TRIGGER artifact_gc_queue_owner_insert`); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(queueSQL,
			root.ManifestDigest.String(), token[:], storeTime(now.Add(-2*time.Hour)), storeTime(now), storeTime(now)); err != nil {
			t.Fatal(err)
		}
		for _, expiry := range []any{storeTime(now.Add(time.Hour)), nil} {
			if _, err := st.db.Exec(`UPDATE artifact_pins SET expires_at=? WHERE root_digest=?`,
				expiry, root.RootDigest.String()); err == nil || !strings.Contains(err.Error(), "GC queue owns pin closure") {
				t.Fatalf("pin revival %v error = %v", expiry, err)
			}
		}
		tx, err := st.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := requireArtifactGCQueueAvailableForRoot(context.Background(), tx, root.RootDigest); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) {
			_ = tx.Rollback()
			t.Fatalf("public pin precheck error = %v", err)
		}
		_ = tx.Rollback()
	})
}

func TestArtifactGCStaleTransientAuthorityFailsStartupReads(t *testing.T) {
	for _, test := range []struct {
		name   string
		insert func(*testing.T, *Store, time.Time)
	}{
		{name: "root guard", insert: func(t *testing.T, st *Store, at time.Time) {
			_, err := st.db.Exec(`INSERT INTO artifact_gc_delete_guard(root_digest,authorized_at) VALUES(?,?)`,
				model.Sum([]byte("stale-root-guard")).String(), storeTime(at))
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "block guard", insert: func(t *testing.T, st *Store, at time.Time) {
			_, err := st.db.Exec(`INSERT INTO artifact_gc_block_delete_guard(block_digest,authorized_at) VALUES(?,?)`,
				model.Sum([]byte("stale-block-guard")).String(), storeTime(at))
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "completion guard", insert: func(t *testing.T, st *Store, at time.Time) {
			token := artifactGCStoreToken(222)
			_, err := st.db.Exec(`INSERT INTO artifact_gc_completion_guard(digest,token,completed_at) VALUES(?,?,?)`,
				model.Sum([]byte("stale-completion-guard")).String(), token[:], storeTime(at))
			if err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			st := openTestStore(t)
			test.insert(t, st, artifactGCStoreTestTime())
			if _, err := st.ListArtifactGCQueue(context.Background(), 1); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) {
				t.Fatalf("live stale guard list error = %v", err)
			}
			path := st.Path()
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := OpenExisting(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			if _, err := restarted.ListArtifactGCQueue(context.Background(), 1); !errors.Is(err, artifactdomain.ErrGCStoreInvariant) {
				t.Fatalf("restart stale guard list error = %v", err)
			}
		})
	}
}

func artifactGCStoreCandidates(now time.Time, count int) []artifactdomain.GCCandidate {
	result := make([]artifactdomain.GCCandidate, 0, count)
	for index := 0; index < count; index++ {
		content := []byte(fmt.Sprintf("artifact-gc-store-candidate-%d", index))
		result = append(result, artifactdomain.GCCandidate{Digest: model.Sum(content),
			SizeBytes: uint64(len(content)), ModifiedAt: now.Add(-2 * time.Hour),
			Token: artifactGCStoreToken(byte(index + 1))})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Digest.String() < result[j].Digest.String() })
	return result
}

func artifactGCStoreStagingSpec(t *testing.T, st *Store, cutoff time.Time, maxItems int,
	maxBytes uint64, at time.Time,
) artifactdomain.GCStagingSweepSpec {
	t.Helper()
	cursor, err := st.OpenArtifactGCStagingScan(context.Background(), artifactdomain.GCScanSpec{
		InitializeCutoff: cutoff, At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifactdomain.GCStagingSweepSpec{Current: cursor, MaxItems: maxItems,
		MaxBytes: maxBytes, At: at}
}

func artifactGCStoreToken(seed byte) [32]byte {
	var token [32]byte
	for index := range token {
		token[index] = seed + byte(index)
	}
	return token
}

func artifactGCStoreUniqueToken(seed string) [32]byte {
	digest := model.Sum([]byte("artifact-gc-token:" + seed))
	var token [32]byte
	copy(token[:], digest.Bytes())
	return token
}

func artifactGCStoreTestTime() time.Time {
	return time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
}
