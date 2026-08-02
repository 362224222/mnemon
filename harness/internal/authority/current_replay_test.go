package authority

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestCurrentOperationReplaysFrozenViewAfterRestartAndExpiry(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:current-replay")
	root := rootRequest(t, fixture.current(t), "operation:current-replay-root", "durable work")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}

	currentOperation := mustCurrentOperation(t, "operation:current-response-loss")
	first, err := fixture.store.Current(fixture.ctx, fixture.proof, currentOperation)
	if err != nil {
		t.Fatal(err)
	}
	firstPublic := first.public.CanonicalJSON()
	firstAuthority := first.authority.CanonicalJSON()
	advance := subjectRequest(t, first, "operation:current-replay-advance",
		agency.ConsequenceAdvanceHandling, "state changed after the response was lost", nil)
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, advance); err != nil {
		t.Fatal(err)
	}

	*fixture.now = fixture.proof.ExpiresAt().Add(time.Second)
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := open(fixture.ctx, fixture.path, func() time.Time { return *fixture.now })
	if err != nil {
		t.Fatal(err)
	}
	reopened.artifactVerifier = fixture.verifier
	fixture.store = reopened

	replayed, err := reopened.Current(fixture.ctx, fixture.proof, currentOperation)
	if err != nil {
		t.Fatalf("Current replay after restart and expiry: %v", err)
	}
	if !bytes.Equal(replayed.public.CanonicalJSON(), firstPublic) ||
		!bytes.Equal(replayed.authority.CanonicalJSON(), firstAuthority) {
		t.Fatal("Current replay did not return the frozen byte-identical View")
	}
	var claimAttachment any
	var fence uint64
	if err := reopened.db.QueryRow(`SELECT claim_attachment_id, claim_fence FROM handlings LIMIT 1`).
		Scan(&claimAttachment, &fence); err != nil {
		t.Fatal(err)
	}
	if claimAttachment != nil || fence != 1 {
		t.Fatalf("Current replay changed claim occupancy: attachment=%v fence=%d", claimAttachment, fence)
	}
	if got := countCurrentOperations(t, reopened, currentOperation); got != 1 {
		t.Fatalf("Current operation rows = %d, want 1", got)
	}
}

func TestCurrentOperationAuthenticatesBeforeReplayAndRejectsDigestConflict(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:current-auth-replay")
	operation := mustCurrentOperation(t, "operation:current-auth-replay")
	if _, err := fixture.store.Current(fixture.ctx, fixture.proof, operation); err != nil {
		t.Fatal(err)
	}

	wrong := fixture.proof
	wrong.credential[0] ^= 0xff
	if _, err := fixture.store.Current(fixture.ctx, wrong, operation); !errors.Is(err, ErrAttachmentAuth) {
		t.Fatalf("wrong credential replay = %v, want ErrAttachmentAuth", err)
	}
	conflict := operation
	conflict.requestDigest = agency.Sum([]byte("different-current-request"))
	if _, err := fixture.store.Current(fixture.ctx, fixture.proof, conflict); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("Current digest conflict = %v, want ErrOperationConflict", err)
	}
}

func TestCurrentOperationAndClaimRollbackTogether(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:current-rollback")
	root := rootRequest(t, fixture.current(t), "operation:current-rollback-root", "claim me")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`CREATE TEMP TRIGGER fail_current_operation
		BEFORE INSERT ON current_operations BEGIN SELECT RAISE(ABORT, 'injected current fault'); END`); err != nil {
		t.Fatal(err)
	}
	operation := mustCurrentOperation(t, "operation:current-rollback-claim")
	if _, err := fixture.store.Current(fixture.ctx, fixture.proof, operation); err == nil {
		t.Fatal("faulted Current unexpectedly succeeded")
	}
	var claimAttachment any
	var fence uint64
	if err := fixture.store.db.QueryRow(`SELECT claim_attachment_id, claim_fence FROM handlings LIMIT 1`).
		Scan(&claimAttachment, &fence); err != nil {
		t.Fatal(err)
	}
	if claimAttachment != nil || fence != 0 {
		t.Fatalf("faulted Current retained claim: attachment=%v fence=%d", claimAttachment, fence)
	}
	if got := countCurrentOperations(t, fixture.store, operation); got != 0 {
		t.Fatalf("faulted Current retained %d operation rows", got)
	}
	if _, err := fixture.store.db.Exec("DROP TRIGGER fail_current_operation"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Current(fixture.ctx, fixture.proof, operation); err != nil {
		t.Fatalf("Current retry after rolled-back fault: %v", err)
	}
}

func TestConcurrentCurrentReplayCreatesOneClaimAndOneOperation(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:current-concurrent")
	root := rootRequest(t, fixture.current(t), "operation:current-concurrent-root", "claim once")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	operation := mustCurrentOperation(t, "operation:current-concurrent-claim")
	type outcome struct {
		view BoundView
		err  error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			<-start
			view, err := fixture.store.Current(fixture.ctx, fixture.proof, operation)
			results <- outcome{view: view, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	var views [][]byte
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		views = append(views, result.view.public.CanonicalJSON())
	}
	if len(views) != 2 || !bytes.Equal(views[0], views[1]) {
		t.Fatal("concurrent Current retry did not replay one exact View")
	}
	if got := countCurrentOperations(t, fixture.store, operation); got != 1 {
		t.Fatalf("concurrent Current operation rows = %d, want 1", got)
	}
	var fence uint64
	if err := fixture.store.db.QueryRow("SELECT claim_fence FROM handlings LIMIT 1").Scan(&fence); err != nil {
		t.Fatal(err)
	}
	if fence != 1 {
		t.Fatalf("concurrent Current claim fence = %d, want 1", fence)
	}
}

func TestCurrentReplayRejectsCorruptStoredProjection(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:current-corruption")
	operation := mustCurrentOperation(t, "operation:current-corruption")
	if _, err := fixture.store.Current(fixture.ctx, fixture.proof, operation); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE current_operations SET agent_view_json = ?
		WHERE attachment_id = ? AND operation_key = ?`, []byte(`{"schema":"corrupt"}`),
		fixture.proof.ID().String(), operation.key.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Current(fixture.ctx, fixture.proof, operation); err == nil ||
		!strings.Contains(err.Error(), "corrupt Agent projection") {
		t.Fatalf("corrupt Current replay = %v", err)
	}
	if got := countCurrentOperations(t, fixture.store, operation); got != 1 {
		t.Fatalf("corrupt replay created %d Current rows, want 1", got)
	}
}

func TestCurrentRejectsEventAuthorityColumnDivergence(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:event-column-corruption")
	root := rootRequest(t, fixture.current(t), "operation:event-column-root", "durable work")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec("UPDATE events SET request_digest = ?",
		agency.Sum([]byte("different request")).String()); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.store.Current(fixture.ctx, fixture.proof,
		mustCurrentOperation(t, "operation:event-column-current"))
	if err == nil || !strings.Contains(err.Error(), "authority columns diverge") {
		t.Fatalf("Current with divergent Event columns = %v", err)
	}
}

func TestCurrentRejectsEventArtifactPinDivergence(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:event-pin-corruption")
	root := rootRequest(t, fixture.current(t), "operation:event-pin-root", "durable work")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	digest := fixture.catalog(t, "unrelated Artifact pin")
	var eventID string
	if err := fixture.store.db.QueryRow("SELECT event_id FROM events LIMIT 1").Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`INSERT INTO event_artifacts(event_id, artifact_digest)
		VALUES(?, ?)`, eventID, digest.String()); err != nil {
		t.Fatal(err)
	}
	_, err := fixture.store.Current(fixture.ctx, fixture.proof,
		mustCurrentOperation(t, "operation:event-pin-current"))
	if err == nil || !strings.Contains(err.Error(), "Artifact pins diverge") {
		t.Fatalf("Current with divergent Event pins = %v", err)
	}
}

func TestCurrentViewHandleBindsAttachmentOperationAndAuthority(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:current-handle")
	tx, err := fixture.store.db.BeginTx(fixture.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	authenticated, err := authenticateAttachmentTx(fixture.ctx, tx, fixture.proof)
	if err != nil {
		t.Fatal(err)
	}
	first, err := projectBoundViewTx(fixture.ctx, tx, authenticated.value, nil,
		mustCurrentOperation(t, "operation:current-handle-one").key)
	if err != nil {
		t.Fatal(err)
	}
	second, err := projectBoundViewTx(fixture.ctx, tx, authenticated.value, nil,
		mustCurrentOperation(t, "operation:current-handle-two").key)
	if err != nil {
		t.Fatal(err)
	}
	if first.authority.Digest() != second.authority.Digest() {
		t.Fatal("identical world state unexpectedly changed authority digest")
	}
	if first.public.Handle() == second.public.Handle() {
		t.Fatal("different Current operations reused one public View handle")
	}
}

func TestManagedCurrentProjectionCannotInitiateRoot(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:managed-projection")
	attachmentID, err := agency.NewAttachmentID("attachment:managed-projection")
	if err != nil {
		t.Fatal(err)
	}
	managed, err := agency.NewAttachment(attachmentID, fixture.principal, false,
		*fixture.now, fixture.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(fixture.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	view, err := projectBoundViewTx(fixture.ctx, tx, managed, nil,
		mustCurrentOperation(t, "operation:managed-projection").key)
	if err != nil {
		t.Fatal(err)
	}
	public := string(view.public.CanonicalJSON())
	for _, forbidden := range []string{"handling.create", "handling.advance", "handling.completed",
		"handling.declined", "handling.unresolved"} {
		if strings.Contains(public, forbidden) {
			t.Fatalf("managed no-subject View exposed %q: %s", forbidden, public)
		}
	}
	if strings.Contains(public, `"targets"`) {
		t.Fatalf("managed no-subject View exposed an unusable target: %s", public)
	}
}

func mustCurrentOperation(t *testing.T, value string) CurrentOperation {
	t.Helper()
	operation, err := NewCurrentOperation(mustOperation(t, value))
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func countCurrentOperations(t *testing.T, store *Store, operation CurrentOperation) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM current_operations WHERE operation_key = ?`,
		operation.key.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
