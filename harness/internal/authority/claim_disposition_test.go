package authority

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestFreshCurrentDurablySettlesExpiredClaimWithoutDomainEffect(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:expiry-settlement")
	root := rootRequest(t, fixture.current(t), "operation:expiry-root", "survive a lost turn")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	claimed := fixture.current(t)
	beforeView := claimed.AgentView().CanonicalJSON()
	var beforeHead string
	if err := fixture.store.db.QueryRow("SELECT head_event_id FROM handlings LIMIT 1").
		Scan(&beforeHead); err != nil {
		t.Fatal(err)
	}
	*fixture.now = fixture.proof.ExpiresAt().Add(time.Second)
	replacement, err := fixture.store.IssueInteractiveAttachment(fixture.ctx, fixture.principal,
		nextAttachmentBoundary(t))
	if err != nil {
		t.Fatal(err)
	}
	operation := mustCurrentOperation(t, "operation:expiry-reclaim")
	reclaimed, err := fixture.store.Current(fixture.ctx, replacement, operation)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(beforeView, reclaimed.AgentView().CanonicalJSON()) {
		t.Fatal("replacement turn unexpectedly reused the expired View")
	}
	var state string
	var outcome any
	var attachment, afterHead string
	var fence uint64
	if err := fixture.store.db.QueryRow(`SELECT state, outcome, claim_attachment_id, claim_fence,
		head_event_id FROM handlings LIMIT 1`).
		Scan(&state, &outcome, &attachment, &fence, &afterHead); err != nil {
		t.Fatal(err)
	}
	if state != "open" || outcome != nil || attachment != replacement.ID().String() || fence != 2 ||
		afterHead != beforeHead {
		t.Fatalf("reclaimed Handling = %s/%v attachment=%s fence=%d", state, outcome, attachment, fence)
	}
	if got := countRows(t, fixture.store, "claim_dispositions"); got != 1 {
		t.Fatalf("claim dispositions = %d, want 1", got)
	}
	if got := countRows(t, fixture.store, "events"); got != 1 {
		t.Fatalf("lease expiry created %d Events, want the root Event only", got)
	}
	first := reclaimed.AgentView().CanonicalJSON()
	replay, err := fixture.store.Current(fixture.ctx, replacement, operation)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, replay.AgentView().CanonicalJSON()) ||
		countRows(t, fixture.store, "claim_dispositions") != 1 {
		t.Fatal("Current replay changed the durable claim disposition")
	}
}

func TestClaimExpiryDispositionFaultRollsBackExactClaim(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:expiry-rollback")
	root := rootRequest(t, fixture.current(t), "operation:expiry-rollback-root", "remain exact")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	fixture.current(t)
	oldAttachment := fixture.proof.ID().String()
	*fixture.now = fixture.proof.ExpiresAt().Add(time.Second)
	if _, err := fixture.store.db.Exec(`CREATE TEMP TRIGGER fail_claim_disposition
		BEFORE INSERT ON claim_dispositions BEGIN SELECT RAISE(ABORT, 'injected disposition fault'); END`); err != nil {
		t.Fatal(err)
	}
	boundary := nextAttachmentBoundary(t)
	if _, err := fixture.store.IssueInteractiveAttachment(fixture.ctx, fixture.principal,
		boundary); err == nil {
		t.Fatal("faulted boundary replacement unexpectedly succeeded")
	}
	var attachment string
	var fence uint64
	if err := fixture.store.db.QueryRow(`SELECT claim_attachment_id, claim_fence
		FROM handlings LIMIT 1`).Scan(&attachment, &fence); err != nil {
		t.Fatal(err)
	}
	if attachment != oldAttachment || fence != 1 || countRows(t, fixture.store, "claim_dispositions") != 0 {
		t.Fatalf("fault retained partial expiry: attachment=%s fence=%d", attachment, fence)
	}
	if _, err := fixture.store.db.Exec("DROP TRIGGER fail_claim_disposition"); err != nil {
		t.Fatal(err)
	}
	replacement, err := fixture.store.IssueInteractiveAttachment(fixture.ctx, fixture.principal,
		boundary)
	if err != nil {
		t.Fatal(err)
	}
	operation := mustCurrentOperation(t, "operation:expiry-rollback-current")
	if _, err := fixture.store.Current(fixture.ctx, replacement, operation); err != nil {
		t.Fatalf("Current after rolled-back replacement retry: %v", err)
	}
	if countRows(t, fixture.store, "claim_dispositions") != 1 {
		t.Fatal("retry did not commit exactly one claim disposition")
	}
}

func TestCurrentDoesNotSettleAnotherPrincipalsExpiredClaim(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:expiry-local")
	root := rootRequest(t, fixture.current(t), "operation:expiry-local-root", "local work")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	other := mustPrincipal(t, "principal:expiry-other")
	if err := fixture.store.EnrollPrincipal(fixture.ctx, other); err != nil {
		t.Fatal(err)
	}
	otherProof, err := fixture.store.IssueInteractiveAttachment(fixture.ctx, other,
		nextAttachmentBoundary(t))
	if err != nil {
		t.Fatal(err)
	}
	var headEvent string
	if err := fixture.store.db.QueryRow("SELECT event_id FROM events LIMIT 1").Scan(&headEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec(`INSERT INTO handlings(
		handling_id, target_principal_id, head_event_id, state, claim_attachment_id,
		claim_fence, claim_until, created_sequence)
		VALUES('handling:expiry-other', ?, ?, 'open', ?, 1, ?, 2)`, other.String(), headEvent,
		otherProof.ID().String(), formatTime(otherProof.ExpiresAt())); err != nil {
		t.Fatal(err)
	}
	*fixture.now = otherProof.ExpiresAt().Add(time.Second)
	before := exactClaimSnapshot(t, fixture, "handling:expiry-other")
	replacement, err := fixture.store.IssueInteractiveAttachment(fixture.ctx, fixture.principal,
		nextAttachmentBoundary(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Current(fixture.ctx, replacement,
		mustCurrentOperation(t, "operation:expiry-local-current")); err != nil {
		t.Fatal(err)
	}
	if after := exactClaimSnapshot(t, fixture, "handling:expiry-other"); after != before {
		t.Fatalf("another Principal's expired claim changed:\nbefore %q\nafter  %q", before, after)
	}
	if got := countRows(t, fixture.store, "claim_dispositions"); got != 0 {
		t.Fatalf("another Principal produced %d local dispositions", got)
	}
}

func TestClaimExpiryMaintenanceIsBoundedAndLeavesExcessClaimsExact(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:expiry-bound")
	root := rootRequest(t, fixture.current(t), "operation:expiry-bound-root", "bounded cleanup")
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	}
	proofs := installExpiredClaimFixtures(t, fixture, MaxClaimExpirySettlementsPerCurrent+2)
	*fixture.now = proofs[0].ExpiresAt().Add(time.Second)
	ordered := expiredHandlingIDs(t, fixture)
	if len(ordered) != len(proofs) {
		t.Fatalf("expired claims before Current = %d, want %d", len(ordered), len(proofs))
	}
	excess := make(map[string]string)
	for _, handling := range ordered[MaxClaimExpirySettlementsPerCurrent:] {
		excess[handling] = exactClaimSnapshot(t, fixture, handling)
	}
	replacement, err := fixture.store.IssueInteractiveAttachment(fixture.ctx, fixture.principal,
		nextAttachmentBoundary(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Current(fixture.ctx, replacement,
		mustCurrentOperation(t, "operation:expiry-bound-current")); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, fixture.store, "claim_dispositions"); got != MaxClaimExpirySettlementsPerCurrent {
		t.Fatalf("claim dispositions = %d, want %d", got, MaxClaimExpirySettlementsPerCurrent)
	}
	settled := dispositionHandlingIDs(t, fixture)
	if !slices.Equal(settled, ordered[:MaxClaimExpirySettlementsPerCurrent]) {
		t.Fatalf("settled prefix = %v, want %v", settled,
			ordered[:MaxClaimExpirySettlementsPerCurrent])
	}
	assertExcessClaimsUnchanged(t, fixture, proofs, excess)
}

func installExpiredClaimFixtures(t *testing.T, fixture *authorityFixture,
	count int,
) []AttachmentProof {
	t.Helper()
	var headEvent, rootHandling string
	if err := fixture.store.db.QueryRow("SELECT event_id FROM events LIMIT 1").Scan(&headEvent); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow("SELECT handling_id FROM handlings LIMIT 1").
		Scan(&rootHandling); err != nil {
		t.Fatal(err)
	}
	proofs := make([]AttachmentProof, count)
	for index := range proofs {
		var err error
		proofs[index], err = fixture.store.IssueInteractiveAttachment(fixture.ctx, fixture.principal,
			nextAttachmentBoundary(t))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.store.EndInteractiveAttachment(fixture.ctx, proofs[len(proofs)-1]); err != nil {
		t.Fatal(err)
	}
	for index, proof := range proofs {
		if index == 0 {
			if _, err := fixture.store.db.Exec(`UPDATE handlings SET claim_attachment_id = ?,
				claim_fence = 1, claim_until = ? WHERE handling_id = ?`, proof.ID().String(),
				formatTime(proof.ExpiresAt()), rootHandling); err != nil {
				t.Fatal(err)
			}
			continue
		}
		handling := fmt.Sprintf("handling:expiry-bound-%02d", index)
		if _, err := fixture.store.db.Exec(`INSERT INTO handlings(
			handling_id, target_principal_id, head_event_id, state, claim_attachment_id,
			claim_fence, claim_until, created_sequence)
			VALUES(?, ?, ?, 'open', ?, 1, ?, ?)`, handling, fixture.principal.String(),
			headEvent, proof.ID().String(), formatTime(proof.ExpiresAt()), index+2); err != nil {
			t.Fatal(err)
		}
	}
	return proofs
}

func assertExcessClaimsUnchanged(t *testing.T, fixture *authorityFixture,
	proofs []AttachmentProof, excess map[string]string,
) {
	t.Helper()
	var stillExpired int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM handlings
		WHERE claim_until IS NOT NULL AND claim_until <= ?`, formatTime(*fixture.now)).
		Scan(&stillExpired); err != nil {
		t.Fatal(err)
	}
	if want := len(proofs) - MaxClaimExpirySettlementsPerCurrent; stillExpired != want {
		t.Fatalf("unsettled expired claims = %d, want %d", stillExpired, want)
	}
	for handling, before := range excess {
		if after := exactClaimSnapshot(t, fixture, handling); after != before {
			t.Fatalf("excess claim %s changed:\nbefore %q\nafter  %q", handling, before, after)
		}
	}
}

func expiredHandlingIDs(t *testing.T, fixture *authorityFixture) []string {
	t.Helper()
	rows, err := fixture.store.db.Query(`SELECT handling_id FROM handlings
		WHERE claim_until IS NOT NULL AND claim_until <= ? ORDER BY claim_until, handling_id`,
		formatTime(*fixture.now))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func dispositionHandlingIDs(t *testing.T, fixture *authorityFixture) []string {
	t.Helper()
	rows, err := fixture.store.db.Query(`SELECT handling_id FROM claim_dispositions
		ORDER BY claim_until, handling_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func exactClaimSnapshot(t *testing.T, fixture *authorityFixture, handling string) string {
	t.Helper()
	var attachment, until, head, state string
	var fence uint64
	var outcome any
	if err := fixture.store.db.QueryRow(`SELECT claim_attachment_id, claim_fence, claim_until,
		head_event_id, state, outcome FROM handlings WHERE handling_id = ?`, handling).
		Scan(&attachment, &fence, &until, &head, &state, &outcome); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s\x00%v", attachment, fence, until,
		head, state, outcome)
}
