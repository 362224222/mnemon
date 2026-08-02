package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestEnrollmentAndMachineIssuedAttachmentStaySeparate(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 3, 1, 2, 3, 4, time.UTC)
	store, err := open(ctx, testDatabasePath(t), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	principal := mustPrincipal(t, "principal:attachment")
	if _, err := store.IssueInteractiveAttachment(ctx, principal); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("Issue before enrollment = %v, want ErrPrincipalUnavailable", err)
	}
	if err := store.EnrollPrincipal(ctx, principal); err != nil {
		t.Fatal(err)
	}
	if err := store.EnrollPrincipal(ctx, principal); err != nil {
		t.Fatalf("repeat enrollment: %v", err)
	}
	proof, err := store.IssueInteractiveAttachment(ctx, principal)
	if err != nil {
		t.Fatal(err)
	}
	if proof.ID().IsZero() || len(proof.Credential()) != attachmentCredentialBytes ||
		proof.ExpiresAt() != now.Add(interactiveAttachmentLifetime) {
		t.Fatalf("invalid issued proof: id=%q bytes=%d expiry=%s", proof.ID().String(),
			len(proof.Credential()), proof.ExpiresAt())
	}
	var mode, credentialDigest string
	if err := store.db.QueryRow(`SELECT mode, credential_digest FROM attachments
		WHERE attachment_id = ?`, proof.ID().String()).Scan(&mode, &credentialDigest); err != nil {
		t.Fatal(err)
	}
	if mode != "interactive" || credentialDigest == string(proof.Credential()) {
		t.Fatalf("stored attachment = mode:%q credential:%q", mode, credentialDigest)
	}
	if credentialDigest != agency.Sum(proof.Credential()).String() {
		t.Fatalf("stored digest = %q, want digest of proof", credentialDigest)
	}
	reconstructed, err := NewAttachmentProof(proof.ID(), proof.Credential())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := authenticateAttachmentTx(ctx, tx, reconstructed); err != nil {
		t.Fatalf("transported proof did not authenticate: %v", err)
	}
	if _, err := NewAttachmentProof(proof.ID(), proof.Credential()[:8]); err == nil {
		t.Fatal("short transported credential constructed a proof")
	}
}

func TestAttachmentCredentialDoesNotAuthenticateByIDAlone(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 3, 2, 3, 4, 5, time.UTC)
	store, err := open(ctx, testDatabasePath(t), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	principal := mustPrincipal(t, "principal:auth")
	if err := store.EnrollPrincipal(ctx, principal); err != nil {
		t.Fatal(err)
	}
	proof, err := store.IssueInteractiveAttachment(ctx, principal)
	if err != nil {
		t.Fatal(err)
	}
	wrong := proof
	wrong.credential[0] ^= 0xff
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := authenticateAttachmentTx(ctx, tx, wrong); !errors.Is(err, ErrAttachmentAuth) {
		t.Fatalf("wrong credential = %v, want ErrAttachmentAuth", err)
	}
	authenticated, err := authenticateAttachmentTx(ctx, tx, proof)
	if err != nil {
		t.Fatal(err)
	}
	if authenticated.value.Principal() != principal || !authenticated.value.MayInitiate() {
		t.Fatalf("authenticated attachment = %#v", authenticated.value)
	}
}

func TestAttachmentSchemaDoesNotReserveManagedWakeMode(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:interactive-only")
	credential := agency.Sum([]byte("managed mode must remain outside R7 T0"))
	_, err := fixture.store.db.Exec(`INSERT INTO attachments(
		attachment_id, principal_id, mode, credential_digest, issued_at, expires_at)
		VALUES('attachment:managed-forbidden', ?, 'managed', ?, ?, ?)`,
		fixture.principal.String(), credential.String(), formatTime(*fixture.now),
		formatTime(fixture.now.Add(time.Minute)))
	if err == nil {
		t.Fatal("R7 T0 schema accepted a managed-wake attachment")
	}
}

func mustPrincipal(t *testing.T, value string) agency.AgentPrincipalID {
	t.Helper()
	principal, err := agency.NewAgentPrincipalID(value)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}
