package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestBootstrapStoresAttachmentModeAndCredentialDigest(t *testing.T) {
	store, err := Open(context.Background(), testDatabasePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	principal, err := agency.NewAgentPrincipalID("principal:bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	attachmentID, err := agency.NewAttachmentID("attachment:bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Date(2026, 8, 3, 1, 2, 3, 4, time.UTC)
	attachment, err := agency.NewAttachment(attachmentID, principal, true, issuedAt, issuedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	credentialDigest := agency.Sum([]byte("opaque-runtime-credential"))
	if err := store.Bootstrap(context.Background(), principal, attachment, credentialDigest); err != nil {
		t.Fatal(err)
	}
	if err := store.Bootstrap(context.Background(), principal, attachment, credentialDigest); err != nil {
		t.Fatalf("exact Bootstrap replay: %v", err)
	}
	var mode, storedCredential string
	if err := store.db.QueryRow(`SELECT mode, credential_digest FROM attachments
		WHERE attachment_id = ?`, attachmentID.String()).Scan(&mode, &storedCredential); err != nil {
		t.Fatal(err)
	}
	if mode != "interactive" || storedCredential != credentialDigest.String() {
		t.Fatalf("attachment authority = mode:%q credential:%q", mode, storedCredential)
	}

	otherCredential := agency.Sum([]byte("different-runtime-credential"))
	if err := store.Bootstrap(context.Background(), principal, attachment, otherCredential); !errors.Is(err, ErrBootstrapConflict) {
		t.Fatalf("Bootstrap(conflicting credential) = %v, want ErrBootstrapConflict", err)
	}
}

func TestBootstrapStoresManagedModeAndRejectsZeroCredentialDigest(t *testing.T) {
	store, err := Open(context.Background(), testDatabasePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	principal, err := agency.NewAgentPrincipalID("principal:managed")
	if err != nil {
		t.Fatal(err)
	}
	attachmentID, err := agency.NewAttachmentID("attachment:managed")
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Date(2026, 8, 3, 2, 3, 4, 5, time.UTC)
	attachment, err := agency.NewAttachment(attachmentID, principal, false, issuedAt, issuedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Bootstrap(context.Background(), principal, attachment, agency.Digest{}); err == nil {
		t.Fatal("Bootstrap(zero credential digest) succeeded")
	}
	digest := agency.Sum([]byte("managed-credential"))
	if err := store.Bootstrap(context.Background(), principal, attachment, digest); err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := store.db.QueryRow("SELECT mode FROM attachments WHERE attachment_id = ?",
		attachmentID.String()).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "managed" {
		t.Fatalf("mode = %q, want managed", mode)
	}
}

func TestAttachmentSchemaRejectsMalformedCredentialDigest(t *testing.T) {
	store, err := Open(context.Background(), testDatabasePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.db.Exec(`INSERT INTO principals(principal_id, created_at) VALUES(?, ?)`,
		"principal:direct", "2026-08-03T00:00:00.000000000Z"); err != nil {
		t.Fatal(err)
	}
	_, err = store.db.Exec(`INSERT INTO attachments(
		attachment_id, principal_id, mode, credential_digest, issued_at, expires_at)
		VALUES(?, ?, ?, ?, ?, ?)`, "attachment:direct", "principal:direct", "interactive",
		"sha256:not-a-digest", "2026-08-03T00:00:00.000000000Z", "2026-08-03T01:00:00.000000000Z")
	if err == nil {
		t.Fatal("malformed credential digest satisfied attachment schema")
	}
}
