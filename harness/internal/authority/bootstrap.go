package authority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// Bootstrap persists a stable Principal and one machine-issued Runtime
// attachment. It registers the digest of an opaque credential; it does not
// authenticate a caller or accept the credential itself. Repeating the exact
// registration is harmless, while reusing its ID with different authority
// fails closed.
func (s *Store) Bootstrap(ctx context.Context, principal agency.AgentPrincipalID,
	attachment agency.Attachment, credentialDigest agency.Digest,
) error {
	if ctx == nil || principal.IsZero() || attachment.ID().IsZero() ||
		attachment.Principal() != principal || credentialDigest.IsZero() {
		return errors.New("bootstrap authority: incomplete or mismatched identity")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("bootstrap authority: begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(principal_id, created_at)
		VALUES(?, ?) ON CONFLICT(principal_id) DO NOTHING`,
		principal.String(), formatTime(attachment.IssuedAt())); err != nil {
		return fmt.Errorf("bootstrap authority: insert Principal: %w", err)
	}

	mode := attachmentMode(attachment)
	var storedPrincipal, storedMode, storedCredential, issuedAt, expiresAt string
	err = tx.QueryRowContext(ctx, `SELECT principal_id, mode, credential_digest, issued_at, expires_at
		FROM attachments WHERE attachment_id = ?`, attachment.ID().String()).
		Scan(&storedPrincipal, &storedMode, &storedCredential, &issuedAt, &expiresAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO attachments(
			attachment_id, principal_id, mode, credential_digest, issued_at, expires_at)
			VALUES(?, ?, ?, ?, ?, ?)`, attachment.ID().String(), principal.String(),
			mode, credentialDigest.String(), formatTime(attachment.IssuedAt()),
			formatTime(attachment.ExpiresAt()))
		if err != nil {
			return fmt.Errorf("bootstrap authority: insert attachment: %w", err)
		}
	case err != nil:
		return fmt.Errorf("bootstrap authority: inspect attachment: %w", err)
	case storedPrincipal != principal.String() || storedMode != mode ||
		storedCredential != credentialDigest.String() ||
		issuedAt != formatTime(attachment.IssuedAt()) || expiresAt != formatTime(attachment.ExpiresAt()):
		return ErrBootstrapConflict
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bootstrap authority: commit: %w", err)
	}
	return nil
}

func attachmentMode(attachment agency.Attachment) string {
	if attachment.MayInitiate() {
		return "interactive"
	}
	return "managed"
}
