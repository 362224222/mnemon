package authority

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const (
	interactiveAttachmentLifetime = 15 * time.Minute
	attachmentCredentialBytes     = 32
	randomIdentifierBytes         = 16
)

// AttachmentProof is the exact short-lived capability returned by machine
// issuance. Callers may persist it only in an owner-private Runtime surface;
// the authority store retains only its digest.
type AttachmentProof struct {
	id         agency.AttachmentID
	credential [attachmentCredentialBytes]byte
	expiresAt  time.Time
}

// NewAttachmentProof reconstructs a transported capability at the daemon
// boundary. The caller supplies no Principal, mode, timestamps, or authority;
// exact credential verification resolves all of them from the private store.
func NewAttachmentProof(id agency.AttachmentID, credential []byte) (AttachmentProof, error) {
	if id.IsZero() || len(credential) != attachmentCredentialBytes {
		return AttachmentProof{}, errors.New("attachment proof: exact ID and credential are required")
	}
	proof := AttachmentProof{id: id}
	copy(proof.credential[:], credential)
	return proof, nil
}

func (proof AttachmentProof) ID() agency.AttachmentID { return proof.id }
func (proof AttachmentProof) ExpiresAt() time.Time    { return proof.expiresAt }
func (proof AttachmentProof) Credential() []byte {
	return append([]byte(nil), proof.credential[:]...)
}

// EnrollPrincipal is setup authority, not Agent authentication. It creates
// only the stable local Principal that later machine-issued attachments may
// authenticate as.
func (s *Store) EnrollPrincipal(ctx context.Context, principal agency.AgentPrincipalID) error {
	if ctx == nil || principal.IsZero() {
		return errors.New("enroll Principal: Principal is required")
	}
	now, err := s.trustedNow()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO principals(principal_id, created_at)
		VALUES(?, ?) ON CONFLICT(principal_id) DO NOTHING`, principal.String(), formatTime(now)); err != nil {
		return fmt.Errorf("enroll Principal: %w", err)
	}
	return nil
}

// IssueInteractiveAttachment creates a short-lived initiation-capable
// boundary. Random generation happens before the durable transaction; the
// transaction contains no external callback.
func (s *Store) IssueInteractiveAttachment(ctx context.Context,
	principal agency.AgentPrincipalID,
) (AttachmentProof, error) {
	if ctx == nil || principal.IsZero() {
		return AttachmentProof{}, errors.New("issue attachment: Principal is required")
	}
	now, err := s.trustedNow()
	if err != nil {
		return AttachmentProof{}, err
	}
	id, err := newAttachmentID()
	if err != nil {
		return AttachmentProof{}, err
	}
	var credential [attachmentCredentialBytes]byte
	if _, err := rand.Read(credential[:]); err != nil {
		return AttachmentProof{}, fmt.Errorf("issue attachment: random credential: %w", err)
	}
	expiresAt := now.Add(interactiveAttachmentLifetime)
	digest := agency.Sum(credential[:])

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return AttachmentProof{}, err
	}
	var exists int
	if err := s.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM principals WHERE principal_id = ?)", principal.String()).Scan(&exists); err != nil {
		return AttachmentProof{}, fmt.Errorf("issue attachment: inspect Principal: %w", err)
	}
	if exists != 1 {
		return AttachmentProof{}, ErrPrincipalUnavailable
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO attachments(
		attachment_id, principal_id, mode, credential_digest, issued_at, expires_at)
		VALUES(?, ?, 'interactive', ?, ?, ?)`, id.String(), principal.String(), digest.String(),
		formatTime(now), formatTime(expiresAt)); err != nil {
		return AttachmentProof{}, fmt.Errorf("issue attachment: persist: %w", err)
	}
	proof, err := NewAttachmentProof(id, credential[:])
	if err != nil {
		return AttachmentProof{}, err
	}
	proof.expiresAt = expiresAt
	return proof, nil
}

type authenticatedAttachment struct {
	value agency.Attachment
	mode  string
}

// authenticateAttachmentTx verifies a fixed-size proof in constant comparison
// time. Expiry is deliberately not checked here: replay lookup must precede
// mutable lifecycle checks.
func authenticateAttachmentTx(ctx context.Context, tx *sql.Tx,
	proof AttachmentProof,
) (authenticatedAttachment, error) {
	if proof.id.IsZero() {
		return authenticatedAttachment{}, ErrAttachmentAuth
	}
	var principalValue, mode, storedDigest, issuedValue, expiresValue string
	err := tx.QueryRowContext(ctx, `SELECT principal_id, mode, credential_digest, issued_at, expires_at
		FROM attachments WHERE attachment_id = ?`, proof.id.String()).
		Scan(&principalValue, &mode, &storedDigest, &issuedValue, &expiresValue)
	if errors.Is(err, sql.ErrNoRows) {
		return authenticatedAttachment{}, ErrAttachmentAuth
	}
	if err != nil {
		return authenticatedAttachment{}, fmt.Errorf("authenticate attachment: load: %w", err)
	}
	parsedStoredDigest, err := agency.ParseDigest(storedDigest)
	if err != nil {
		return authenticatedAttachment{}, errors.New("authenticate attachment: corrupt credential digest")
	}
	actualDigest := agency.Sum(proof.credential[:])
	if subtle.ConstantTimeCompare(actualDigest[:], parsedStoredDigest[:]) != 1 {
		return authenticatedAttachment{}, ErrAttachmentAuth
	}
	principal, err := agency.NewAgentPrincipalID(principalValue)
	if err != nil {
		return authenticatedAttachment{}, errors.New("authenticate attachment: corrupt Principal")
	}
	issuedAt, err := parseTime(issuedValue)
	if err != nil {
		return authenticatedAttachment{}, err
	}
	expiresAt, err := parseTime(expiresValue)
	if err != nil {
		return authenticatedAttachment{}, err
	}
	attachment, err := agency.NewAttachment(proof.id, principal, mode == "interactive", issuedAt, expiresAt)
	if err != nil {
		return authenticatedAttachment{}, fmt.Errorf("authenticate attachment: corrupt authority: %w", err)
	}
	return authenticatedAttachment{value: attachment, mode: mode}, nil
}

func requireLiveAttachment(attachment authenticatedAttachment, now time.Time) error {
	if !now.Before(attachment.value.ExpiresAt()) {
		return ErrAttachmentExpired
	}
	return nil
}

func newAttachmentID() (agency.AttachmentID, error) {
	token, err := randomIdentifier("attachment")
	if err != nil {
		return agency.AttachmentID{}, err
	}
	return agency.NewAttachmentID(token)
}

func newEventID() (agency.EventID, error) {
	token, err := randomIdentifier("event")
	if err != nil {
		return agency.EventID{}, err
	}
	return agency.NewEventID(token)
}

func newHandlingID() (agency.HandlingID, error) {
	token, err := randomIdentifier("handling")
	if err != nil {
		return agency.HandlingID{}, err
	}
	return agency.NewHandlingID(token)
}

func randomIdentifier(prefix string) (string, error) {
	var entropy [randomIdentifierBytes]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("authority: random %s ID: %w", prefix, err)
	}
	return prefix + ":" + base64.RawURLEncoding.EncodeToString(entropy[:]), nil
}
