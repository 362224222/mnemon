package store

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrAgentAttachmentInput     = errors.New("invalid Agent Run attachment input")
	ErrAgentAttachmentStale     = errors.New("Agent Run attachment is stale")
	ErrAgentAttachmentInvariant = errors.New("Agent Run attachment durable invariant violated")
)

// AgentWakePreclaimSpec binds one already-fsynced attachment capability to a
// Store-selected Handling and server-owned AgentRun. The attachment token
// becomes the claim context only after current consumes it; using one digest
// preserves that one-way capability transition without persisting raw bytes.
type AgentWakePreclaimSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	ClaimOwner            string
	AttachmentTokenHash   model.Digest
	At                    time.Time
	LeaseUntil            time.Time
}

type AgentAttachmentSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	AttachmentTokenHash   model.Digest
	At                    time.Time
}

type AgentAttachmentCleanupSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	At                    time.Time
	Candidates            []AgentRunAttachmentCandidate
}

type AgentRunAttachmentCandidate struct {
	RunID     model.RunID
	TokenHash model.Digest
}

type ReapableAgentRunAttachment struct {
	RunID     model.RunID
	TokenHash model.Digest
}

const maxAgentAttachmentCleanupCandidates = 65

// PreclaimAgentWake is the durable middle of the attachment staging protocol:
// the caller stages/fsyncs a token first, calls this transaction, then publishes
// the file under the returned Run ID before launching a Runtime.
func (s *Store) PreclaimAgentWake(ctx context.Context,
	spec AgentWakePreclaimSpec,
) (AgentClaimResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return AgentClaimResult{}, fmt.Errorf("%w: nil store or context", ErrAgentAttachmentInput)
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: trusted time: %v", ErrAgentAttachmentInput, err)
	}
	leaseUntil, err := canonicalClaimTime(spec.LeaseUntil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: lease: %v", ErrAgentAttachmentInput, err)
	}
	if spec.AttachmentTokenHash.IsZero() {
		return AgentClaimResult{}, fmt.Errorf("%w: zero attachment token hash", ErrAgentAttachmentInput)
	}
	if _, err := model.ParseRunID(spec.ClaimOwner); err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: claim owner: %v", ErrAgentAttachmentInput, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: begin: %w", err)
	}
	defer tx.Rollback()
	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return AgentClaimResult{}, err
	}
	expectedLease, err := canonicalStoreTime(at.Add(time.Duration(budget.Spec().ClaimLeaseSeconds) * time.Second))
	if err != nil || !leaseUntil.Equal(expectedLease) {
		return AgentClaimResult{}, fmt.Errorf("%w: lease must equal trusted time plus Profile budget",
			ErrAgentAttachmentInput)
	}
	busy, err := recoverExpiredAgentClaim(ctx, tx, profile, budget, at)
	if err != nil {
		return AgentClaimResult{}, err
	}
	if busy {
		return AgentClaimResult{Status: AgentClaimBusy}, nil
	}
	if err := deadExhaustedPendingHandlings(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at); err != nil {
		return AgentClaimResult{}, err
	}
	handling, err := selectReadyAgentHandling(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at)
	if errors.Is(err, sql.ErrNoRows) {
		status, probeErr := probeAgentClaimStatus(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at)
		if probeErr != nil {
			return AgentClaimResult{}, probeErr
		}
		if status == AgentClaimActionable || status == AgentClaimBusy {
			return AgentClaimResult{}, fmt.Errorf("%w: readiness changed inside serialized preclaim",
				ErrAgentAttachmentInvariant)
		}
		if err := tx.Commit(); err != nil {
			return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: commit recovery: %w", err)
		}
		return AgentClaimResult{Status: status}, nil
	}
	if err != nil {
		return AgentClaimResult{}, err
	}
	if handling.Attempts() >= uint32(budget.Spec().MaxAttempts) || at.Before(handling.UpdatedAt()) {
		return AgentClaimResult{}, fmt.Errorf("%w: selected Handling cannot be preclaimed",
			ErrAgentAttachmentInvariant)
	}
	runID, err := newServerAgentRunID()
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: allocate Run ID: %w", err)
	}
	nextAttempt := handling.Attempts() + 1
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET
		status='claimed',claim_owner=?,claim_token_hash=?,lease_until=?,attempts=?,
		last_disposition='claimed',last_error=NULL,dead_at=NULL,updated_at=?
		WHERE handling_id=? AND profile_id=? AND status='pending' AND attempts=? AND available_at<=?`,
		spec.ClaimOwner, spec.AttachmentTokenHash.Bytes(), storeTime(leaseUntil), nextAttempt,
		storeTime(at), handling.ID().String(), profile.ID().String(), handling.Attempts(), storeTime(at))
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: claim Handling: %w", err)
	}
	if err := requireExactlyOneRow(result, "preclaim Agent wake: Handling CAS"); err != nil {
		return AgentClaimResult{}, err
	}
	claimed, err := readAgentHandling(ctx, tx, handling.ID())
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: read claimed Handling: %w", err)
	}
	run, err := newWakeAgentRun(runID, profile, claimed, spec.AttachmentTokenHash, at, leaseUntil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: build AgentRun: %w", err)
	}
	if err := insertAgentRun(ctx, tx, run); err != nil {
		return AgentClaimResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentClaimResult{}, fmt.Errorf("preclaim Agent wake: commit: %w", err)
	}
	return AgentClaimResult{Status: AgentClaimActionable, Handling: claimed, Run: run}, nil
}

// PeekAgentRunAttachment validates only the preclaimed Run named by the
// capability. It never falls back to general pending work and never consumes
// the attachment.
func (s *Store) PeekAgentRunAttachment(ctx context.Context, spec AgentAttachmentSpec) error {
	_, err := s.withAgentRunAttachment(ctx, spec, false)
	return err
}

// ConsumeAgentRunAttachment atomically writes attached_at once and returns
// the already-owned claim. It does not select another Handling or create a new
// Run when the capability is stale.
func (s *Store) ConsumeAgentRunAttachment(ctx context.Context,
	spec AgentAttachmentSpec,
) (AgentClaimResult, error) {
	return s.withAgentRunAttachment(ctx, spec, true)
}

// ListReapableAgentRunAttachments settles an expired claim first, then
// authorizes only the bounded set of files observed by the filesystem layer.
// Deleted historical DB rows therefore cannot starve later real files. Active
// consumed Runs are excluded so current can finish its exact inode removal.
func (s *Store) ListReapableAgentRunAttachments(ctx context.Context,
	spec AgentAttachmentCleanupSpec,
) ([]ReapableAgentRunAttachment, error) {
	if s == nil || s.db == nil || ctx == nil {
		return nil, ErrAgentAttachmentInput
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return nil, fmt.Errorf("%w: cleanup time: %v", ErrAgentAttachmentInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list reapable Agent Run attachments: begin: %w", err)
	}
	defer tx.Rollback()
	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return nil, err
	}
	if _, err := recoverExpiredAgentClaim(ctx, tx, profile, budget, at); err != nil {
		return nil, err
	}
	if err := deadExhaustedPendingHandlings(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at); err != nil {
		return nil, err
	}
	if len(spec.Candidates) > maxAgentAttachmentCleanupCandidates {
		return nil, fmt.Errorf("%w: too many filesystem attachment candidates", ErrAgentAttachmentInput)
	}
	result := make([]ReapableAgentRunAttachment, 0, len(spec.Candidates))
	seen := make(map[model.RunID]struct{}, len(spec.Candidates))
	for _, candidate := range spec.Candidates {
		if candidate.RunID.IsZero() || candidate.TokenHash.IsZero() {
			return nil, fmt.Errorf("%w: incomplete filesystem attachment candidate",
				ErrAgentAttachmentInput)
		}
		if _, duplicate := seen[candidate.RunID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate filesystem attachment candidate",
				ErrAgentAttachmentInput)
		}
		seen[candidate.RunID] = struct{}{}
		run, err := readAgentRun(ctx, tx, candidate.RunID)
		if err != nil {
			return nil, fmt.Errorf("%w: candidate AgentRun cannot be read: %v",
				ErrAgentAttachmentInvariant, err)
		}
		hash, hasHash := run.AttachmentTokenHash()
		expiresAt, hasExpiry := run.AttachmentExpiresAt()
		lease, hasLease := run.LeaseUntil()
		_, hasHandling := run.HandlingID()
		terminal := run.Status().Terminal()
		if !hasHash || !hasExpiry || !hasLease || !hasHandling ||
			run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() ||
			run.Launcher() != "mnemond-wake" || !sameAttachmentDigest(hash, candidate.TokenHash) ||
			!expiresAt.Equal(lease) {
			return nil, fmt.Errorf("%w: candidate AgentRun differs from wake authority",
				ErrAgentAttachmentInvariant)
		}
		if !terminal {
			if !expiresAt.After(at) {
				return nil, fmt.Errorf("%w: expired candidate AgentRun remained active",
					ErrAgentAttachmentInvariant)
			}
			continue
		}
		result = append(result, ReapableAgentRunAttachment{RunID: candidate.RunID,
			TokenHash: candidate.TokenHash})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("list reapable Agent Run attachments: commit: %w", err)
	}
	return result, nil
}

func (s *Store) withAgentRunAttachment(ctx context.Context, spec AgentAttachmentSpec,
	consume bool,
) (AgentClaimResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.AttachmentTokenHash.IsZero() {
		return AgentClaimResult{}, ErrAgentAttachmentInput
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: trusted time: %v", ErrAgentAttachmentInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("check Agent Run attachment: begin: %w", err)
	}
	defer tx.Rollback()
	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return AgentClaimResult{}, err
	}
	busy, err := recoverExpiredAgentClaim(ctx, tx, profile, budget, at)
	if err != nil {
		return AgentClaimResult{}, err
	}
	if !busy {
		if err := tx.Commit(); err != nil {
			return AgentClaimResult{}, fmt.Errorf("check Agent Run attachment: commit expiry: %w", err)
		}
		return AgentClaimResult{}, ErrAgentAttachmentStale
	}
	run, handling, err := readExactAgentRunAttachment(ctx, tx, profile, spec.AttachmentTokenHash, at)
	if err != nil {
		return AgentClaimResult{}, err
	}
	if consume {
		expiresAt, _ := run.AttachmentExpiresAt()
		result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET attached_at=?,status='running'
			WHERE run_id=? AND profile_id=? AND attachment_token_hash=? AND attachment_expires_at=?
			AND attached_at IS NULL AND status IN ('starting','running')`, storeTime(at), run.ID().String(),
			profile.ID().String(), spec.AttachmentTokenHash.Bytes(), storeTime(expiresAt))
		if err != nil {
			return AgentClaimResult{}, fmt.Errorf("consume Agent Run attachment: %w", err)
		}
		if err := requireExactlyOneRow(result, "consume Agent Run attachment: AgentRun CAS"); err != nil {
			return AgentClaimResult{}, fmt.Errorf("%w: %v", ErrAgentAttachmentStale, err)
		}
		run, err = readAgentRun(ctx, tx, run.ID())
		if err != nil {
			return AgentClaimResult{}, fmt.Errorf("%w: consumed AgentRun cannot be read",
				ErrAgentAttachmentInvariant)
		}
		if attachedAt, ok := run.AttachedAt(); !ok || !attachedAt.Equal(at) || run.Status() != model.AgentRunRunning {
			return AgentClaimResult{}, fmt.Errorf("%w: consumed AgentRun evidence differs",
				ErrAgentAttachmentInvariant)
		}
	}
	if err := tx.Commit(); err != nil {
		return AgentClaimResult{}, fmt.Errorf("check Agent Run attachment: commit: %w", err)
	}
	return AgentClaimResult{Status: AgentClaimActionable, Handling: handling, Run: run}, nil
}

func readExactAgentRunAttachment(ctx context.Context, tx *sql.Tx, profile model.Profile,
	token model.Digest, at time.Time,
) (model.AgentRun, model.Handling, error) {
	var runText string
	err := tx.QueryRowContext(ctx, `SELECT run_id FROM agent_runs
		WHERE profile_id=? AND attachment_token_hash=? AND attached_at IS NULL
		AND status IN ('starting','running')`, profile.ID().String(), token.Bytes()).Scan(&runText)
	if errors.Is(err, sql.ErrNoRows) {
		return model.AgentRun{}, model.Handling{}, ErrAgentAttachmentStale
	}
	if err != nil {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("check Agent Run attachment: locate Run: %w", err)
	}
	runID, err := model.ParseRunID(runText)
	if err != nil {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: invalid attached Run ID",
			ErrAgentAttachmentInvariant)
	}
	run, err := readAgentRun(ctx, tx, runID)
	if err != nil {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: invalid attached Run evidence: %v",
			ErrAgentAttachmentInvariant, err)
	}
	handlingID, ok := run.HandlingID()
	if !ok {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: attached Run is not Handling-bound",
			ErrAgentAttachmentInvariant)
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if err != nil {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: attached Handling cannot be read",
			ErrAgentAttachmentInvariant)
	}
	attachmentHash, hasAttachment := run.AttachmentTokenHash()
	expiresAt, hasExpiry := run.AttachmentExpiresAt()
	claimHash, hasClaim := run.ClaimFenceHash()
	lease, hasLease := run.LeaseUntil()
	if !hasAttachment || !hasExpiry || !hasClaim || !hasLease || run.ProfileID() != profile.ID() ||
		run.Runtime() != profile.Runtime() || run.Launcher() != "mnemond-wake" ||
		run.Status() != model.AgentRunStarting || !sameAttachmentDigest(attachmentHash, token) ||
		!sameAttachmentDigest(claimHash, token) || !expiresAt.Equal(lease) || !expiresAt.After(at) {
		return model.AgentRun{}, model.Handling{}, ErrAgentAttachmentStale
	}
	if _, attached := run.AttachedAt(); attached {
		return model.AgentRun{}, model.Handling{}, ErrAgentAttachmentStale
	}
	if err := requireExactCurrentClaim(run, handling, claimHash, at); err != nil {
		if errors.Is(err, ErrCurrentReadStale) {
			return model.AgentRun{}, model.Handling{}, ErrAgentAttachmentStale
		}
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: %v", ErrAgentAttachmentInvariant, err)
	}
	return run, handling, nil
}

func newWakeAgentRun(id model.RunID, profile model.Profile, handling model.Handling,
	token model.Digest, at, leaseUntil time.Time,
) (model.AgentRun, error) {
	cause, err := model.JSONFrom(struct {
		Kind       string           `json:"kind"`
		HandlingID model.HandlingID `json:"handling_id"`
		EventID    model.EventID    `json:"event_id"`
	}{"mnemond_wake", handling.ID(), handling.EventID()})
	if err != nil {
		return model.AgentRun{}, err
	}
	empty, err := model.NewJSON([]byte(`{}`))
	if err != nil {
		return model.AgentRun{}, err
	}
	handlingID := handling.ID()
	return model.NewAgentRun(model.AgentRunSpec{
		ID: id, ProfileID: profile.ID(), HandlingID: &handlingID, Cause: cause,
		HandlingAttempt: handling.Attempts(), ClaimFenceHash: &token, LeaseUntil: &leaseUntil,
		AttachmentTokenHash: &token, AttachmentExpiresAt: &leaseUntil,
		Launcher: "mnemond-wake", Runtime: profile.Runtime(), LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: model.AgentRunStarting, StartedAt: at,
	})
}

func sameAttachmentDigest(left, right model.Digest) bool {
	leftBytes, rightBytes := left.Bytes(), right.Bytes()
	equal := subtle.ConstantTimeCompare(leftBytes, rightBytes) == 1
	clear(leftBytes)
	clear(rightBytes)
	return equal
}
