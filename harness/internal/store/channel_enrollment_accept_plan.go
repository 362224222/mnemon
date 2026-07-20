package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// PrepareChannelEnrollmentSigningSpec contains no private-key capability.
type PrepareChannelEnrollmentSigningSpec struct {
	AuthenticatedPeerID  model.PeerID
	Transcript           model.EnrollmentTranscript
	AdvertisedMultiaddrs []string
	Proof                model.Digest
	At                   time.Time
}

// ChannelEnrollmentSignatures carries the two owner signatures produced
// outside Store transactions. Replay plans require both fields to be empty.
type ChannelEnrollmentSignatures struct {
	MemberSignature  []byte
	ReceiptSignature []byte
}

// ChannelEnrollmentSigningPlan is an opaque, Store-bound frozen signing job.
type ChannelEnrollmentSigningPlan struct {
	preimage       channelAuthorityPlan
	input          channelEnrollmentInput
	evidence       channelEnrollmentEvidence
	memberRecord   model.MemberRecord
	receiptRecord  model.EnrollmentReceiptRecord
	memberMessage  string
	receiptMessage string
	replayResult   AcceptChannelEnrollmentResult
}

func (plan ChannelEnrollmentSigningPlan) RequiresSignatures() bool {
	return !plan.memberRecord.IsZero()
}

func (plan ChannelEnrollmentSigningPlan) MemberSigningMessage() []byte {
	return []byte(plan.memberMessage)
}

func (plan ChannelEnrollmentSigningPlan) ReceiptSigningMessage() []byte {
	return []byte(plan.receiptMessage)
}

func (plan ChannelEnrollmentSigningPlan) validFor(st *Store) bool {
	return plan.preimage.validFor(st) && plan.input.valid() && plan.evidence.valid()
}

// ChannelEnrollmentPlan is the final opaque owner-authority transition.
type ChannelEnrollmentPlan struct {
	channelAuthorityPlan
	input     channelEnrollmentInput
	before    channelEnrollmentEvidence
	after     channelEnrollmentEvidence
	candidate channelEnrollmentCandidate
	result    AcceptChannelEnrollmentResult
}

func (plan ChannelEnrollmentPlan) Result() AcceptChannelEnrollmentResult { return plan.result }

// PrepareChannelEnrollmentSigning validates proof and exact preimage in a
// rollback-only transaction, then exposes only immutable signing messages.
func (s *Store) PrepareChannelEnrollmentSigning(ctx context.Context,
	spec PrepareChannelEnrollmentSigningSpec,
) (ChannelEnrollmentSigningPlan, error) {
	input, err := freezeChannelEnrollmentInput(s, ctx, spec)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, fmt.Errorf("prepare Channel enrollment signing: begin: %w", err)
	}
	defer tx.Rollback()
	before, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	state, err := readChannelEnrollmentState(ctx, tx, input)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	prepared, err := prepareChannelEnrollmentSigningCandidate(input, state)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	core, err := newChannelAuthorityPlan(s, before, before)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	prepared.preimage, prepared.input, prepared.evidence = core, input, state.evidence
	return prepared, nil
}

// PrepareSignedChannelEnrollment verifies exact external signatures and
// constructs the final authority candidate in a second rollback-only transaction.
func (s *Store) PrepareSignedChannelEnrollment(ctx context.Context,
	signing ChannelEnrollmentSigningPlan, signatures ChannelEnrollmentSignatures,
) (ChannelEnrollmentPlan, error) {
	if ctx == nil || !signing.validFor(s) {
		return ChannelEnrollmentPlan{}, ErrChannelAuthorityPlan
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ChannelEnrollmentPlan{}, fmt.Errorf("prepare signed Channel enrollment: begin: %w", err)
	}
	defer tx.Rollback()
	resolution, err := inspectChannelAuthorityPlan(ctx, tx, signing.preimage)
	if err != nil {
		return ChannelEnrollmentPlan{}, err
	}
	state, err := readChannelEnrollmentState(ctx, tx, signing.input)
	if err != nil || resolution != ChannelAuthorityPlanUnchanged || state.evidence != signing.evidence {
		return ChannelEnrollmentPlan{}, ErrChannelAuthorityPlanDiverged
	}
	candidate, err := buildSignedChannelEnrollmentCandidate(signing, signatures, state)
	if err != nil {
		return ChannelEnrollmentPlan{}, err
	}
	before, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return ChannelEnrollmentPlan{}, err
	}
	result, err := applyChannelEnrollmentCandidate(ctx, tx, state, candidate)
	if err != nil {
		return ChannelEnrollmentPlan{}, err
	}
	core, err := finishChannelAuthorityPlan(s, ctx, tx, before)
	if err != nil {
		return ChannelEnrollmentPlan{}, err
	}
	after, err := readChannelEnrollmentState(ctx, tx, signing.input)
	if err != nil {
		return ChannelEnrollmentPlan{}, err
	}
	candidate.result = result
	return ChannelEnrollmentPlan{channelAuthorityPlan: core, input: signing.input,
		before: signing.evidence, after: after.evidence, candidate: candidate, result: result}, nil
}

// CommitChannelEnrollment atomically applies only the frozen exact preimage.
func (s *Store) CommitChannelEnrollment(ctx context.Context,
	plan ChannelEnrollmentPlan,
) (AcceptChannelEnrollmentResult, error) {
	if !plan.validFor(s) || !plan.input.valid() || !plan.before.valid() || !plan.after.valid() {
		return AcceptChannelEnrollmentResult{}, ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, false)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	defer tx.Rollback()
	state, err := readChannelEnrollmentState(ctx, tx, plan.input)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if state.evidence == plan.after && channelEnrollmentPlanMatchesRuntime(resolution, plan) {
		return plan.result, nil
	}
	if resolution != ChannelAuthorityPlanUnchanged || state.evidence != plan.before {
		return AcceptChannelEnrollmentResult{}, ErrChannelAuthorityPlanDiverged
	}
	result, err := applyChannelEnrollmentCandidate(ctx, tx, state, plan.candidate)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if err := verifyChannelEnrollmentCommitPostimage(ctx, tx, plan); err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	return result, nil
}

// ResolveChannelEnrollment classifies an unknown commit using whole-mesh and
// exact member/use/receipt/grant/channel/binding evidence.
func (s *Store) ResolveChannelEnrollment(ctx context.Context,
	plan ChannelEnrollmentPlan,
) (ChannelAuthorityPlanResolution, error) {
	if !plan.validFor(s) || !plan.input.valid() || !plan.before.valid() || !plan.after.valid() {
		return "", ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	state, err := readChannelEnrollmentState(ctx, tx, plan.input)
	if err != nil {
		return "", err
	}
	if state.evidence == plan.after && channelEnrollmentPlanMatchesRuntime(resolution, plan) {
		return ChannelAuthorityPlanCandidate, nil
	}
	if resolution == ChannelAuthorityPlanUnchanged && state.evidence == plan.before {
		return ChannelAuthorityPlanUnchanged, nil
	}
	return ChannelAuthorityPlanDiverged, nil
}

func (plan ChannelEnrollmentPlan) validFor(st *Store) bool {
	return plan.channelAuthorityPlan.validFor(st) && plan.result.Status != ""
}

func channelEnrollmentPlanMatchesRuntime(resolution ChannelAuthorityPlanResolution,
	plan ChannelEnrollmentPlan,
) bool {
	return resolution == ChannelAuthorityPlanCandidate ||
		(!plan.ChangesAuthority() && resolution == ChannelAuthorityPlanUnchanged)
}

func verifyChannelEnrollmentCommitPostimage(ctx context.Context, tx *sql.Tx,
	plan ChannelEnrollmentPlan,
) error {
	resolution, err := inspectChannelAuthorityPlan(ctx, tx, plan.channelAuthorityPlan)
	if err != nil {
		return err
	}
	state, err := readChannelEnrollmentState(ctx, tx, plan.input)
	if err != nil {
		return err
	}
	if state.evidence != plan.after || !channelEnrollmentPlanMatchesRuntime(resolution, plan) {
		return ErrChannelAuthorityPlanDiverged
	}
	return nil
}

func signChannelEnrollmentPlan(ctx context.Context, signer ChannelAuthoritySigner,
	plan ChannelEnrollmentSigningPlan,
) (ChannelEnrollmentSignatures, error) {
	if !plan.RequiresSignatures() {
		return ChannelEnrollmentSignatures{}, nil
	}
	member, err := signer.Sign(ctx, plan.MemberSigningMessage())
	if err != nil {
		return ChannelEnrollmentSignatures{}, fmt.Errorf("accept Channel enrollment: sign member: %w", err)
	}
	receipt, err := signer.Sign(ctx, plan.ReceiptSigningMessage())
	if err != nil {
		return ChannelEnrollmentSignatures{}, fmt.Errorf("accept Channel enrollment: sign receipt: %w", err)
	}
	return ChannelEnrollmentSignatures{MemberSignature: member, ReceiptSignature: receipt}, nil
}
