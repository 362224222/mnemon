package node

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
)

const agencyCandidateEntropyBytes = 16

// LocalAgencyServiceOptions supplies only deterministic test seams. Production
// uses the wall clock and crypto/rand.
type LocalAgencyServiceOptions struct {
	Clock  Clock
	Random io.Reader
}

// LocalAgencyService mechanically composes the R7 local authority and the
// existing immutable CAS adapter. It owns no workflow or semantic dispatch.
type LocalAgencyService struct {
	principal agency.AgentPrincipalID
	store     *authority.Store
	artifacts *r7ArtifactAdapter
	clock     Clock
	random    io.Reader
}

func NewLocalAgencyService(principal agency.AgentPrincipalID, store *authority.Store,
	artifacts *r7ArtifactAdapter, supplied ...LocalAgencyServiceOptions,
) (*LocalAgencyService, error) {
	if len(supplied) > 1 {
		return nil, errors.New("local agency service: at most one option set is allowed")
	}
	options := LocalAgencyServiceOptions{}
	if len(supplied) == 1 {
		options = supplied[0]
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.Random == nil {
		options.Random = cryptorand.Reader
	}
	if principal.IsZero() || store == nil || store.Path() == "" || artifacts == nil ||
		artifacts.cas == nil || options.Clock == nil || options.Random == nil {
		return nil, errors.New("local agency service: Principal, authority, CAS, clock, and randomness are required")
	}
	return &LocalAgencyService{principal: principal, store: store, artifacts: artifacts,
		clock: options.Clock, random: options.Random}, nil
}

func (service *LocalAgencyService) AgencyAttach(ctx context.Context) (AgencyAttachment, error) {
	if err := service.available(ctx); err != nil {
		return AgencyAttachment{}, err
	}
	proof, err := service.store.IssueInteractiveAttachment(ctx, service.principal)
	if err != nil {
		return AgencyAttachment{}, fmt.Errorf("local agency attach: %w", err)
	}
	return AgencyAttachment{ID: proof.ID().String(), Credential: proof.Credential(),
		ExpiresAt: proof.ExpiresAt()}, nil
}

func (service *LocalAgencyService) AgencyCurrent(ctx context.Context,
	authorityValue AgencyAuthority,
) (AgencyView, error) {
	if err := service.available(ctx); err != nil {
		return AgencyView{}, err
	}
	view, err := service.store.Current(ctx, authorityValue.proof, authorityValue.current)
	if err != nil {
		return AgencyView{}, fmt.Errorf("local agency current: %w", err)
	}
	projected, err := ProjectAgencyView(view.AgentView())
	if err != nil {
		return AgencyView{}, fmt.Errorf("local agency current project: %w", err)
	}
	return projected, nil
}

func (service *LocalAgencyService) AgencySubmit(ctx context.Context, authorityValue AgencyAuthority,
	submission AgencySubmission,
) (AgencyReceipt, error) {
	if err := service.available(ctx); err != nil {
		return AgencyReceipt{}, err
	}
	view, err := service.store.ReplayCurrent(ctx, authorityValue.proof, authorityValue.current)
	if err != nil {
		return AgencyReceipt{}, fmt.Errorf("local agency submit current: %w", err)
	}
	candidates, err := bindAgencyCandidates(submission.operation, submission.intent,
		submission.candidates)
	if err != nil {
		return AgencyReceipt{}, err
	}
	bound, err := view.Bind(submission.intent, submission.operation, candidates)
	if err != nil {
		return AgencyReceipt{}, fmt.Errorf("local agency submit bind: %w", err)
	}
	result, err := service.store.Admit(ctx, authorityValue.proof, bound)
	if err != nil {
		return AgencyReceipt{}, fmt.Errorf("local agency submit admit: %w", err)
	}
	receipt, err := agency.ParseReceiptCanonicalJSON(result.ReceiptJSON())
	if err != nil || receipt.Digest() != result.ReceiptDigest() || receipt.Outcome() != result.Outcome() {
		return AgencyReceipt{}, errors.New("local agency submit: authority returned corrupt Receipt")
	}
	projected, err := agency.ProjectAgentReceipt(receipt, result.Replayed())
	if err != nil {
		return AgencyReceipt{}, fmt.Errorf("local agency submit project Receipt: %w", err)
	}
	transport, err := ProjectAgencyReceipt(projected)
	if err != nil {
		return AgencyReceipt{}, fmt.Errorf("local agency submit transport Receipt: %w", err)
	}
	return transport, nil
}

func bindAgencyCandidates(operation agency.OperationKey, intent agency.AgentIntent,
	specs []AgencyCapturedCandidate,
) ([]agency.CapturedCandidate, error) {
	if operation.IsZero() {
		return nil, errors.New("local agency submit: admission operation is required")
	}
	if len(specs) > agency.MaxArtifactInputs {
		return nil, fmt.Errorf("local agency submit: candidate count exceeds %d", agency.MaxArtifactInputs)
	}
	inputs := make(map[string]agency.ArtifactInput)
	for _, input := range intent.Artifacts() {
		if input.Kind() == agency.ArtifactInputCandidate {
			inputs[input.Handle().String()] = input
		}
	}
	result := make([]agency.CapturedCandidate, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		input, exists := inputs[spec.handle.String()]
		if spec.handle.IsZero() || spec.digest.IsZero() || !exists {
			return nil, errors.New("local agency submit: candidate is absent from Intent")
		}
		if _, duplicate := seen[spec.handle.String()]; duplicate {
			return nil, errors.New("local agency submit: duplicate candidate binding")
		}
		seen[spec.handle.String()] = struct{}{}
		candidate, err := agency.NewCapturedCandidate(operation, input, spec.digest)
		if err != nil {
			return nil, fmt.Errorf("local agency submit candidate: %w", err)
		}
		result = append(result, candidate)
	}
	return result, nil
}

func (service *LocalAgencyService) AgencyCapture(ctx context.Context,
	content []byte,
) (AgencyArtifactCapture, error) {
	if err := service.available(ctx); err != nil {
		return AgencyArtifactCapture{}, err
	}
	verified, err := service.artifacts.Put(ctx, content, service.clock.Now())
	if err != nil {
		return AgencyArtifactCapture{}, fmt.Errorf("local agency capture: %w", err)
	}
	if err := service.store.CatalogArtifact(ctx, verified); err != nil {
		return AgencyArtifactCapture{}, fmt.Errorf("local agency capture catalog: %w", err)
	}
	handle, err := service.newCandidateHandle()
	if err != nil {
		return AgencyArtifactCapture{}, err
	}
	return AgencyArtifactCapture{Handle: handle.String(), Digest: verified.Digest().String(),
		ByteSize: verified.ByteSize()}, nil
}

func (service *LocalAgencyService) AgencyStatus(ctx context.Context) (AgencyStatusSnapshot, error) {
	if err := service.available(ctx); err != nil {
		return AgencyStatusSnapshot{}, err
	}
	return AgencyStatusSnapshot{Ready: true}, nil
}

func (service *LocalAgencyService) available(ctx context.Context) error {
	if service == nil || service.store == nil || service.artifacts == nil || service.clock == nil ||
		service.random == nil || service.principal.IsZero() || ctx == nil {
		return errors.New("local agency service is unavailable")
	}
	return ctx.Err()
}

func (service *LocalAgencyService) newCandidateHandle() (agency.OpaqueHandle, error) {
	var entropy [agencyCandidateEntropyBytes]byte
	if _, err := io.ReadFull(service.random, entropy[:]); err != nil {
		return agency.OpaqueHandle{}, fmt.Errorf("local agency capture handle: %w", err)
	}
	value := "artifact:" + base64.RawURLEncoding.EncodeToString(entropy[:])
	handle, err := agency.NewOpaqueHandle(value)
	if err != nil {
		return agency.OpaqueHandle{}, fmt.Errorf("local agency capture handle: %w", err)
	}
	return handle, nil
}

var _ AgencyService = (*LocalAgencyService)(nil)
