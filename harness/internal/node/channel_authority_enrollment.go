package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const channelAuthoritySigningTimeout = 10 * time.Second

var _ peer.ChannelEnrollmentOwnerController = (*ChannelAuthorityCoordinator)(nil)

func nilChannelAuthoritySigner(signer store.ChannelAuthoritySigner) bool {
	if signer == nil {
		return true
	}
	value := reflect.ValueOf(signer)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// PrepareEnrollmentChallenge reads the one durable roster head a proof must
// bind. It deliberately does not acquire the mutation token: the Store method
// is a bounded read-only transaction and owns no Channel authority mutation.
func (coordinator *ChannelAuthorityCoordinator) PrepareEnrollmentChallenge(ctx context.Context,
	control peer.ChannelEnrollmentChallengeControl,
) (peer.ChannelEnrollmentChallengeAuthority, error) {
	if coordinator == nil || coordinator.store == nil || ctx == nil {
		return peer.ChannelEnrollmentChallengeAuthority{}, fmt.Errorf(
			"%w: coordinator is unavailable", ErrChannelAuthority)
	}
	prepared, err := coordinator.store.PrepareChannelEnrollment(ctx,
		store.PrepareChannelEnrollmentSpec{ChannelID: control.ChannelID,
			GrantID: control.GrantID, RequestID: control.RequestID,
			AuthenticatedPeerID: control.AuthenticatedPeerID,
			JoinerOriginEpoch:   control.JoinerOriginEpoch,
			JoinerPublicKey:     append([]byte(nil), control.JoinerPublicKey...), At: control.At})
	if err != nil {
		return peer.ChannelEnrollmentChallengeAuthority{}, mapChannelEnrollmentPrecommitError(err)
	}
	return peer.ChannelEnrollmentChallengeAuthority{RosterHead: prepared.RosterHead}, nil
}

// AcceptEnrollmentAuthority owns the complete signed Store plan and runtime
// transition while holding the same logical mutation token as every other
// local Channel authority mutation. Signer I/O occurs only between the two
// rollback-only Store preparation stages.
func (coordinator *ChannelAuthorityCoordinator) AcceptEnrollmentAuthority(ctx context.Context,
	control peer.ChannelEnrollmentAcceptanceControl,
) (peer.ChannelEnrollmentAcceptanceAuthority, error) {
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	defer release()
	signing, err := coordinator.store.PrepareChannelEnrollmentSigning(ctx,
		store.PrepareChannelEnrollmentSigningSpec{
			AuthenticatedPeerID: control.AuthenticatedPeerID, Transcript: control.Transcript,
			AdvertisedMultiaddrs: append([]string(nil), control.AdvertisedMultiaddrs...),
			Proof:                control.Proof, At: control.At})
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, mapChannelEnrollmentPrecommitError(err)
	}
	signatures, err := coordinator.signEnrollment(ctx, signing)
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, fmt.Errorf(
			"%w: sign enrollment authority: %w", ErrChannelAuthority, err)
	}
	plan, err := coordinator.store.PrepareSignedChannelEnrollment(ctx, signing, signatures)
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	expected := plan.Result()
	if _, err := channelEnrollmentAcceptanceAuthority(control, expected); err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	result, err := executeChannelAuthorityPlan(ctx, coordinator.runtime,
		channelAuthorityPlanSteps[store.AcceptChannelEnrollmentResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: expected,
			commit: func(commitCtx context.Context) (store.AcceptChannelEnrollmentResult, error) {
				committed, commitErr := coordinator.store.CommitChannelEnrollment(commitCtx, plan)
				if commitErr == nil {
					if _, validationErr := channelEnrollmentAcceptanceAuthority(control,
						committed); validationErr != nil {
						return store.AcceptChannelEnrollmentResult{}, validationErr
					}
					if !sameChannelEnrollmentResult(committed, expected) {
						return store.AcceptChannelEnrollmentResult{}, fmt.Errorf(
							"%w: committed enrollment result differs from prepared authority",
							ErrChannelAuthority)
					}
				}
				return committed, commitErr
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return coordinator.store.ResolveChannelEnrollment(resolveCtx, plan)
			},
		})
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	return channelEnrollmentAcceptanceAuthority(control, result)
}

// signEnrollment is the only arbitrary test seam around the Node-owned
// identity signer. Implementations must be synchronous, honor ctx, and never
// re-enter this coordinator; the production constructor never accepts one.
func (coordinator *ChannelAuthorityCoordinator) signEnrollment(ctx context.Context,
	plan store.ChannelEnrollmentSigningPlan,
) (store.ChannelEnrollmentSignatures, error) {
	if !plan.RequiresSignatures() {
		return store.ChannelEnrollmentSignatures{}, nil
	}
	signingCtx, cancel := context.WithTimeout(ctx, channelAuthoritySigningTimeout)
	defer cancel()
	member, err := coordinator.signer.Sign(signingCtx, plan.MemberSigningMessage())
	if err != nil {
		return store.ChannelEnrollmentSignatures{}, err
	}
	member = append([]byte(nil), member...)
	receipt, err := coordinator.signer.Sign(signingCtx, plan.ReceiptSigningMessage())
	if err != nil {
		return store.ChannelEnrollmentSignatures{}, err
	}
	return store.ChannelEnrollmentSignatures{MemberSignature: member,
		ReceiptSignature: append([]byte(nil), receipt...)}, nil
}

func channelEnrollmentAcceptanceAuthority(control peer.ChannelEnrollmentAcceptanceControl,
	result store.AcceptChannelEnrollmentResult,
) (peer.ChannelEnrollmentAcceptanceAuthority, error) {
	status, err := channelEnrollmentAcceptanceStatus(result.Status)
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	if err := validateChannelEnrollmentAcceptance(control, result, status); err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	member, err := model.ParseMember(result.Member.WireJSON().Bytes())
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, fmt.Errorf(
			"%w: clone accepted member", ErrChannelAuthority)
	}
	receipt, err := model.ParseEnrollmentReceipt(result.Receipt.WireJSON().Bytes())
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, fmt.Errorf(
			"%w: clone accepted receipt", ErrChannelAuthority)
	}
	roster, err := cloneChannelEnrollmentRoster(result.Roster.Members())
	if err != nil {
		return peer.ChannelEnrollmentAcceptanceAuthority{}, err
	}
	return peer.ChannelEnrollmentAcceptanceAuthority{Status: status, Member: member,
		Roster: roster, Receipt: receipt}, nil
}

func channelEnrollmentAcceptanceStatus(
	status store.ChannelEnrollmentStatus,
) (peer.ChannelEnrollmentStatus, error) {
	switch status {
	case store.ChannelEnrollmentAccepted:
		return peer.ChannelEnrollmentAccepted, nil
	case store.ChannelEnrollmentReplayed:
		return peer.ChannelEnrollmentReplayed, nil
	case store.ChannelEnrollmentMemberRevoked:
		return peer.ChannelEnrollmentMemberRevoked, nil
	case store.ChannelEnrollmentChannelClosed:
		return peer.ChannelEnrollmentChannelClosed, nil
	default:
		return "", fmt.Errorf("%w: Store returned unknown enrollment result",
			ErrChannelAuthority)
	}
}

func validateChannelEnrollmentAcceptance(control peer.ChannelEnrollmentAcceptanceControl,
	result store.AcceptChannelEnrollmentResult, status peer.ChannelEnrollmentStatus,
) error {
	descriptor, transcript := result.Channel.Descriptor(), control.Transcript
	roster := result.Roster
	addresses, addressErr := model.AdvertisedAddressDigest(control.AdvertisedMultiaddrs)
	if control.AuthenticatedPeerID.IsZero() || transcript.IsZero() || result.Channel.ID().IsZero() ||
		result.Member.IsZero() || result.Receipt.IsZero() || roster.IsZero() ||
		addressErr != nil || addresses != transcript.AdvertisedAddressDigest() ||
		transcript.ChannelID() != result.Channel.ID() ||
		transcript.JoinerPeerID() != control.AuthenticatedPeerID ||
		!bytes.Equal(descriptor.WireJSON().Bytes(), roster.Descriptor().WireJSON().Bytes()) ||
		result.Channel.RosterHead() != roster.Head() ||
		model.VerifyEnrollmentReceipt(descriptor, result.Member, transcript, result.Receipt) != nil ||
		!channelEnrollmentRosterContains(roster, result.Member) ||
		!validChannelEnrollmentAcceptanceStatus(status, result.Channel, roster,
			control.AuthenticatedPeerID) {
		return fmt.Errorf("%w: Store returned enrollment authority outside the request",
			ErrChannelAuthority)
	}
	return nil
}

func channelEnrollmentRosterContains(roster model.VerifiedRoster, member model.Member) bool {
	revision := member.Head().Revision()
	members := roster.Members()
	return revision != 0 && revision <= uint64(len(members)) &&
		bytes.Equal(members[revision-1].WireJSON().Bytes(), member.WireJSON().Bytes())
}

func validChannelEnrollmentAcceptanceStatus(status peer.ChannelEnrollmentStatus,
	channel model.Channel, roster model.VerifiedRoster, joiner model.PeerID,
) bool {
	current, currentOK := roster.CurrentMember(joiner)
	owner, ownerOK := roster.CurrentMember(channel.OwnerPeerID())
	if !currentOK || !ownerOK || channel.Status() == model.ChannelConflicted {
		return false
	}
	if current.Status().Terminal() {
		return status == peer.ChannelEnrollmentMemberRevoked
	}
	if owner.Status().Terminal() || channel.Status() == model.ChannelClosed {
		return status == peer.ChannelEnrollmentChannelClosed
	}
	return channel.Status() == model.ChannelActive &&
		(status == peer.ChannelEnrollmentAccepted || status == peer.ChannelEnrollmentReplayed)
}

func cloneChannelEnrollmentRoster(members []model.Member) ([]model.Member, error) {
	cloned := make([]model.Member, len(members))
	for index, member := range members {
		parsed, err := model.ParseMember(member.WireJSON().Bytes())
		if err != nil {
			return nil, fmt.Errorf("%w: clone accepted roster", ErrChannelAuthority)
		}
		cloned[index] = parsed
	}
	return cloned, nil
}

func sameChannelEnrollmentResult(left, right store.AcceptChannelEnrollmentResult) bool {
	if left.Status != right.Status || left.Channel.ID() != right.Channel.ID() ||
		left.Channel.LocalAlias() != right.Channel.LocalAlias() ||
		left.Channel.RosterHead() != right.Channel.RosterHead() ||
		left.Channel.Status() != right.Channel.Status() ||
		left.Channel.TopicState() != right.Channel.TopicState() ||
		left.Channel.UpdatedAt() != right.Channel.UpdatedAt() ||
		!bytes.Equal(left.Channel.Descriptor().WireJSON().Bytes(),
			right.Channel.Descriptor().WireJSON().Bytes()) ||
		!bytes.Equal(left.Roster.Descriptor().WireJSON().Bytes(),
			right.Roster.Descriptor().WireJSON().Bytes()) ||
		!bytes.Equal(left.Member.WireJSON().Bytes(), right.Member.WireJSON().Bytes()) ||
		!bytes.Equal(left.Receipt.WireJSON().Bytes(), right.Receipt.WireJSON().Bytes()) ||
		left.Roster.Head() != right.Roster.Head() {
		return false
	}
	leftMembers, rightMembers := left.Roster.Members(), right.Roster.Members()
	if len(leftMembers) != len(rightMembers) {
		return false
	}
	for index := range leftMembers {
		if !bytes.Equal(leftMembers[index].WireJSON().Bytes(), rightMembers[index].WireJSON().Bytes()) {
			return false
		}
	}
	return true
}

// mapChannelEnrollmentPrecommitError is deliberately limited to deterministic
// business rejections before signing begins. Every later failure remains an
// ordinary internal error so the peer layer resets instead of writing a code.
func mapChannelEnrollmentPrecommitError(cause error) error {
	var code peer.ChannelProtocolErrorCode
	switch {
	case errors.Is(cause, store.ErrChannelEnrollmentOwner):
		code = peer.ChannelErrorWrongOwner
	case errors.Is(cause, store.ErrChannelEnrollmentProof):
		code = peer.ChannelErrorBadProof
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExpired):
		code = peer.ChannelErrorTokenExpired
	case errors.Is(cause, store.ErrChannelEnrollmentTokenClosed):
		code = peer.ChannelErrorTokenClosed
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExhausted):
		code = peer.ChannelErrorTokenExhausted
	case errors.Is(cause, store.ErrChannelFull):
		code = peer.ChannelErrorChannelFull
	case errors.Is(cause, store.ErrChannelEnrollmentChannelClosed):
		code = peer.ChannelErrorChannelClosed
	case errors.Is(cause, store.ErrChannelEnrollmentMemberRevoked):
		code = peer.ChannelErrorMemberRevoked
	case errors.Is(cause, store.ErrChannelEnrollmentStale):
		code = peer.ChannelErrorRosterGap
	case errors.Is(cause, store.ErrChannelEnrollmentConflict):
		code = peer.ChannelErrorRosterConflict
	case errors.Is(cause, store.ErrChannelEnrollmentUnavailable),
		errors.Is(cause, store.ErrChannelEnrollmentInput):
		code = peer.ChannelErrorInvalidToken
	default:
		return cause
	}
	failure, err := peer.NewChannelEnrollmentControlFailure(code)
	if err != nil {
		return cause
	}
	return failure
}
