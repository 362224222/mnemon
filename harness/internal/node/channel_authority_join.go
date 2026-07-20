package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ChannelJoinSpec is the complete caller-selected join surface. Network
// addresses come from the managed MeshRuntime and cannot be supplied here.
type ChannelJoinSpec struct {
	Token        model.EnrollmentToken
	DisplayLabel string
	LocalAlias   string
}

type frozenChannelJoinSpec struct {
	token        model.EnrollmentToken
	displayLabel string
	localAlias   string
}

type channelAuthorityJoinRuntime interface {
	joinChannel(context.Context, peer.JoinChannelSpec,
		peer.ChannelJoinSession,
	) (peer.ChannelJoinResult, error)
}

func (runtime meshChannelAuthorityRuntime) joinChannel(ctx context.Context,
	spec peer.JoinChannelSpec, session peer.ChannelJoinSession,
) (peer.ChannelJoinResult, error) {
	return runtime.runtime.JoinChannel(ctx, spec, session)
}

// JoinChannel holds the same capacity-one authority token as create, owner
// enrollment, roster and baseline mutations for the complete remote exchange
// and local durable settlement.
func (coordinator *ChannelAuthorityCoordinator) JoinChannel(ctx context.Context,
	spec ChannelJoinSpec,
) (peer.ChannelJoinResult, error) {
	frozen, err := freezeChannelJoinSpec(spec)
	if err != nil {
		return peer.ChannelJoinResult{}, err
	}
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return peer.ChannelJoinResult{}, err
	}
	defer release()
	joining, ok := coordinator.runtime.(channelAuthorityJoinRuntime)
	if !ok {
		return peer.ChannelJoinResult{}, fmt.Errorf("%w: managed join runtime is unavailable",
			ErrChannelAuthority)
	}
	session := &channelAuthorityJoinSession{store: coordinator.store,
		runtime: coordinator.runtime, spec: frozen}
	result, err := joining.joinChannel(ctx, peer.JoinChannelSpec{Token: frozen.token,
		DisplayLabel: frozen.displayLabel, LocalAlias: frozen.localAlias}, session)
	if err == nil && !session.finished() {
		return peer.ChannelJoinResult{}, fmt.Errorf(
			"%w: managed join returned before durable settlement", ErrChannelAuthority)
	}
	return result, err
}

func freezeChannelJoinSpec(spec ChannelJoinSpec) (frozenChannelJoinSpec, error) {
	token, err := model.ParseEnrollmentToken(spec.Token.Reveal())
	if err != nil || model.VerifyEnrollmentToken(token) != nil || spec.DisplayLabel == "" ||
		spec.LocalAlias == "" {
		return frozenChannelJoinSpec{}, fmt.Errorf("%w: invalid Channel join request",
			ErrChannelAuthority)
	}
	return frozenChannelJoinSpec{token: token, displayLabel: spec.DisplayLabel,
		localAlias: spec.LocalAlias}, nil
}

type channelJoinSessionPhase uint8

const (
	channelJoinSessionUnused channelJoinSessionPhase = iota
	channelJoinSessionBeginning
	channelJoinSessionPrepared
	channelJoinSessionMarking
	channelJoinSessionMarked
	channelJoinSessionFinished
)

// channelAuthorityJoinSession is the one-use semantic adapter between peer
// framing and Node-owned Store authority. It never reacquires the coordinator
// token because JoinChannel holds that token around every callback.
type channelAuthorityJoinSession struct {
	store   channelAuthorityStore
	runtime channelAuthorityRuntime
	spec    frozenChannelJoinSpec

	mu       sync.Mutex
	phase    channelJoinSessionPhase
	control  peer.ChannelJoinPrepareControl
	prepared store.PrepareJoinedChannelResult
}

func (session *channelAuthorityJoinSession) BeginChannelJoin(ctx context.Context,
	control peer.ChannelJoinPrepareControl,
) (peer.PreparedChannelJoin, error) {
	if !session.move(channelJoinSessionUnused, channelJoinSessionBeginning) {
		return peer.PreparedChannelJoin{}, session.orderError("begin")
	}
	frozen, err := session.freezePrepareControl(control)
	if err != nil {
		session.finish()
		return peer.PreparedChannelJoin{}, err
	}
	prepared, err := session.store.PrepareJoinedChannel(ctx, store.PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: frozen.AuthenticatedLocalPeerID,
		LocalPublicKey:           append([]byte(nil), frozen.LocalPublicKey...),
		Descriptor:               frozen.Descriptor, GrantID: frozen.GrantID,
		LocalAlias: frozen.LocalAlias, At: frozen.At})
	if err != nil {
		session.finish()
		return peer.PreparedChannelJoin{}, mapChannelJoinBeginError(err)
	}
	if !validPreparedChannelJoin(frozen, prepared) {
		session.finish()
		return peer.PreparedChannelJoin{}, fmt.Errorf(
			"%w: Store returned invalid joined Channel reservation", ErrChannelAuthority)
	}
	projection, err := peer.NewPreparedChannelJoin(prepared.RequestID, prepared.OriginEpoch,
		prepared.Reserved, prepared.CommitUnknown)
	if err != nil {
		session.finish()
		return peer.PreparedChannelJoin{}, fmt.Errorf(
			"%w: project joined Channel reservation: %w", ErrChannelAuthority, err)
	}
	session.mu.Lock()
	session.control, session.prepared = frozen, prepared
	session.phase = channelJoinSessionPrepared
	session.mu.Unlock()
	return projection, nil
}

func (session *channelAuthorityJoinSession) MarkChannelJoinCommitUnknown(ctx context.Context,
	at time.Time,
) error {
	control, prepared, ok := session.startMark()
	if !ok {
		return session.orderError("mark commit_unknown")
	}
	err := session.store.MarkJoinedChannelCommitUnknown(ctx, prepared.RequestID,
		control.AuthenticatedLocalPeerID, prepared.Attempt, at)
	session.mu.Lock()
	if err == nil {
		session.phase = channelJoinSessionMarked
	} else {
		session.phase = channelJoinSessionPrepared
	}
	session.mu.Unlock()
	if err != nil {
		return fmt.Errorf("%w: mark joined Channel commit_unknown: %w",
			ErrChannelAuthority, err)
	}
	return nil
}

func (session *channelAuthorityJoinSession) ReleaseChannelJoinReservation(
	ctx context.Context,
) error {
	control, prepared, ok := session.startTerminal(true)
	if !ok {
		return session.orderError("release reservation")
	}
	if err := session.store.ReleaseJoinedChannelReservation(ctx, prepared.RequestID,
		control.AuthenticatedLocalPeerID, prepared.Attempt); err != nil {
		return fmt.Errorf("%w: release joined Channel reservation: %w",
			ErrChannelAuthority, err)
	}
	return nil
}

func (session *channelAuthorityJoinSession) InstallAcceptedChannelJoin(ctx context.Context,
	accepted peer.VerifiedChannelEnrollment, at time.Time,
) (peer.ChannelJoinResult, error) {
	control, prepared, ok := session.startTerminal(false)
	if !ok {
		return peer.ChannelJoinResult{}, session.orderError("install accepted authority")
	}
	evidence, err := freezeChannelJoinEvidence(session.spec, control, prepared, accepted)
	if err != nil {
		return peer.ChannelJoinResult{}, err
	}
	status, err := storeChannelJoinStatus(evidence.status)
	if err != nil {
		return peer.ChannelJoinResult{}, err
	}
	plan, err := session.store.PrepareJoinedChannelInstall(ctx, store.InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: evidence.owner, OwnerOutcome: status,
		LocalAlias: control.LocalAlias, Descriptor: evidence.descriptor,
		Transcript: evidence.transcript, Receipt: evidence.receipt,
		Members: evidence.roster.Members(), At: at})
	if err != nil {
		return peer.ChannelJoinResult{}, fmt.Errorf(
			"%w: prepare joined Channel install: %w", ErrChannelAuthority, err)
	}
	expected := plan.Result()
	if _, err := projectChannelJoinResult(evidence, control.LocalAlias, expected); err != nil {
		return peer.ChannelJoinResult{}, err
	}
	result, err := executeChannelAuthorityPlan(ctx, session.runtime,
		channelAuthorityPlanSteps[store.InstallJoinedChannelResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: expected,
			commit: func(commitCtx context.Context) (store.InstallJoinedChannelResult, error) {
				committed, commitErr := session.store.CommitJoinedChannelInstall(commitCtx, plan)
				if commitErr == nil && !sameJoinedChannelInstallResult(
					committed, expected, plan.ChangesAuthority()) {
					return store.InstallJoinedChannelResult{}, fmt.Errorf(
						"%w: committed joined Channel differs from prepared authority",
						ErrChannelAuthority)
				}
				return committed, commitErr
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return session.store.ResolveJoinedChannelInstall(resolveCtx, plan)
			},
		})
	if err != nil {
		return peer.ChannelJoinResult{}, err
	}
	return projectChannelJoinResult(evidence, control.LocalAlias, result)
}

func (session *channelAuthorityJoinSession) freezePrepareControl(
	control peer.ChannelJoinPrepareControl,
) (peer.ChannelJoinPrepareControl, error) {
	payload := session.spec.token.Payload()
	descriptor, err := model.ParseSignedChannelDescriptor(control.Descriptor.WireJSON().Bytes())
	if err != nil || control.AuthenticatedLocalPeerID.IsZero() || len(control.LocalPublicKey) == 0 ||
		control.GrantID != payload.GrantID() || control.LocalAlias != session.spec.localAlias ||
		!bytes.Equal(descriptor.WireJSON().Bytes(), payload.Descriptor().WireJSON().Bytes()) {
		return peer.ChannelJoinPrepareControl{}, fmt.Errorf(
			"%w: managed runtime changed the Channel join request", ErrChannelAuthority)
	}
	addresses := append([]string(nil), control.AdvertisedMultiaddrs...)
	if _, err := model.AdvertisedAddressDigest(addresses); err != nil {
		return peer.ChannelJoinPrepareControl{}, fmt.Errorf(
			"%w: managed runtime returned invalid enrollment addresses", ErrChannelAuthority)
	}
	control.Descriptor = descriptor
	control.LocalPublicKey = append([]byte(nil), control.LocalPublicKey...)
	control.AdvertisedMultiaddrs = addresses
	return control, nil
}

func validPreparedChannelJoin(control peer.ChannelJoinPrepareControl,
	prepared store.PrepareJoinedChannelResult,
) bool {
	identity, err := model.EnrollmentJoinIdentityDigest(control.Descriptor.Descriptor().ID(),
		control.GrantID, control.AuthenticatedLocalPeerID, control.LocalPublicKey,
		prepared.OriginEpoch)
	if err != nil {
		return false
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	return err == nil && requestID == prepared.RequestID && !prepared.OriginEpoch.IsZero() &&
		((prepared.Reserved && prepared.Attempt > 0) ||
			(!prepared.Reserved && prepared.Attempt == 0 && !prepared.CommitUnknown))
}

func mapChannelJoinBeginError(err error) error {
	var code peer.ChannelProtocolErrorCode
	switch {
	case errors.Is(err, store.ErrNodeChannelLimit):
		code = peer.ChannelErrorNodeChannelLimit
	case errors.Is(err, store.ErrChannelJoinConflict):
		code = peer.ChannelErrorRosterConflict
	default:
		return fmt.Errorf("%w: prepare joined Channel: %w", ErrChannelAuthority, err)
	}
	failure, failureErr := peer.NewChannelJoinControlFailure(code)
	if failureErr != nil {
		return fmt.Errorf("%w: project joined Channel rejection: %w",
			ErrChannelAuthority, failureErr)
	}
	return failure
}

func (session *channelAuthorityJoinSession) startMark() (
	peer.ChannelJoinPrepareControl, store.PrepareJoinedChannelResult, bool,
) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.phase != channelJoinSessionPrepared || !session.prepared.Reserved {
		return peer.ChannelJoinPrepareControl{}, store.PrepareJoinedChannelResult{}, false
	}
	session.phase = channelJoinSessionMarking
	return session.control, session.prepared, true
}

func (session *channelAuthorityJoinSession) startTerminal(release bool) (
	peer.ChannelJoinPrepareControl, store.PrepareJoinedChannelResult, bool,
) {
	session.mu.Lock()
	defer session.mu.Unlock()
	allowed := release && session.prepared.Reserved &&
		(session.phase == channelJoinSessionPrepared || session.phase == channelJoinSessionMarked)
	allowed = allowed || !release && (session.phase == channelJoinSessionMarked ||
		session.phase == channelJoinSessionPrepared && !session.prepared.Reserved)
	if !allowed {
		return peer.ChannelJoinPrepareControl{}, store.PrepareJoinedChannelResult{}, false
	}
	session.phase = channelJoinSessionFinished
	return session.control, session.prepared, true
}

func (session *channelAuthorityJoinSession) move(from, to channelJoinSessionPhase) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.phase != from {
		return false
	}
	session.phase = to
	return true
}

func (session *channelAuthorityJoinSession) finish() {
	session.mu.Lock()
	session.phase = channelJoinSessionFinished
	session.mu.Unlock()
}

func (session *channelAuthorityJoinSession) finished() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.phase == channelJoinSessionFinished
}

func (session *channelAuthorityJoinSession) orderError(operation string) error {
	return fmt.Errorf("%w: joined Channel session cannot %s in its current phase",
		ErrChannelAuthority, operation)
}
