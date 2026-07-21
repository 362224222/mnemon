package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelMemberProtocol         = errors.New("Mnemon Channel member protocol failed")
	ErrChannelMemberNotMember        = errors.New("Channel peer is not a member")
	ErrChannelMemberRevoked          = errors.New("Channel peer membership is terminal")
	ErrChannelMemberClosed           = errors.New("Channel is closed")
	ErrChannelMemberRosterGap        = errors.New("Channel roster has a gap")
	ErrChannelMemberRosterConflict   = errors.New("Channel roster conflicts with durable authority")
	ErrChannelMemberBaselineConflict = errors.New("Channel baseline conflicts with durable authority")
	ErrChannelMemberEpochMismatch    = errors.New("Channel member origin epoch does not match signed authority")
	ErrChannelMemberBusy             = errors.New("Channel member controller is busy")
)

const (
	channelMemberBusyRetry = time.Second
	channelMemberGapRetry  = 250 * time.Millisecond
)

// ChannelMemberHelloControl is the authenticated input to one MemberHello
// reconciliation. AuthenticatedPeerID always comes from the secure stream;
// ActiveMemberRecord.PeerID is evidence to verify, never an identity source.
type ChannelMemberHelloControl struct {
	AuthenticatedPeerID model.PeerID
	ChannelID           model.ChannelID
	ActiveMemberRecord  model.Member
	KnownRosterHead     model.RecordHead
	ProofRecords        []model.Member
	At                  time.Time
}

type ChannelMemberHelloAuthority struct {
	Roster model.VerifiedRoster
}

type ChannelMemberSyncControl struct {
	AuthenticatedPeerID model.PeerID
	ChannelID           model.ChannelID
	AfterHead           model.RecordHead
	At                  time.Time
}

// ChannelMemberRosterSnapshot must be one coherent, immutable roster
// generation. The service paginates only this value, so a concurrent durable
// append cannot change roster_head between pages.
type ChannelMemberRosterSnapshot struct {
	Roster model.VerifiedRoster
}

type ChannelMemberBaselineControl struct {
	AuthenticatedPeerID model.PeerID
	Baseline            DataBaselineSpec
	At                  time.Time
}

type ChannelMemberBaselineAuthority struct {
	Baseline DataBaselineSpec
	Roster   model.VerifiedRoster
}

// ChannelMemberController is the narrow control/runtime boundary used by the
// wire service. Its method names intentionally do not mirror Store methods, so
// *store.Store cannot accidentally satisfy this interface without the runtime
// authority half of each operation.
//
// ReconcileMemberHelloGate must hold the affected Channel gate while it
// durably merges any proof records and installs the resulting whole runtime
// authority. It may return only after both are complete; exact replay is a
// read-only success.
//
// InstallMemberBaselineGate has the same ordering requirement: the inbound
// cursor and binding activation must be durable and the resulting runtime
// authority visible before it returns. ChannelMemberService writes an ACK only
// after that return.
type ChannelMemberController interface {
	ReconcileMemberHelloGate(context.Context,
		ChannelMemberHelloControl,
	) (ChannelMemberHelloAuthority, error)
	FreezeMemberRosterForSync(context.Context,
		ChannelMemberSyncControl,
	) (ChannelMemberRosterSnapshot, error)
	InstallMemberBaselineGate(context.Context,
		ChannelMemberBaselineControl,
	) (ChannelMemberBaselineAuthority, error)
}

type ChannelMemberClock interface {
	Now() time.Time
}

type wallChannelMemberClock struct{}

func (wallChannelMemberClock) Now() time.Time { return time.Now() }

type ChannelMemberServiceOptions struct {
	Controller ChannelMemberController
	Clock      ChannelMemberClock
}

// ChannelMemberService owns only member control exchanges. ChannelDispatcher
// retains the stream deadline, first-frame reservation and Close/Reset policy.
type ChannelMemberService struct {
	controller ChannelMemberController
	clock      ChannelMemberClock
}

var _ ChannelRequestHandler = (*ChannelMemberService)(nil)

func NewChannelMemberService(options ChannelMemberServiceOptions) (*ChannelMemberService, error) {
	if isNilChannelMemberController(options.Controller) {
		return nil, fmt.Errorf("%w: controller is required", ErrChannelMemberProtocol)
	}
	if options.Clock == nil {
		options.Clock = wallChannelMemberClock{}
	}
	return &ChannelMemberService{controller: options.Controller, clock: options.Clock}, nil
}

func (service *ChannelMemberService) handleHello(ctx context.Context, stream network.Stream,
	requestID ChannelRequestID, remotePeerID model.PeerID, remotePublicKey []byte,
	hello MemberHello, at time.Time,
) error {
	active := hello.ActiveMemberRecord()
	if active.PeerID() != remotePeerID || !bytes.Equal(active.PublicKey(), remotePublicKey) {
		return ErrChannelMemberProtocol
	}
	proof := hello.OwnerSignedProofChain()
	result, err := service.controller.ReconcileMemberHelloGate(ctx, ChannelMemberHelloControl{
		AuthenticatedPeerID: remotePeerID, ChannelID: hello.ChannelID(),
		ActiveMemberRecord: active, KnownRosterHead: hello.KnownRosterHead(),
		ProofRecords: proof, At: at,
	})
	if err != nil {
		return service.respondControllerFailure(stream, requestID, err)
	}
	terminalAck, terminal, err := terminalMemberHelloAcknowledgement(result.Roster,
		hello, remotePeerID, remotePublicKey)
	if err != nil {
		return err
	}
	if terminal {
		return writeChannelMemberFrame(stream, requestID, terminalAck)
	}
	if code, err := validateChannelMemberAuthority(result.Roster, hello.ChannelID(),
		remotePeerID, remotePublicKey); err != nil {
		return err
	} else if code != "" {
		return service.writeFailure(stream, requestID, code, 0)
	}
	members := result.Roster.Members()
	activeIndex := active.Head().Revision() - 1
	if activeIndex >= uint64(len(members)) || !sameMember(members[activeIndex], active) {
		return fmt.Errorf("%w: controller admitted unverified hello member", ErrChannelMemberProtocol)
	}
	current, _ := result.Roster.CurrentMember(remotePeerID)
	if current.OriginEpoch() != active.OriginEpoch() ||
		!bytes.Equal(current.PublicKey(), active.PublicKey()) {
		return fmt.Errorf("%w: hello identity changed under signed authority", ErrChannelMemberProtocol)
	}
	missing, code := channelMemberMissingSuffix(result.Roster, hello.KnownRosterHead())
	if code != "" {
		return service.writeFailure(stream, requestID, code, channelMemberRetryAfter(code))
	}
	ack, err := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: hello.ChannelID(),
		MissingRecords: missing, RosterHead: result.Roster.Head()})
	if err != nil {
		return fmt.Errorf("%w: construct hello acknowledgement: %v", ErrChannelMemberProtocol, err)
	}
	return writeChannelMemberFrame(stream, requestID, ack)
}

func (service *ChannelMemberService) handleSync(ctx context.Context, stream network.Stream,
	requestID ChannelRequestID, remotePeerID model.PeerID, remotePublicKey []byte,
	request SyncRequest, at time.Time,
) error {
	result, err := service.controller.FreezeMemberRosterForSync(ctx, ChannelMemberSyncControl{
		AuthenticatedPeerID: remotePeerID, ChannelID: request.ChannelID(),
		AfterHead: request.AfterHead(), At: at,
	})
	if err != nil {
		return service.respondControllerFailure(stream, requestID, err)
	}
	if code, err := validateChannelMemberAuthority(result.Roster, request.ChannelID(),
		remotePeerID, remotePublicKey); err != nil {
		return err
	} else if code != "" {
		return service.writeFailure(stream, requestID, code, 0)
	}
	members := result.Roster.Members()
	after := request.AfterHead()
	if after.Revision() > uint64(len(members)) {
		return service.writeFailure(stream, requestID, ChannelErrorRosterGap,
			channelMemberGapRetry)
	}
	if members[after.Revision()-1].Head() != after {
		return service.writeFailure(stream, requestID, ChannelErrorRosterConflict, 0)
	}
	remaining := members[after.Revision():]
	if len(remaining) == 0 {
		page, err := NewSyncPage(SyncPageSpec{ChannelID: request.ChannelID(),
			OwnerSignedRecords: []model.Member{}, RosterHead: result.Roster.Head()})
		if err != nil {
			return fmt.Errorf("%w: construct empty sync page: %v", ErrChannelMemberProtocol, err)
		}
		return writeChannelMemberFrame(stream, requestID, page)
	}
	for offset := 0; offset < len(remaining); offset += channelSyncPageRecordLimit {
		end := offset + channelSyncPageRecordLimit
		if end > len(remaining) {
			end = len(remaining)
		}
		more := end < len(remaining)
		page, err := NewSyncPage(SyncPageSpec{ChannelID: request.ChannelID(), More: more,
			OwnerSignedRecords: remaining[offset:end], RosterHead: result.Roster.Head()})
		if err != nil {
			return fmt.Errorf("%w: construct sync page: %v", ErrChannelMemberProtocol, err)
		}
		if err := writeChannelMemberFrame(stream, requestID, page); err != nil {
			return err
		}
	}
	return nil
}

func (service *ChannelMemberService) handleBaseline(ctx context.Context, stream network.Stream,
	requestID ChannelRequestID, remotePeerID model.PeerID, remotePublicKey []byte,
	baseline DataBaseline, at time.Time,
) error {
	if baseline.OriginPeerID() != remotePeerID {
		return ErrChannelMemberProtocol
	}
	spec := DataBaselineSpec{ChannelID: baseline.ChannelID(), OriginPeerID: baseline.OriginPeerID(),
		OriginEpoch:             baseline.OriginEpoch(),
		BaselineChannelSequence: baseline.BaselineChannelSequence()}
	result, err := service.controller.InstallMemberBaselineGate(ctx, ChannelMemberBaselineControl{
		AuthenticatedPeerID: remotePeerID, Baseline: spec, At: at,
	})
	if err != nil {
		return service.respondControllerFailure(stream, requestID, err)
	}
	if result.Baseline != spec {
		return fmt.Errorf("%w: controller changed immutable baseline", ErrChannelMemberProtocol)
	}
	if code, err := validateChannelMemberAuthority(result.Roster, baseline.ChannelID(),
		remotePeerID, remotePublicKey); err != nil {
		return err
	} else if code != "" {
		return service.writeFailure(stream, requestID, code, 0)
	}
	member, _ := result.Roster.CurrentMember(remotePeerID)
	if member.OriginEpoch() != baseline.OriginEpoch() {
		return service.writeFailure(stream, requestID, ChannelErrorOriginEpochMismatch, 0)
	}
	ack, err := NewDataBaselineAck(spec)
	if err != nil {
		return fmt.Errorf("%w: construct baseline acknowledgement: %v", ErrChannelMemberProtocol, err)
	}
	return writeChannelMemberFrame(stream, requestID, ack)
}

func (service *ChannelMemberService) respondControllerFailure(stream network.Stream,
	requestID ChannelRequestID, cause error,
) error {
	code, retryAfter, ok := channelMemberControllerFailure(cause)
	if !ok {
		return fmt.Errorf("%w: controller: %v", ErrChannelMemberProtocol, cause)
	}
	return service.writeFailure(stream, requestID, code, retryAfter)
}

func (service *ChannelMemberService) writeFailure(stream network.Stream,
	requestID ChannelRequestID, code ChannelProtocolErrorCode, retryAfter time.Duration,
) error {
	payload, err := NewProtocolError(ProtocolErrorSpec{Code: code,
		Retryable: code.retryable(), RetryAfter: retryAfter})
	if err != nil {
		return fmt.Errorf("%w: construct stable failure: %v", ErrChannelMemberProtocol, err)
	}
	return writeChannelMemberFrame(stream, requestID, payload)
}

func writeChannelMemberFrame(stream network.Stream, requestID ChannelRequestID,
	payload ChannelFramePayload,
) error {
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		return fmt.Errorf("%w: construct response frame: %v", ErrChannelMemberProtocol, err)
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		return fmt.Errorf("%w: write response frame: %v", ErrChannelMemberProtocol, err)
	}
	return nil
}

func validateChannelMemberAuthority(roster model.VerifiedRoster, channelID model.ChannelID,
	remotePeerID model.PeerID, remotePublicKey []byte,
) (ChannelProtocolErrorCode, error) {
	if roster.IsZero() || channelID.IsZero() || remotePeerID.IsZero() || len(remotePublicKey) != 32 ||
		roster.Descriptor().Descriptor().ID() != channelID {
		return "", fmt.Errorf("%w: controller returned invalid roster authority", ErrChannelMemberProtocol)
	}
	ownerPeerID := roster.Descriptor().Descriptor().OwnerPeerID()
	owner, ownerOK := roster.CurrentMember(ownerPeerID)
	if !ownerOK {
		return "", fmt.Errorf("%w: controller roster has no owner", ErrChannelMemberProtocol)
	}
	if owner.Status().Terminal() {
		return ChannelErrorChannelClosed, nil
	}
	member, memberOK := roster.CurrentMember(remotePeerID)
	if !memberOK {
		return ChannelErrorNotMember, nil
	}
	if member.Status().Terminal() {
		return ChannelErrorMemberRevoked, nil
	}
	if member.Status() != model.MemberActive || !bytes.Equal(member.PublicKey(), remotePublicKey) {
		return "", fmt.Errorf("%w: controller roster mismatches secure peer", ErrChannelMemberProtocol)
	}
	return "", nil
}

func channelMemberMissingSuffix(roster model.VerifiedRoster,
	knownHead model.RecordHead,
) ([]model.Member, ChannelProtocolErrorCode) {
	members := roster.Members()
	if knownHead.IsZero() {
		return nil, ChannelErrorRosterConflict
	}
	if knownHead.Revision() > uint64(len(members)) {
		return nil, ChannelErrorRosterGap
	}
	if members[knownHead.Revision()-1].Head() != knownHead {
		return nil, ChannelErrorRosterConflict
	}
	return append([]model.Member{}, members[knownHead.Revision():]...), ""
}

func channelMemberControllerFailure(cause error) (ChannelProtocolErrorCode, time.Duration, bool) {
	switch {
	case errors.Is(cause, ErrChannelMemberBusy):
		return ChannelErrorBusy, channelMemberBusyRetry, true
	case errors.Is(cause, ErrChannelMemberNotMember):
		return ChannelErrorNotMember, 0, true
	case errors.Is(cause, ErrChannelMemberRevoked):
		return ChannelErrorMemberRevoked, 0, true
	case errors.Is(cause, ErrChannelMemberClosed):
		return ChannelErrorChannelClosed, 0, true
	case errors.Is(cause, ErrChannelMemberRosterGap):
		return ChannelErrorRosterGap, channelMemberGapRetry, true
	case errors.Is(cause, ErrChannelMemberRosterConflict):
		return ChannelErrorRosterConflict, 0, true
	case errors.Is(cause, ErrChannelMemberBaselineConflict):
		return ChannelErrorBaselineConflict, 0, true
	case errors.Is(cause, ErrChannelMemberEpochMismatch):
		return ChannelErrorOriginEpochMismatch, 0, true
	default:
		return "", 0, false
	}
}

func channelMemberRetryAfter(code ChannelProtocolErrorCode) time.Duration {
	if code == ChannelErrorRosterGap {
		return channelMemberGapRetry
	}
	return 0
}

func isNilChannelMemberController(controller ChannelMemberController) bool {
	if controller == nil {
		return true
	}
	value := reflect.ValueOf(controller)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
