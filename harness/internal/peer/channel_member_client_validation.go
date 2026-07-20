package peer

import (
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const channelMemberSyncPageLimit = (model.MaxMemberRecordsPerChannel + channelSyncPageRecordLimit - 1) /
	channelSyncPageRecordLimit

// ChannelMemberRemoteFailure is the closed, diagnostic-free failure returned
// by an authenticated Channel member. Only member-control codes are admitted.
type ChannelMemberRemoteFailure struct {
	code       ChannelProtocolErrorCode
	retryable  bool
	retryAfter time.Duration
}

func (failure *ChannelMemberRemoteFailure) Error() string {
	if failure == nil || !failure.code.Valid() {
		return ErrChannelMemberClient.Error()
	}
	return fmt.Sprintf("%s: %s", ErrChannelMemberClient, failure.code)
}
func (failure *ChannelMemberRemoteFailure) Unwrap() error { return ErrChannelMemberClient }
func (failure *ChannelMemberRemoteFailure) Code() ChannelProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}
func (failure *ChannelMemberRemoteFailure) Retryable() bool {
	return failure != nil && failure.retryable
}
func (failure *ChannelMemberRemoteFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.retryAfter
}

// ChannelMemberSyncResult is one validated suffix from a single immutable
// remote roster generation. Page boundaries are transport details and are not
// exposed to the later durable reconciler.
type ChannelMemberSyncResult struct {
	channelID          model.ChannelID
	ownerSignedRecords []model.Member
	rosterHead         model.RecordHead
}

func (result ChannelMemberSyncResult) ChannelID() model.ChannelID { return result.channelID }
func (result ChannelMemberSyncResult) OwnerSignedRecords() []model.Member {
	return append([]model.Member(nil), result.ownerSignedRecords...)
}
func (result ChannelMemberSyncResult) RosterHead() model.RecordHead { return result.rosterHead }
func (result ChannelMemberSyncResult) IsZero() bool {
	return result.channelID.IsZero() || result.rosterHead.IsZero()
}

func channelMemberResponseFailure(frame ChannelFrame,
	requestID ChannelRequestID,
) (*ChannelMemberRemoteFailure, error) {
	if frame.IsZero() || frame.RequestID() != requestID {
		return nil, ErrChannelMemberClientResponse
	}
	if frame.Type() != ChannelFrameProtocolError {
		return nil, nil
	}
	payload, ok := frame.Payload().(ProtocolError)
	if !ok || !validChannelMemberRemoteFailure(payload) {
		return nil, ErrChannelMemberClientResponse
	}
	return &ChannelMemberRemoteFailure{code: payload.Code(), retryable: payload.Retryable(),
		retryAfter: payload.RetryAfter()}, nil
}

func validChannelMemberRemoteFailure(failure ProtocolError) bool {
	switch failure.Code() {
	case ChannelErrorBusy, ChannelErrorNotMember, ChannelErrorMemberRevoked,
		ChannelErrorChannelClosed, ChannelErrorBaselineConflict,
		ChannelErrorOriginEpochMismatch, ChannelErrorRosterGap, ChannelErrorRosterConflict:
		return !failure.IsZero() && failure.Retryable() == failure.Code().retryable() &&
			failure.RetryAfter() <= HermeticLimits().ChannelRequestTimeout
	default:
		return false
	}
}

func validMemberHelloAck(request MemberHello, ack MemberHelloAck) bool {
	if request.IsZero() || ack.IsZero() || ack.ChannelID() != request.ChannelID() {
		return false
	}
	head, ok := advanceChannelMemberHead(request.ChannelID(), request.KnownRosterHead(),
		ack.MissingRecords())
	return ok && head == ack.RosterHead()
}

type channelMemberSyncState struct {
	channelID model.ChannelID
	cursor    model.RecordHead
	head      model.RecordHead
	records   []model.Member
	pages     int
}

func (state *channelMemberSyncState) append(page SyncPage) bool {
	if state == nil || page.IsZero() || page.ChannelID() != state.channelID ||
		state.pages >= channelMemberSyncPageLimit {
		return false
	}
	if state.pages == 0 {
		state.head = page.RosterHead()
		if state.head.Revision() < state.cursor.Revision() {
			return false
		}
	} else if page.RosterHead() != state.head {
		return false
	}
	records := page.OwnerSignedRecords()
	if len(records) > channelSyncPageRecordLimit || (page.More() && len(records) != channelSyncPageRecordLimit) {
		return false
	}
	next, ok := advanceChannelMemberHead(state.channelID, state.cursor, records)
	if !ok || (page.More() && next.Revision() >= state.head.Revision()) ||
		(!page.More() && next != state.head) {
		return false
	}
	state.cursor = next
	state.records = append(state.records, records...)
	state.pages++
	return true
}

func (state channelMemberSyncState) result() ChannelMemberSyncResult {
	return ChannelMemberSyncResult{channelID: state.channelID,
		ownerSignedRecords: append([]model.Member(nil), state.records...), rosterHead: state.head}
}

func advanceChannelMemberHead(channelID model.ChannelID, after model.RecordHead,
	records []model.Member,
) (model.RecordHead, bool) {
	if channelID.IsZero() || after.IsZero() || len(records) > model.MaxMemberRecordsPerChannel {
		return model.RecordHead{}, false
	}
	cursor := after
	for _, member := range records {
		previous, hasPrevious := member.PreviousDigest()
		if member.IsZero() || member.ChannelID() != channelID || !hasPrevious ||
			member.Head().Revision() != cursor.Revision()+1 || previous != cursor.Digest() {
			return model.RecordHead{}, false
		}
		cursor = member.Head()
	}
	return cursor, true
}

func sameDataBaseline(request DataBaseline, ack DataBaselineAck) bool {
	return !request.IsZero() && !ack.IsZero() && ack.ChannelID() == request.ChannelID() &&
		ack.OriginPeerID() == request.OriginPeerID() && ack.OriginEpoch() == request.OriginEpoch() &&
		ack.BaselineChannelSequence() == request.BaselineChannelSequence()
}
