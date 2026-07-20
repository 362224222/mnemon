package peer

import (
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberClientValidationBindsCorrelatedResponses(t *testing.T) {
	fixture := newChannelFrameFixture(t)
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channelID,
		ActiveMemberRecord: fixture.joiningMember, KnownRosterHead: fixture.joiningMember.Head(),
		OwnerSignedProofChain: fixture.roster})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: fixture.channelID,
		RosterHead: fixture.joiningMember.Head()})
	if err != nil || !validMemberHelloAck(hello, ack) {
		t.Fatalf("exact hello ACK = (%#v, %v)", ack, err)
	}
	hiddenHead, _ := model.NewRecordHead(fixture.joiningMember.Head().Revision()+1,
		model.Sum([]byte("hidden-member-record")))
	hidden, err := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: fixture.channelID,
		RosterHead: hiddenHead})
	if err != nil || validMemberHelloAck(hello, hidden) {
		t.Fatalf("omitted hello suffix accepted = (%#v, %v)", hidden, err)
	}

	response, _ := NewChannelFrame(fixture.frameRequestID, ack)
	if failure, err := channelMemberResponseFailure(response, fixture.frameRequestID); failure != nil || err != nil {
		t.Fatalf("correlated success = (%#v, %v)", failure, err)
	}
	wrongRequestID := channelMemberRequestID(t, 0x71)
	if failure, err := channelMemberResponseFailure(response, wrongRequestID); failure != nil ||
		!errors.Is(err, ErrChannelMemberClientResponse) {
		t.Fatalf("wrong request identity = (%#v, %v)", failure, err)
	}
}

func TestChannelMemberClientValidationReturnsClosedRemoteFailures(t *testing.T) {
	fixture := newChannelFrameFixture(t)
	busy, _ := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBusy,
		Retryable: true, RetryAfter: time.Second})
	busyFrame, _ := NewChannelFrame(fixture.frameRequestID, busy)
	failure, err := channelMemberResponseFailure(busyFrame, fixture.frameRequestID)
	if err != nil || failure == nil || !errors.Is(failure, ErrChannelMemberClient) ||
		failure.Code() != ChannelErrorBusy || !failure.Retryable() ||
		failure.RetryAfter() != time.Second {
		t.Fatalf("closed remote failure = (%#v, %v)", failure, err)
	}
	var nilFailure *ChannelMemberRemoteFailure
	if nilFailure.Code() != "" || nilFailure.Retryable() || nilFailure.RetryAfter() != 0 ||
		nilFailure.Error() != ErrChannelMemberClient.Error() {
		t.Fatal("nil remote failure did not preserve its closed API")
	}
}

func TestChannelMemberClientValidationRejectsForeignOrUnboundedFailures(t *testing.T) {
	fixture := newChannelFrameFixture(t)
	badProof, _ := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBadProof})
	unbounded, _ := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBusy,
		Retryable: true, RetryAfter: HermeticLimits().ChannelRequestTimeout + time.Millisecond})
	for name, payload := range map[string]ProtocolError{
		"enrollment code": badProof,
		"unbounded retry": unbounded,
	} {
		t.Run(name, func(t *testing.T) {
			frame, _ := NewChannelFrame(fixture.frameRequestID, payload)
			failure, err := channelMemberResponseFailure(frame, fixture.frameRequestID)
			if failure != nil || !errors.Is(err, ErrChannelMemberClientResponse) {
				t.Fatalf("invalid remote failure = (%#v, %v)", failure, err)
			}
		})
	}
}

func TestChannelMemberClientValidationBindsBaselineTuple(t *testing.T) {
	fixture := newChannelFrameFixture(t)
	baseline, _ := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channelID,
		OriginPeerID: fixture.joiner.modelID, OriginEpoch: fixture.joinerEpoch,
		BaselineChannelSequence: 9})
	baselineAck, _ := NewDataBaselineAck(DataBaselineSpec{ChannelID: fixture.channelID,
		OriginPeerID: fixture.joiner.modelID, OriginEpoch: fixture.joinerEpoch,
		BaselineChannelSequence: 9})
	changedAck, _ := NewDataBaselineAck(DataBaselineSpec{ChannelID: fixture.channelID,
		OriginPeerID: fixture.joiner.modelID, OriginEpoch: fixture.joinerEpoch,
		BaselineChannelSequence: 10})
	if !sameDataBaseline(baseline, baselineAck) || sameDataBaseline(baseline, changedAck) {
		t.Fatal("baseline ACK tuple was not bound exactly")
	}
}

func TestChannelMemberClientValidationAggregatesFrozenSyncPages(t *testing.T) {
	channelID, after, records, head := newChannelMemberClientSyncFixture(t)
	first, err := NewSyncPage(SyncPageSpec{ChannelID: channelID, More: true,
		OwnerSignedRecords: records[:channelSyncPageRecordLimit], RosterHead: head})
	if err != nil {
		t.Fatal(err)
	}
	last, err := NewSyncPage(SyncPageSpec{ChannelID: channelID,
		OwnerSignedRecords: records[channelSyncPageRecordLimit:], RosterHead: head})
	if err != nil {
		t.Fatal(err)
	}
	state := channelMemberSyncState{channelID: channelID, cursor: after}
	if !state.append(first) || !state.append(last) {
		t.Fatal("valid frozen multi-page suffix was rejected")
	}
	result := state.result()
	resultRecords := result.OwnerSignedRecords()
	if result.IsZero() || result.ChannelID() != channelID || result.RosterHead() != head ||
		len(resultRecords) != len(records) || resultRecords[0].Head().Revision() != 2 {
		t.Fatalf("sync result = %#v", result)
	}
	resultRecords[0] = model.Member{}
	if result.OwnerSignedRecords()[0].IsZero() {
		t.Fatal("sync result exposed a mutable record slice")
	}
}

func TestChannelMemberClientValidationRejectsNonDeterministicSyncPages(t *testing.T) {
	channelID, after, records, head := newChannelMemberClientSyncFixture(t)
	first, _ := NewSyncPage(SyncPageSpec{ChannelID: channelID, More: true,
		OwnerSignedRecords: records[:channelSyncPageRecordLimit], RosterHead: head})
	last, _ := NewSyncPage(SyncPageSpec{ChannelID: channelID,
		OwnerSignedRecords: records[channelSyncPageRecordLimit:], RosterHead: head})
	short, _ := NewSyncPage(SyncPageSpec{ChannelID: channelID, More: true,
		OwnerSignedRecords: records[:1], RosterHead: head})
	shortState := channelMemberSyncState{channelID: channelID, cursor: after}
	if shortState.append(short) {
		t.Fatal("short nonterminal page was accepted")
	}

	intermediateHead := records[channelSyncPageRecordLimit].Head()
	changingFirst, _ := NewSyncPage(SyncPageSpec{ChannelID: channelID, More: true,
		OwnerSignedRecords: records[:channelSyncPageRecordLimit], RosterHead: intermediateHead})
	changingState := channelMemberSyncState{channelID: channelID, cursor: after}
	if !changingState.append(changingFirst) || changingState.append(last) {
		t.Fatal("roster head changed across one frozen Sync stream")
	}

	gappedLast, _ := NewSyncPage(SyncPageSpec{ChannelID: channelID,
		OwnerSignedRecords: records[channelSyncPageRecordLimit+1:], RosterHead: head})
	gappedState := channelMemberSyncState{channelID: channelID, cursor: after}
	if !gappedState.append(first) || gappedState.append(gappedLast) {
		t.Fatal("cross-page roster gap was accepted")
	}

	empty, _ := NewSyncPage(SyncPageSpec{ChannelID: channelID, RosterHead: head})
	emptyState := channelMemberSyncState{channelID: channelID, cursor: head}
	if !emptyState.append(empty) || len(emptyState.result().OwnerSignedRecords()) != 0 {
		t.Fatal("bounded no-op Sync was rejected")
	}
	boundedState := channelMemberSyncState{channelID: channelID, cursor: head,
		pages: channelMemberSyncPageLimit}
	if channelMemberSyncPageLimit != 4 || boundedState.append(empty) {
		t.Fatal("Sync page count is not bounded by the 64-record roster")
	}
}

func newChannelMemberClientSyncFixture(t testing.TB) (model.ChannelID, model.RecordHead,
	[]model.Member, model.RecordHead,
) {
	t.Helper()
	owner := testkit.NewIdentity(t, "member-client-pages-owner")
	channel := testkit.NewSignedChannelForOwnerAt(t, "member-client-pages", owner,
		time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC))
	remote := channel.AppendActive(t, "member-client-pages-remote")
	for revision := 3; revision <= 19; revision++ {
		channel.AppendActiveUpdate(t, remote.Identity().PeerID())
	}
	roster := channel.Roster().Members()
	return channel.Channel().ID(), channel.OwnerMember().Member().Head(), roster[1:],
		roster[len(roster)-1].Head()
}
