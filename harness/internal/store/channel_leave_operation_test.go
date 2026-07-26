package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestOwnerChannelLeaveOperationCommitsWithTerminalRosterAndReplays(t *testing.T) {
	t.Parallel()
	fixture, accepted := acceptedOwnerLeaveFixture(t, "owner-leave-operation")
	owner, ok := accepted.Roster.CurrentMember(fixture.channel.Owner().PeerID())
	if !ok {
		t.Fatal("accepted owner Channel has no owner")
	}
	at := fixture.acceptedAt.Add(time.Second)
	previous := accepted.Roster.Head().Digest()
	terminal, _ := signAndAppendRosterMember(t, fixture.channel.Descriptor(), fixture.signer,
		accepted.Roster, model.MemberRecordSpec{ChannelID: fixture.channel.Channel().ID(),
			DescriptorDigest: fixture.channel.Descriptor().Descriptor().Digest(),
			Revision:         accepted.Roster.Head().Revision() + 1, PreviousDigest: &previous,
			PeerID: owner.PeerID(), OriginEpoch: owner.OriginEpoch(),
			DisplayLabel: owner.DisplayLabel(), PublicKey: owner.PublicKey(),
			Multiaddrs: owner.Multiaddrs(), Protocols: owner.Protocols(), Limits: owner.Limits(),
			Status: model.MemberLeft, CreatedAt: at})
	operation := testChannelLeaveOperation("owner-terminal")
	result, err := fixture.ownerStore.MergeChannelRoster(context.Background(),
		MergeChannelRosterSpec{ChannelID: fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
			Records:                      []model.Member{terminal}, LeaveOperation: &operation, At: at})
	if err != nil || result.Status != ChannelRosterApplied ||
		result.Channel.Status() != model.ChannelClosed {
		t.Fatalf("owner terminal operation = (%#v,%v)", result, err)
	}
	replay, found, err := fixture.ownerStore.ReadChannelLeaveOperation(
		context.Background(), operation)
	if err != nil || !found || replay.ChannelID() != result.Channel.ID() {
		t.Fatalf("owner operation replay = (%#v,%t,%v)", replay, found, err)
	}
	changed := operation
	changed.RequestDigest = model.Sum([]byte("changed owner leave request"))
	if _, _, err := fixture.ownerStore.ReadChannelLeaveOperation(context.Background(), changed); !errors.Is(err, ErrChannelLeaveOperationMismatch) {
		t.Fatalf("changed owner operation error = %v", err)
	}
}
