package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelRuntimeAuthorityAppliesSuffixAndReadsOnlyActiveRemote(t *testing.T) {
	fixture := newRealChannelAuthorityCoordinatorFixture(t)
	channelID := fixture.channel.Channel().ID()
	initial, err := fixture.runtime.Session(channelID)
	if err != nil {
		t.Fatal(err)
	}
	update := ChannelRuntimeRosterUpdate{ChannelID: channelID,
		AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
		Records:             fixture.channel.Roster().Members(), At: fixture.remote.Member().CreatedAt()}
	applied, err := fixture.controller.ReconcileRemoteRoster(context.Background(), update)
	if err != nil || applied.Head() != fixture.channel.Roster().Head() || initial.IsCurrent() {
		t.Fatalf("ReconcileRemoteRoster(suffix) = (%#v, %v), old current=%t",
			applied, err, initial.IsCurrent())
	}
	assertRealChannelMemberBinding(t, fixture.store, channelID, model.BindingPending, false)

	current, err := fixture.controller.ReconcileRemoteRoster(context.Background(),
		ChannelRuntimeRosterUpdate{ChannelID: channelID,
			AuthenticatedPeerID: fixture.remote.Identity().PeerID()})
	if err != nil || current.Head() != applied.Head() {
		t.Fatalf("ReconcileRemoteRoster(empty) = (%#v, %v)", current, err)
	}
	unknown := testkit.NewIdentity(t, "channel-runtime-authority-unknown")
	if _, err := fixture.controller.ReconcileRemoteRoster(context.Background(),
		(ChannelRuntimeRosterUpdate{ChannelID: channelID,
			AuthenticatedPeerID: unknown.PeerID()})); !errors.Is(err, peer.ErrChannelMemberNotMember) {
		t.Fatalf("ReconcileRemoteRoster(empty unknown) error = %v", err)
	}
}

func TestChannelRuntimeAuthorityMapsRosterGapAndConflict(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		fixture := newRealChannelAuthorityCoordinatorFixture(t)
		applyChannelRuntimeFixtureRemote(t, fixture)
		fixture.channel.AppendActive(t, "channel-runtime-authority-gap-prefix")
		gap := fixture.channel.AppendActive(t, "channel-runtime-authority-gap")
		_, err := fixture.controller.ReconcileRemoteRoster(context.Background(),
			ChannelRuntimeRosterUpdate{ChannelID: fixture.channel.Channel().ID(),
				AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
				Records:             []model.Member{gap.Member()}, At: gap.Member().CreatedAt()})
		if !errors.Is(err, peer.ErrChannelMemberRosterGap) {
			t.Fatalf("ReconcileRemoteRoster(gap) error = %v", err)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		fixture := newRealChannelAuthorityCoordinatorFixture(t)
		applyChannelRuntimeFixtureRemote(t, fixture)
		branch := testkit.NewSignedChannelForOwnerAt(t, "node-channel-member",
			fixture.channel.Owner(), fixture.at)
		challenger := branch.AppendActive(t, "channel-runtime-authority-conflict")
		_, err := fixture.controller.ReconcileRemoteRoster(context.Background(),
			ChannelRuntimeRosterUpdate{ChannelID: fixture.channel.Channel().ID(),
				AuthenticatedPeerID: challenger.Identity().PeerID(),
				Records:             []model.Member{challenger.Member()}, At: fixture.at.Add(time.Second)})
		if !errors.Is(err, peer.ErrChannelMemberRosterConflict) {
			t.Fatalf("ReconcileRemoteRoster(conflict) error = %v", err)
		}
	})
}

func TestChannelRuntimeAuthorityValidatesInputAndSharesMutationToken(t *testing.T) {
	fixture := newRealChannelAuthorityCoordinatorFixture(t)
	channelID := fixture.channel.Channel().ID()
	remoteID := fixture.remote.Identity().PeerID()
	for name, test := range map[string]struct {
		ctx    context.Context
		update ChannelRuntimeRosterUpdate
	}{
		"nil context": {update: ChannelRuntimeRosterUpdate{ChannelID: channelID,
			AuthenticatedPeerID: remoteID}},
		"zero Channel": {ctx: context.Background(),
			update: ChannelRuntimeRosterUpdate{AuthenticatedPeerID: remoteID}},
		"zero remote": {ctx: context.Background(),
			update: ChannelRuntimeRosterUpdate{ChannelID: channelID}},
		"zero mutation time": {ctx: context.Background(), update: ChannelRuntimeRosterUpdate{
			ChannelID: channelID, AuthenticatedPeerID: remoteID,
			Records: fixture.channel.Roster().Members()}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fixture.controller.ReconcileRemoteRoster(test.ctx,
				test.update); !errors.Is(err, ErrChannelAuthority) {
				t.Fatalf("ReconcileRemoteRoster() error = %v", err)
			}
		})
	}

	release, err := fixture.controller.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	waitCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, waitErr := fixture.controller.ReconcileRemoteRoster(waitCtx,
			ChannelRuntimeRosterUpdate{ChannelID: channelID,
				AuthenticatedPeerID: fixture.channel.Owner().PeerID()})
		done <- waitErr
	}()
	select {
	case err := <-done:
		t.Fatalf("remote roster reconciliation bypassed authority token: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrChannelAuthority) {
		t.Fatalf("canceled remote roster authority wait error = %v", err)
	}
}

func applyChannelRuntimeFixtureRemote(t *testing.T,
	fixture realChannelAuthorityCoordinatorFixture,
) {
	t.Helper()
	_, err := fixture.controller.ReconcileRemoteRoster(context.Background(),
		ChannelRuntimeRosterUpdate{ChannelID: fixture.channel.Channel().ID(),
			AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Records:             fixture.channel.Roster().Members(), At: fixture.remote.Member().CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
}
