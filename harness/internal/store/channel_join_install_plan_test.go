package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestJoinedChannelInstallPlanFreezesRollbackCandidateAndCommitsExactlyOnce(t *testing.T) {
	t.Parallel()
	_, joinerStore, spec := newJoinedChannelInstallFixture(t, "join-authority-plan", "joined-plan-team")
	plan, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil || plan.Result().Installed || !plan.ChangesAuthority() ||
		len(plan.Candidate().Channels()) != 1 {
		t.Fatalf("PrepareJoinedChannelInstall() = (%#v,%v)", plan.Result(), err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{"channels": 0,
		"channel_members": 0, "enrollment_receipts": 0, "publication_epochs": 0,
		"peer_bindings": 0, "channel_join_reservations": 1})
	spec.Members[0] = model.Member{}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanUnchanged {
		t.Fatalf("ResolveJoinedChannelInstall(before commit) = (%q,%v)", resolution, err)
	}
	other := openTestStore(t)
	if _, err := other.CommitJoinedChannelInstall(context.Background(), plan); !errors.Is(err,
		ErrChannelAuthorityPlan) {
		t.Fatalf("cross-Store commit error = %v", err)
	}
	installed, err := joinerStore.CommitJoinedChannelInstall(context.Background(), plan)
	if err != nil || !installed.Installed || installed.Status != ChannelEnrollmentAccepted {
		t.Fatalf("CommitJoinedChannelInstall() = (%#v,%v)", installed, err)
	}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveJoinedChannelInstall(after commit) = (%q,%v)", resolution, err)
	}
	replayed, err := joinerStore.CommitJoinedChannelInstall(context.Background(), plan)
	if err != nil || replayed.Installed || replayed.Status != ChannelEnrollmentAccepted {
		t.Fatalf("CommitJoinedChannelInstall(response-loss replay) = (%#v,%v)", replayed, err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{"channels": 1,
		"channel_members": 2, "enrollment_receipts": 1, "publication_epochs": 1,
		"peer_bindings": 1, "channel_join_reservations": 0, "enrollment_grants": 0,
		"enrollment_grant_uses": 0})
}

func TestJoinedChannelInstallPlanNoopReplayHasCurrentExactEvidence(t *testing.T) {
	t.Parallel()
	_, joinerStore, spec := newJoinedChannelInstallFixture(t,
		"join-authority-plan-replay", "joined-plan-team")
	initial, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	replay, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil || replay.ChangesAuthority() || replay.Result().Installed ||
		replay.Result().Status != ChannelEnrollmentReplayed {
		t.Fatalf("PrepareJoinedChannelInstall(replay) = (%#v,%v)", replay.Result(), err)
	}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), replay); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveJoinedChannelInstall(replay) = (%q,%v)", resolution, err)
	}
	result, err := joinerStore.CommitJoinedChannelInstall(context.Background(), replay)
	if err != nil || result.Installed || result.Status != ChannelEnrollmentReplayed {
		t.Fatalf("CommitJoinedChannelInstall(replay) = (%#v,%v)", result, err)
	}
}

func TestJoinedChannelInstallPlanFreezesExplicitOwnerOutcomeVerification(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-owner-outcome-plan")
	acceptedTranscript := owner.transcript(t, 0x65, 0x66, owner.head)
	accepted := owner.accept(t, acceptedTranscript)
	replayedTranscript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: owner.channel.Channel().ID(), GrantID: owner.grantID, RequestID: owner.requestID,
		OwnerPeerID: owner.channel.Owner().PeerID(), JoinerPeerID: owner.joiner.PeerID(),
		OwnerNonce: acceptedTranscript.OwnerNonce(), JoinerNonce: acceptedTranscript.JoinerNonce(),
		SelectedVersion: acceptedTranscript.SelectedVersion(), Limits: acceptedTranscript.Limits(),
		JoinerOriginEpoch: owner.joiner.OriginEpoch(), JoinerDisplayLabel: "renamed-after-response-loss",
		JoinerPublicKey:      owner.joiner.PublicKey(),
		AdvertisedMultiaddrs: []string{"/ip4/127.0.0.1/tcp/45555"}, RosterHead: owner.head,
	})
	if err != nil {
		t.Fatal(err)
	}
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	spec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		OwnerOutcome: ChannelEnrollmentAccepted, LocalAlias: "outcome-team",
		Descriptor: owner.channel.Descriptor(), Transcript: replayedTranscript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)}
	reserveJoinedChannelTest(t, joinerStore, spec)
	if _, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec); !errors.Is(err,
		ErrChannelJoinInput) {
		t.Fatalf("Accepted alternate transcript error = %v", err)
	}
	spec.OwnerOutcome = ChannelEnrollmentReplayed
	plan, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil {
		t.Fatalf("Replayed signed evidence prepare = %v", err)
	}
	result, err := joinerStore.CommitJoinedChannelInstall(context.Background(), plan)
	current, ok := result.Roster.CurrentMember(owner.joiner.PeerID())
	if err != nil || !result.Installed || !ok || current.DisplayLabel() != owner.joiner.DisplayName() {
		t.Fatalf("Replayed signed evidence commit = (%#v,%v)", result, err)
	}
}

func TestJoinedChannelInstallPlanTerminalOutcomeConsumesOnlyReservation(t *testing.T) {
	t.Parallel()
	for _, terminal := range []struct {
		name   string
		seed   string
		peer   func(channelEnrollmentFixture) model.PeerID
		status model.MemberStatus
		want   ChannelEnrollmentStatus
	}{
		{name: "owner closed", seed: "owner-closed", peer: func(f channelEnrollmentFixture) model.PeerID {
			return f.channel.Owner().PeerID()
		}, status: model.MemberLeft, want: ChannelEnrollmentChannelClosed},
		{name: "local revoked", seed: "local-revoked", peer: func(f channelEnrollmentFixture) model.PeerID {
			return f.joiner.PeerID()
		}, status: model.MemberRevoked, want: ChannelEnrollmentMemberRevoked},
	} {
		terminal := terminal
		t.Run(terminal.name, func(t *testing.T) {
			t.Parallel()
			owner := newChannelEnrollmentFixture(t, "join-terminal-plan-"+terminal.seed)
			transcript := owner.transcript(t, 0x61, 0x62, owner.head)
			accepted := owner.accept(t, transcript)
			at := owner.acceptedAt.Add(time.Second)
			_, roster := appendRosterTerminal(t, owner.channel.Descriptor(), owner.signer,
				accepted.Roster, terminal.peer(owner), terminal.status, at)
			joinerStore := openTestStore(t)
			insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
			spec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
				OwnerOutcome: terminal.want, LocalAlias: "terminal-team",
				Descriptor: owner.channel.Descriptor(), Transcript: transcript,
				Receipt: accepted.Receipt, Members: roster.Members(), At: at.Add(time.Second)}
			reserveJoinedChannelTest(t, joinerStore, spec)
			plan, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
			if err != nil || plan.ChangesAuthority() {
				t.Fatalf("Prepare terminal plan = (%#v,%v)", plan.Result(), err)
			}
			if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), plan); err != nil ||
				resolution != ChannelAuthorityPlanUnchanged {
				t.Fatalf("Resolve terminal before = (%q,%v)", resolution, err)
			}
			result, err := joinerStore.CommitJoinedChannelInstall(context.Background(), plan)
			if err != nil || result.Installed || result.Status != terminal.want {
				t.Fatalf("Commit terminal plan = (%#v,%v)", result, err)
			}
			if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), plan); err != nil ||
				resolution != ChannelAuthorityPlanUnchanged {
				t.Fatalf("Resolve terminal after = (%q,%v)", resolution, err)
			}
			if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), plan); !errors.Is(err,
				ErrChannelAuthorityPlanDiverged) {
				t.Fatalf("terminal response-loss replay error = %v", err)
			}
			assertEnrollmentTableCounts(t, joinerStore, map[string]int{"channels": 0,
				"channel_members": 0, "enrollment_receipts": 0, "publication_epochs": 0,
				"peer_bindings": 0, "channel_join_reservations": 0})
		})
	}
}

func newJoinedChannelInstallFixture(t *testing.T,
	seed, localAlias string,
) (channelEnrollmentFixture, *Store, InstallJoinedChannelSpec) {
	t.Helper()
	owner := newChannelEnrollmentFixture(t, seed)
	transcript := owner.transcript(t, 0x57, 0x58, owner.head)
	accepted := owner.accept(t, transcript)
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	spec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		OwnerOutcome: ChannelEnrollmentAccepted, LocalAlias: localAlias,
		Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)}
	reserveJoinedChannelTest(t, joinerStore, spec)
	return owner, joinerStore, spec
}
