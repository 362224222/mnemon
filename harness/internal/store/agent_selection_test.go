package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestReadAgentOfferCandidatesFreezesVerifiedSnapshot(t *testing.T) {
	t.Parallel()
	fixture := newAgentCandidateFixture(t)
	fixture.addChannel(t, "production", []agentCandidateRemote{
		fixture.remote(t, "candidate-a", "zulu", model.ReachabilityUnreachable),
		fixture.remote(t, "candidate-b", "alpha", model.ReachabilityReachable),
		fixture.remote(t, "candidate-c", "middle", model.ReachabilityUnknown),
	})

	snapshot, err := fixture.read()
	if err != nil {
		t.Fatal(err)
	}
	channels := snapshot.Channels()
	if len(channels) != 1 || channels[0].LocalAlias() != "production" ||
		channels[0].ChannelID() != fixture.channelID("production") || channels[0].RosterHead().Revision() != 4 {
		t.Fatalf("candidate Channel = %#v", channels)
	}
	reviewers := channels[0].Reviewers()
	if len(reviewers) != 3 {
		t.Fatalf("reviewer count = %d", len(reviewers))
	}
	for index, alias := range []string{"alpha", "middle", "zulu"} {
		if reviewers[index].EffectiveAlias() != alias || !reviewers[index].Eligible() {
			t.Fatalf("reviewer[%d] = alias %q eligible=%v", index,
				reviewers[index].EffectiveAlias(), reviewers[index].Eligible())
		}
	}
	if reviewers[2].Reachability() != model.ReachabilityUnreachable {
		t.Fatal("unreachable binding did not remain eligible and projected")
	}
	copyChannels := snapshot.Channels()
	copyChannels[0] = AgentOfferCandidateChannel{}
	if snapshot.Channels()[0].LocalAlias() != "production" {
		t.Fatal("candidate Channel list is mutable")
	}
	copyReviewers := channels[0].Reviewers()
	copyReviewers[0] = AgentOfferCandidateReviewer{}
	if channels[0].Reviewers()[0].EffectiveAlias() != "alpha" {
		t.Fatal("candidate reviewer list is mutable")
	}
}

func TestReadAgentOfferCandidatesRereadsEligibility(t *testing.T) {
	t.Parallel()

	t.Run("outbound baseline and reachability are independent", func(t *testing.T) {
		fixture := newAgentCandidateFixture(t)
		firstRemote := fixture.remote(t, "baseline-first", "first", model.ReachabilityUnreachable)
		secondRemote := fixture.remote(t, "baseline-second", "second", model.ReachabilityReachable)
		firstRemote.omitOutboundBaseline = true
		fixture.addChannel(t, "alpha", []agentCandidateRemote{
			firstRemote, secondRemote,
		})
		snapshot, err := fixture.read()
		if err != nil {
			t.Fatal(err)
		}
		eligibility := map[string]bool{}
		for _, reviewer := range snapshot.Channels()[0].Reviewers() {
			eligibility[reviewer.EffectiveAlias()] = reviewer.Eligible()
		}
		if eligibility["first"] || !eligibility["second"] {
			t.Fatalf("eligibility = %#v", eligibility)
		}
		contextView, err := fixture.store.ReadAgentInitiationContext(context.Background(), fixture.profile, fixture.now)
		if err != nil {
			t.Fatal(err)
		}
		if len(contextView.Channels()) != 1 || !contextView.Channels()[0].AllowTeam() {
			t.Fatalf("initiation context = %#v", contextView.Channels())
		}
	})
	t.Run("reserved alias stays internal and never enters initiation context", func(t *testing.T) {
		fixture := newAgentCandidateFixture(t)
		remote := fixture.remote(t, "reserved-alias", "reviewer", model.ReachabilityReachable)
		peer := remote.peer
		fixture.addChannel(t, "alpha", []agentCandidateRemote{remote})
		mustExec(t, fixture.store, "UPDATE peer_bindings SET effective_alias='team' WHERE channel_id=? AND peer_id=?",
			fixture.channelID("alpha").String(), peer.String())
		snapshot, err := fixture.read()
		if err != nil || snapshot.Channels()[0].Reviewers()[0].EffectiveAlias() != "team" {
			t.Fatalf("trusted candidate snapshot = (%#v, %v)", snapshot.Channels(), err)
		}
		if _, err := fixture.store.ReadAgentInitiationContext(context.Background(), fixture.profile, fixture.now); !errors.Is(err, ErrAgentOfferCandidatesInvariant) {
			t.Fatalf("reserved initiation alias error = %v", err)
		}
	})

	for _, test := range []struct {
		name      string
		mutate    func(*testing.T, *agentCandidateFixture, model.PeerID)
		want      error
		wantCount int
	}{
		{name: "active binding missing inbound baseline", want: ErrAgentOfferCandidatesInvariant,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				mustExec(t, fixture.store, "DROP TRIGGER peer_repairs_no_delete")
				mustExec(t, fixture.store, "DELETE FROM peer_repairs WHERE channel_id=? AND origin_peer_id=?",
					fixture.channelID("alpha").String(), peer.String())
				mustExec(t, fixture.store, "DROP TRIGGER peer_cursors_no_delete")
				mustExec(t, fixture.store, "DELETE FROM peer_cursors WHERE channel_id=? AND origin_peer_id=?",
					fixture.channelID("alpha").String(), peer.String())
			}},
		{name: "binding no longer active", wantCount: 0,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				fixture.revokeMember(t, "alpha", peer)
			}},
		{name: "binding stale against latest member", want: ErrAgentOfferCandidatesInvariant,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				fixture.appendActiveMember(t, "alpha", peer)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAgentCandidateFixture(t)
			remote := fixture.remote(t, "mutated-"+strings.ReplaceAll(test.name, " ", "-"),
				"reviewer", model.ReachabilityUnreachable)
			peer := remote.peer
			fixture.addChannel(t, "alpha", []agentCandidateRemote{remote})
			test.mutate(t, fixture, peer)
			snapshot, err := fixture.read()
			if test.want != nil {
				if !errors.Is(err, test.want) {
					t.Fatalf("ReadAgentOfferCandidates() error = %v, want %v", err, test.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			reviewers := snapshot.Channels()[0].Reviewers()
			if len(reviewers) != test.wantCount {
				t.Fatalf("candidate reviewer count = %d, want %d", len(reviewers), test.wantCount)
			}
			if len(reviewers) == 1 && reviewers[0].Eligible() {
				t.Fatal("incomplete protocol/baseline remained eligible")
			}
		})
	}
}

func TestReadAgentOfferCandidatesRejectsInactiveAuthorityAndUntrustedTime(t *testing.T) {
	t.Parallel()
	fixture := newAgentCandidateFixture(t)
	fixture.addChannel(t, "alpha", []agentCandidateRemote{
		fixture.remote(t, "authority-reviewer", "reviewer", model.ReachabilityReachable)})

	staleSpec := fixture.profile.Spec()
	staleSpec.ActiveAssetRevision = "asset-stale"
	staleProfile, err := model.NewProfile(staleSpec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.store.ReadAgentOfferCandidates(context.Background(), staleProfile, fixture.now)
	if !errors.Is(err, ErrAgentOfferCandidatesAuthority) {
		t.Fatalf("stale authenticated Profile error = %v", err)
	}
	_, err = fixture.store.ReadAgentOfferCandidates(context.Background(), fixture.profile,
		fixture.profile.UpdatedAt().Add(-time.Nanosecond))
	if !errors.Is(err, ErrAgentOfferCandidatesAuthority) {
		t.Fatalf("past trusted time error = %v", err)
	}
	if _, err := fixture.store.ReadAgentOfferCandidates(context.Background(), fixture.profile, time.Time{}); !errors.Is(err, ErrAgentOfferCandidatesInput) {
		t.Fatalf("zero trusted time error = %v", err)
	}

	mustExec(t, fixture.store, "UPDATE channels SET topic_state='not_joined' WHERE local_alias='alpha'")
	snapshot, err := fixture.read()
	if err != nil || len(snapshot.Channels()) != 0 {
		t.Fatalf("not-joined Channel candidates = (%#v, %v)", snapshot.Channels(), err)
	}
	mustExec(t, fixture.store, "UPDATE channels SET status='leaving',topic_state='left' WHERE local_alias='alpha'")
	snapshot, err = fixture.read()
	if err != nil || len(snapshot.Channels()) != 0 {
		t.Fatalf("inactive Channel candidates = (%#v, %v)", snapshot.Channels(), err)
	}
}

func TestReadAgentInitiationContextIsBoundedToActiveBindings(t *testing.T) {
	t.Parallel()
	fixture := newAgentCandidateFixture(t)
	remotes := make([]agentCandidateRemote, model.MaxChildWorks)
	for index := range remotes {
		remotes[index] = fixture.remote(t, fmt.Sprintf("wide-%d", index),
			fmt.Sprintf("reviewer-%d", index), model.ReachabilityUnknown)
		switch index % 3 {
		case 1:
			remotes[index].reachability = model.ReachabilityReachable
		case 2:
			remotes[index].reachability = model.ReachabilityUnreachable
		}
	}
	fixture.addChannel(t, "channel-0", remotes)
	for index := 1; index < model.MaxChannelsPerNode; index++ {
		fixture.addChannel(t, fmt.Sprintf("channel-%d", index), []agentCandidateRemote{
			fixture.remote(t, fmt.Sprintf("narrow-%d", index), "reviewer", model.ReachabilityUnknown)})
	}

	contextView, err := fixture.store.ReadAgentInitiationContext(context.Background(), fixture.profile, fixture.now)
	if err != nil {
		t.Fatal(err)
	}
	channels := contextView.Channels()
	if len(channels) != model.MaxChannelsPerNode || len(channels[0].Participants()) != model.MaxChildWorks {
		t.Fatalf("initiation bounds = %d Channels, %d first participants", len(channels),
			len(channels[0].Participants()))
	}
	for index, channel := range channels {
		if channel.LocalAlias() != fmt.Sprintf("channel-%d", index) || !channel.AllowTeam() {
			t.Fatalf("channel[%d] = alias %q allow_team=%v", index, channel.LocalAlias(), channel.AllowTeam())
		}
	}
	projection, err := contextView.CanonicalJSON()
	if err != nil || strings.Contains(projection.String(), "peer_id") ||
		strings.Contains(projection.String(), remotes[0].peer.String()) ||
		!strings.Contains(projection.String(), `"initiation_context"`) ||
		!strings.Contains(projection.String(), `"effective_alias":"reviewer-0"`) {
		t.Fatalf("identity-free initiation projection = %s, %v", projection.String(), err)
	}

	fixture.revokeMember(t, "channel-0", remotes[0].peer)
	contextView, err = fixture.store.ReadAgentInitiationContext(context.Background(), fixture.profile, fixture.now)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(contextView.Channels()[0].Participants()); got != model.MaxChildWorks-1 {
		t.Fatalf("pending binding remained in initiation context: %d participants", got)
	}
}

type agentCandidateRemote struct {
	peer                 model.PeerID
	alias                string
	reachability         model.Reachability
	omitOutboundBaseline bool
}

type agentCandidateFixture struct {
	store      *Store
	node       model.Node
	profile    model.Profile
	now        time.Time
	local      testkit.Identity
	identities map[model.PeerID]testkit.Identity
	channels   map[string]*testkit.SignedChannel
}

func newAgentCandidateFixture(t *testing.T) *agentCandidateFixture {
	t.Helper()
	store := openTestStore(t)
	local := testkit.NewIdentity(t, "local-"+t.Name())
	createdAt := time.Date(2026, 7, 16, 12, 0, 0, 123, time.UTC)
	node, profile := signedBootstrapValues(t, local,
		"principal-agent-candidate-"+agentCandidateSlug(t.Name()),
		"/workspace/agent-candidate/"+agentCandidateSlug(t.Name()), createdAt)
	node, profile = activateTestNode(t, store, node, profile)
	return &agentCandidateFixture{store: store, node: node, profile: profile,
		now: profile.UpdatedAt().Add(time.Hour), local: local,
		identities: map[model.PeerID]testkit.Identity{local.PeerID(): local},
		channels:   make(map[string]*testkit.SignedChannel)}
}

func (fixture *agentCandidateFixture) read() (AgentOfferCandidates, error) {
	return fixture.store.ReadAgentOfferCandidates(context.Background(), fixture.profile, fixture.now)
}

func (fixture *agentCandidateFixture) addChannel(t *testing.T, alias string,
	remotes []agentCandidateRemote,
) {
	t.Helper()
	if len(remotes) == 0 || len(remotes) > model.MaxChildWorks {
		t.Fatalf("invalid remote fixture count %d", len(remotes))
	}
	created := fixture.profile.UpdatedAt().Add(time.Minute)
	signed := testkit.NewSignedChannelForOwnerAt(t, "agent-candidate-"+alias, fixture.local, created)
	for _, remote := range remotes {
		identity, ok := fixture.identities[remote.peer]
		if !ok {
			t.Fatalf("remote %s has no deterministic identity", remote.peer.String())
		}
		signed.AppendActiveIdentity(t, identity)
	}
	fixture.channels[alias] = signed
	insertSignedChannelFixture(t, fixture.store.db, signed, model.TopicJoined)
	mustExec(t, fixture.store, "UPDATE channels SET local_alias=? WHERE channel_id=?", alias,
		signed.Channel().ID().String())
	createdText := storeTime(signed.Channel().UpdatedAt())
	mustExec(t, fixture.store, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		signed.Channel().ID().String(),
		fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(), createdText)

	members := signed.Members()
	for index, remote := range remotes {
		member := members[index+1]
		insertSignedPeerBinding(t, fixture.store.db, signed.Channel().ID(), member, remote.alias,
			model.BindingPending, remote.reachability, signed.Channel().UpdatedAt())
		epoch := member.Identity().OriginEpoch().String()
		mustExec(t, fixture.store, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
			baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at) VALUES(?,?,?,0,0,0,?)`,
			signed.Channel().ID().String(), remote.peer.String(), epoch, createdText)
		mustExec(t, fixture.store, "UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?",
			signed.Channel().ID().String(), remote.peer.String())
		if !remote.omitOutboundBaseline {
			mustExec(t, fixture.store, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
				origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
				VALUES(?,?,?,?,0,0,NULL,?)`, signed.Channel().ID().String(), remote.peer.String(),
				fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(), createdText)
			mustExec(t, fixture.store, `UPDATE peer_pull_acks SET baseline_confirmed_at=?
				WHERE channel_id=? AND target_peer_id=?`, createdText, signed.Channel().ID().String(),
				remote.peer.String())
		}
	}
}

func (fixture *agentCandidateFixture) appendActiveMember(t *testing.T, alias string, peer model.PeerID) {
	t.Helper()
	signed := fixture.channels[alias]
	memberFixture := signed.AppendActiveUpdate(t, peer)
	member := memberFixture.Projection()
	memberCreatedAt := storeTime(memberFixture.Member().CreatedAt())
	mustExec(t, fixture.store, `INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
		member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,
		status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		member.ChannelID, member.Revision, member.RecordHash, member.PreviousHash, member.MemberPeerID,
		member.OriginEpoch, member.DisplayLabel, member.PublicKey, member.MultiaddrsJSON, member.ProtocolsJSON,
		member.LimitsJSON, member.Status, member.SignedRecordJSON, member.OwnerSignature, memberCreatedAt)
	mustExec(t, fixture.store, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, member.Revision, member.RecordHash, memberCreatedAt, member.ChannelID)
}

func (fixture *agentCandidateFixture) revokeMember(t *testing.T, alias string, peer model.PeerID) {
	t.Helper()
	signed := fixture.channels[alias]
	memberFixture := signed.AppendTerminal(t, peer, model.MemberRevoked)
	member := memberFixture.Projection()
	memberCreatedAt := storeTime(memberFixture.Member().CreatedAt())
	mustExec(t, fixture.store, `INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
		member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,
		status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		member.ChannelID, member.Revision, member.RecordHash, member.PreviousHash, member.MemberPeerID,
		member.OriginEpoch, member.DisplayLabel, member.PublicKey, member.MultiaddrsJSON, member.ProtocolsJSON,
		member.LimitsJSON, member.Status, member.SignedRecordJSON, member.OwnerSignature, memberCreatedAt)
	mustExec(t, fixture.store, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, member.Revision, member.RecordHash, memberCreatedAt, member.ChannelID)
	mustExec(t, fixture.store, `UPDATE peer_bindings SET member_revision=?,member_record_hash=?,state='revoked'
		WHERE channel_id=? AND peer_id=?`, member.Revision, member.RecordHash, member.ChannelID, peer.String())
}

func (fixture *agentCandidateFixture) remote(t *testing.T, seed, alias string,
	reachability model.Reachability,
) agentCandidateRemote {
	t.Helper()
	identity := testkit.NewIdentity(t, seed)
	fixture.identities[identity.PeerID()] = identity
	return agentCandidateRemote{peer: identity.PeerID(), alias: alias, reachability: reachability}
}

func (fixture *agentCandidateFixture) channelID(alias string) model.ChannelID {
	return fixture.channels[alias].Channel().ID()
}

func agentCandidateSlug(value string) string {
	value = strings.ToLower(value)
	var result strings.Builder
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			result.WriteRune(character)
		} else {
			result.WriteByte('-')
		}
	}
	return strings.Trim(result.String(), "-")
}
