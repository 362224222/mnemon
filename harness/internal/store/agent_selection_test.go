package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReadAgentOfferCandidatesFreezesVerifiedSnapshot(t *testing.T) {
	t.Parallel()
	fixture := newAgentCandidateFixture(t)
	fixture.addChannel(t, "production", []agentCandidateRemote{
		{peer: agentCandidatePeer(t, "candidate-a"), alias: "zulu", reachability: model.ReachabilityUnreachable},
		{peer: agentCandidatePeer(t, "candidate-b"), alias: "alpha", reachability: model.ReachabilityReachable},
		{peer: agentCandidatePeer(t, "candidate-c"), alias: "middle", reachability: model.ReachabilityUnknown},
	})

	snapshot, err := fixture.read()
	if err != nil {
		t.Fatal(err)
	}
	channels := snapshot.Channels()
	if len(channels) != 1 || channels[0].LocalAlias() != "production" ||
		channels[0].ChannelID().String() != "channel-production" || channels[0].RosterHead().Revision() != 4 {
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
		first := agentCandidatePeer(t, "baseline-first")
		second := agentCandidatePeer(t, "baseline-second")
		fixture.addChannel(t, "alpha", []agentCandidateRemote{
			{peer: first, alias: "first", reachability: model.ReachabilityUnreachable},
			{peer: second, alias: "second", reachability: model.ReachabilityReachable},
		})
		mustExec(t, fixture.store, "DELETE FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?",
			"channel-alpha", first.String())
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
		peer := agentCandidatePeer(t, "reserved-alias")
		fixture.addChannel(t, "alpha", []agentCandidateRemote{{peer: peer, alias: "reviewer",
			reachability: model.ReachabilityReachable}})
		mustExec(t, fixture.store, "UPDATE peer_bindings SET effective_alias='team' WHERE channel_id=? AND peer_id=?",
			"channel-alpha", peer.String())
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
		{name: "missing inbound baseline", wantCount: 1,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				mustExec(t, fixture.store, "DELETE FROM peer_cursors WHERE channel_id=? AND origin_peer_id=?",
					"channel-alpha", peer.String())
			}},
		{name: "selected protocol missing", wantCount: 1,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				mustExec(t, fixture.store, `UPDATE peer_bindings SET protocols_json=?
					WHERE channel_id=? AND peer_id=?`, []byte(`["/mnemon/channel/1","/mnemon/events/1"]`),
					"channel-alpha", peer.String())
			}},
		{name: "binding no longer active", wantCount: 0,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				mustExec(t, fixture.store, "UPDATE peer_bindings SET state='pending' WHERE channel_id=? AND peer_id=?",
					"channel-alpha", peer.String())
			}},
		{name: "binding stale against latest member", wantCount: 0,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, peer model.PeerID) {
				fixture.appendActiveMember(t, "alpha", peer)
			}},
		{name: "roster head digest drift", want: ErrAgentOfferCandidatesInvariant,
			mutate: func(t *testing.T, fixture *agentCandidateFixture, _ model.PeerID) {
				mustExec(t, fixture.store, "UPDATE channels SET roster_head_hash=? WHERE channel_id=?",
					model.Sum([]byte("drifted-roster-head")).Bytes(), "channel-alpha")
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAgentCandidateFixture(t)
			peer := agentCandidatePeer(t, "mutated-"+strings.ReplaceAll(test.name, " ", "-"))
			fixture.addChannel(t, "alpha", []agentCandidateRemote{{peer: peer, alias: "reviewer",
				reachability: model.ReachabilityUnreachable}})
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
	fixture.addChannel(t, "alpha", []agentCandidateRemote{{peer: agentCandidatePeer(t, "authority-reviewer"),
		alias: "reviewer", reachability: model.ReachabilityReachable}})

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
		remotes[index] = agentCandidateRemote{peer: agentCandidatePeer(t, fmt.Sprintf("wide-%d", index)),
			alias: fmt.Sprintf("reviewer-%d", index), reachability: model.ReachabilityUnknown}
		switch index % 3 {
		case 1:
			remotes[index].reachability = model.ReachabilityReachable
		case 2:
			remotes[index].reachability = model.ReachabilityUnreachable
		}
	}
	fixture.addChannel(t, "channel-0", remotes)
	for index := 1; index < model.MaxChannelsPerNode; index++ {
		fixture.addChannel(t, fmt.Sprintf("channel-%d", index), []agentCandidateRemote{{
			peer: agentCandidatePeer(t, fmt.Sprintf("narrow-%d", index)), alias: "reviewer",
			reachability: model.ReachabilityUnknown,
		}})
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

	mustExec(t, fixture.store, "UPDATE peer_bindings SET state='pending' WHERE channel_id=? AND peer_id=?",
		"channel-channel-0", remotes[0].peer.String())
	contextView, err = fixture.store.ReadAgentInitiationContext(context.Background(), fixture.profile, fixture.now)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(contextView.Channels()[0].Participants()); got != model.MaxChildWorks-1 {
		t.Fatalf("pending binding remained in initiation context: %d participants", got)
	}
}

type agentCandidateRemote struct {
	peer         model.PeerID
	alias        string
	reachability model.Reachability
}

type agentCandidateFixture struct {
	store   *Store
	node    model.Node
	profile model.Profile
	now     time.Time
}

func newAgentCandidateFixture(t *testing.T) *agentCandidateFixture {
	t.Helper()
	store := openTestStore(t)
	local := agentCandidatePeer(t, "local-"+t.Name())
	node, profile := bootstrapValues(t, local.String(), "principal-agent-candidate-"+agentCandidateSlug(t.Name()),
		"/workspace/agent-candidate/"+agentCandidateSlug(t.Name()))
	node, profile = activateTestNode(t, store, node, profile)
	return &agentCandidateFixture{store: store, node: node, profile: profile,
		now: profile.UpdatedAt().Add(time.Hour)}
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
	channelID := "channel-" + alias
	created := fixture.profile.UpdatedAt().Add(time.Minute)
	createdText := storeTime(created)
	localKey := []byte("local-public-key-" + alias)
	hashes := make([]model.Digest, len(remotes)+1)
	for index := range hashes {
		hashes[index] = model.Sum([]byte(fmt.Sprintf("%s-member-%d", alias, index)))
	}
	mustExec(t, fixture.store, `INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,owner_public_key,
		member_limit,roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES(?,?,?,?,?,8,?,?,'active','joined',?,?)`, channelID, "Channel "+alias, alias,
		fixture.node.PeerID().String(), localKey, len(hashes), hashes[len(hashes)-1].Bytes(), createdText, createdText)
	mustExec(t, fixture.store, `INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
		member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,status,signed_record_json,
		owner_signature,created_at) VALUES(?,1,?,NULL,?,?,?,?,?,'active',?,?,?)`, channelID,
		hashes[0].Bytes(), fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(), "local", localKey,
		[]byte(`[]`), []byte(`{}`), []byte("owner-signature"), createdText)
	mustExec(t, fixture.store, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`, channelID,
		fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(), createdText)

	for index, remote := range remotes {
		revision := index + 2
		epoch := "epoch-" + remote.peer.String()
		publicKey := []byte("remote-public-key-" + remote.peer.String())
		mustExec(t, fixture.store, `INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
			member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,status,signed_record_json,
			owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,'active',?,?,?)`, channelID, revision,
			hashes[index+1].Bytes(), hashes[index].Bytes(), remote.peer.String(), epoch, remote.alias,
			publicKey, []byte(`[]`), []byte(`{}`), []byte("owner-signature"), createdText)
		mustExec(t, fixture.store, `INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,effective_alias,
			public_key,multiaddrs_json,protocols_json,limits_json,member_revision,member_record_hash,state,
			reachability,joined_at) VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?,?)`, channelID, remote.peer.String(),
			epoch, remote.alias, publicKey, []byte(`[]`), agentCandidateProtocols(),
			[]byte(`{"frame_bytes":65536}`), revision, hashes[index+1].Bytes(),
			string(remote.reachability), createdText)
		mustExec(t, fixture.store, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
			baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at) VALUES(?,?,?,0,0,0,?)`,
			channelID, remote.peer.String(), epoch, createdText)
		mustExec(t, fixture.store, "UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?",
			channelID, remote.peer.String())
		mustExec(t, fixture.store, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
			origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
			VALUES(?,?,?,?,0,0,?,?)`, channelID, remote.peer.String(), fixture.node.PeerID().String(),
			fixture.node.OriginEpoch().String(), createdText, createdText)
	}
}

func (fixture *agentCandidateFixture) appendActiveMember(t *testing.T, alias string, peer model.PeerID) {
	t.Helper()
	channelID := "channel-" + alias
	var revision uint64
	var previousHash, publicKey []byte
	var epoch, displayLabel string
	if err := fixture.store.db.QueryRow(`SELECT c.roster_head_revision,c.roster_head_hash,m.origin_epoch,
		m.display_label,m.public_key FROM channels c JOIN channel_members m ON m.channel_id=c.channel_id
		AND m.member_peer_id=? WHERE c.channel_id=? ORDER BY m.revision DESC LIMIT 1`, peer.String(), channelID).
		Scan(&revision, &previousHash, &epoch, &displayLabel, &publicKey); err != nil {
		t.Fatal(err)
	}
	revision++
	hash := model.Sum([]byte(fmt.Sprintf("%s-latest-%d", alias, revision)))
	created := fixture.now.Add(-time.Minute)
	mustExec(t, fixture.store, `INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
		member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,status,signed_record_json,
		owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,'active',?,?,?)`, channelID, revision,
		hash.Bytes(), previousHash, peer.String(), epoch, displayLabel, publicKey, []byte(`[]`), []byte(`{}`),
		[]byte("owner-signature"), storeTime(created))
	mustExec(t, fixture.store, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, revision, hash.Bytes(), storeTime(created), channelID)
}

func agentCandidateProtocols() []byte {
	return []byte(`["/mnemon/artifacts/1","/mnemon/channel/1","/mnemon/events/1"]`)
}

func agentCandidatePeer(t *testing.T, label string) model.PeerID {
	t.Helper()
	digest := sha256.Sum256([]byte(label))
	peerID, err := model.ParsePeerID(fmt.Sprintf("peer-%x", digest[:]))
	if err != nil {
		t.Fatal(err)
	}
	return peerID
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
