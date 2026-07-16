package agent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestOfferSelectorTeamUsesCanonicalPeerBytesAndIgnoresReachability(t *testing.T) {
	t.Parallel()
	peers := []model.PeerID{
		agentSelectorPeer(t, "team-a"),
		agentSelectorPeer(t, "team-b"),
		agentSelectorPeer(t, "team-c"),
	}
	sort.Slice(peers, func(left, right int) bool {
		leftBytes, _ := canonicalAgentPeerBytes(peers[left])
		rightBytes, _ := canonicalAgentPeerBytes(peers[right])
		return bytes.Compare(leftBytes, rightBytes) < 0
	})
	reviewers := []decodedAgentOfferReviewer{
		agentDecodedReviewer(t, peers[2], "alpha", model.ReachabilityReachable, true),
		agentDecodedReviewer(t, peers[0], "zulu", model.ReachabilityUnreachable, true),
		agentDecodedReviewer(t, peers[1], "middle", model.ReachabilityUnknown, true),
	}
	sortAgentOfferReviewers(reviewers)

	team, err := selectAgentOfferReviewers(reviewers, AgentParticipantTeam)
	if err != nil {
		t.Fatal(err)
	}
	for index, alias := range []string{"zulu", "middle", "alpha"} {
		if team[index].PeerID() != peers[index] || team[index].EffectiveAlias() != alias {
			t.Fatalf("team[%d] = (%s,%q), want (%s,%q)", index, team[index].PeerID().String(),
				team[index].EffectiveAlias(), peers[index].String(), alias)
		}
	}
	if team[0].Reachability() != model.ReachabilityUnreachable {
		t.Fatal("unreachable reviewer was removed or replaced")
	}
	explicit, err := selectAgentOfferReviewers(reviewers, "zulu")
	if err != nil || len(explicit) != 1 || explicit[0].PeerID() != peers[0] {
		t.Fatalf("explicit unreachable reviewer = (%#v, %v)", explicit, err)
	}
	for _, selector := range []string{"", AgentParticipantAuto} {
		_, err := selectAgentOfferReviewers(reviewers, selector)
		if !errors.Is(err, ErrAgentSelectionParticipantAmbiguous) {
			t.Fatalf("selector %q error = %v", selector, err)
		}
		var candidates *AgentSelectionCandidatesError
		if !errors.As(err, &candidates) || fmt.Sprint(candidates.Candidates()) != "[alpha middle zulu]" {
			t.Fatalf("selector %q candidates = %#v", selector, candidates)
		}
		copyCandidates := candidates.Candidates()
		copyCandidates[0] = "changed"
		if candidates.Candidates()[0] != "alpha" {
			t.Fatal("ambiguity candidates are mutable")
		}
	}
}

func TestOfferSelectorRequiresUniqueChannelAndNeverFallsBack(t *testing.T) {
	t.Parallel()
	head := agentSelectorHead(t, "selector-roster")
	alphaPeer := agentSelectorPeer(t, "alpha-reviewer")
	betaPeer := agentSelectorPeer(t, "beta-reviewer")
	channels := []decodedAgentOfferChannel{
		{id: agentSelectorChannel(t, "channel-beta"), alias: "beta", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{agentDecodedReviewer(t, betaPeer, "beta-reviewer",
				model.ReachabilityReachable, true)}},
		{id: agentSelectorChannel(t, "channel-alpha"), alias: "alpha", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{agentDecodedReviewer(t, alphaPeer, "alpha-reviewer",
				model.ReachabilityUnreachable, true)}},
	}

	_, err := selectAgentOfferChannel(channels, "")
	if !errors.Is(err, ErrAgentSelectionChannelAmbiguous) {
		t.Fatalf("omitted Channel error = %v", err)
	}
	var candidates *AgentSelectionCandidatesError
	if !errors.As(err, &candidates) || fmt.Sprint(candidates.Candidates()) != "[alpha beta]" {
		t.Fatalf("Channel candidates = %#v", candidates)
	}
	selected, err := selectAgentOfferChannel(channels, "alpha")
	if err != nil || selected.id.String() != "channel-alpha" {
		t.Fatalf("explicit Channel = (%#v, %v)", selected, err)
	}
	if _, err := selectAgentOfferChannel(channels, "missing"); !errors.Is(err, ErrAgentSelectionChannelUnavailable) {
		t.Fatalf("missing explicit Channel error = %v", err)
	}

	first := agentDecodedReviewer(t, agentSelectorPeer(t, "no-fallback-first"), "first",
		model.ReachabilityUnreachable, false)
	second := agentDecodedReviewer(t, agentSelectorPeer(t, "no-fallback-second"), "second",
		model.ReachabilityReachable, true)
	reviewers := []decodedAgentOfferReviewer{first, second}
	sortAgentOfferReviewers(reviewers)
	if _, err := selectAgentOfferReviewers(reviewers, "first"); !errors.Is(err, ErrAgentSelectionParticipantUnavailable) {
		t.Fatalf("explicit ineligible reviewer error = %v", err)
	}
	chosen, err := selectAgentOfferReviewers(reviewers, "second")
	if err != nil || len(chosen) != 1 || chosen[0].EffectiveAlias() != "second" {
		t.Fatalf("eligible explicit reviewer = (%#v, %v)", chosen, err)
	}
}

func TestOfferSelectorReadsOnceAndRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	profile := agentSelectorProfile(t)
	now := profile.UpdatedAt().Add(time.Hour)
	readError := errors.New("candidate read failed")
	reader := &agentSelectorReader{err: readError}
	selector, err := NewOfferSelector(reader)
	if err != nil {
		t.Fatal(err)
	}
	_, err = selector.Resolve(context.Background(), AgentOfferSelectionSpec{
		Profile: profile, ChannelAlias: "alpha", ParticipantSelector: "reviewer", At: now,
	})
	if !errors.Is(err, readError) || reader.calls != 1 || reader.profile.ID() != profile.ID() || !reader.at.Equal(now) {
		t.Fatalf("reader boundary = error %v calls %d Profile %q at %s", err, reader.calls,
			reader.profile.ID().String(), reader.at)
	}

	reader.calls = 0
	_, err = selector.Resolve(context.Background(), AgentOfferSelectionSpec{
		Profile: profile, ParticipantSelector: strings.Repeat("x", model.MaxLabelBytes+1), At: now,
	})
	if !errors.Is(err, ErrAgentSelectionInput) || reader.calls != 0 {
		t.Fatalf("invalid selector = error %v calls %d", err, reader.calls)
	}
	invalidPeer, _ := model.ParsePeerID("not-a-libp2p-peer")
	if _, err := canonicalAgentPeerBytes(invalidPeer); err == nil {
		t.Fatal("invalid libp2p PeerID was accepted")
	}
	for _, reserved := range []string{AgentParticipantAuto, AgentParticipantTeam} {
		if err := validateAgentCandidateAlias(reserved); !errors.Is(err, ErrAgentSelectionInvariant) {
			t.Fatalf("reserved candidate alias %q error = %v", reserved, err)
		}
	}
	if _, err := NewOfferSelector(nil); !errors.Is(err, ErrAgentSelectionInput) {
		t.Fatalf("nil selector reader error = %v", err)
	}
}

type agentSelectorReader struct {
	calls   int
	profile model.Profile
	at      time.Time
	err     error
}

func (r *agentSelectorReader) ReadAgentOfferCandidates(_ context.Context, profile model.Profile,
	at time.Time,
) (store.AgentOfferCandidates, error) {
	r.calls++
	r.profile, r.at = profile, at
	return store.AgentOfferCandidates{}, r.err
}

func agentDecodedReviewer(t *testing.T, peer model.PeerID, alias string,
	reachability model.Reachability, eligible bool,
) decodedAgentOfferReviewer {
	t.Helper()
	canonical, err := canonicalAgentPeerBytes(peer)
	if err != nil {
		t.Fatal(err)
	}
	return decodedAgentOfferReviewer{
		projection:    AgentOfferReviewer{peerID: peer, effectiveAlias: alias, reachability: reachability},
		canonicalPeer: canonical, eligible: eligible,
	}
}

func agentSelectorPeer(t *testing.T, label string) model.PeerID {
	t.Helper()
	seed := sha256.Sum256([]byte(label))
	standardPrivate := ed25519.NewKeyFromSeed(seed[:])
	privateKey, err := libp2pcrypto.UnmarshalEd25519PrivateKey(standardPrivate)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	result, err := model.ParsePeerID(peerID.String())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func agentSelectorChannel(t *testing.T, value string) model.ChannelID {
	t.Helper()
	channel, err := model.ParseChannelID(value)
	if err != nil {
		t.Fatal(err)
	}
	return channel
}

func agentSelectorHead(t *testing.T, value string) model.RecordHead {
	t.Helper()
	head, err := model.NewRecordHead(2, model.Sum([]byte(value)))
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func agentSelectorProfile(t *testing.T) model.Profile {
	t.Helper()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-selector", WorkspaceRoot: "/workspace/selector",
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash: model.Sum([]byte("selector-credential")), ActiveAssetRevision: "asset-r5",
		HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: true, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}
