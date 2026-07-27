package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestOfferSelectorRequiresChannelForCrossChannelAliasCollision(t *testing.T) {
	t.Parallel()
	head := agentSelectorHead(t, "selector-roster")
	alphaPeer := agentSelectorPeer(t, "alpha-reviewer")
	betaPeer := agentSelectorPeer(t, "beta-reviewer")
	uniquePeer := agentSelectorPeer(t, "unique-reviewer")
	channels := []decodedAgentOfferChannel{
		{id: agentSelectorChannel(t, "channel-beta"), alias: "beta", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{
				agentDecodedReviewer(t, betaPeer, "reviewer", model.ReachabilityReachable, true)}},
		{id: agentSelectorChannel(t, "channel-alpha"), alias: "alpha", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{
				agentDecodedReviewer(t, alphaPeer, "reviewer", model.ReachabilityUnreachable, true)}},
		{id: agentSelectorChannel(t, "channel-gamma"), alias: "gamma", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{
				agentDecodedReviewer(t, uniquePeer, "unique", model.ReachabilityUnknown, true)}},
	}

	_, _, err := selectAgentOfferReviewer(channels, "", "reviewer")
	if !errors.Is(err, ErrAgentSelectionChannelAmbiguous) {
		t.Fatalf("cross-Channel alias error = %v", err)
	}
	var candidates *AgentSelectionCandidatesError
	if !errors.As(err, &candidates) || fmt.Sprint(candidates.Candidates()) != "[alpha beta]" {
		t.Fatalf("Channel candidates = %#v", candidates)
	}
	copyCandidates := candidates.Candidates()
	copyCandidates[0] = "changed"
	if candidates.Candidates()[0] != "alpha" {
		t.Fatal("ambiguity candidates are mutable")
	}

	channel, reviewer, err := selectAgentOfferReviewer(channels, "alpha", "reviewer")
	if err != nil || channel.alias != "alpha" || reviewer.PeerID() != alphaPeer ||
		reviewer.Reachability() != model.ReachabilityUnreachable {
		t.Fatalf("explicit unreachable reviewer = (%#v, %#v, %v)", channel, reviewer, err)
	}
	channel, reviewer, err = selectAgentOfferReviewer(channels, "", "unique")
	if err != nil || channel.alias != "gamma" || reviewer.PeerID() != uniquePeer {
		t.Fatalf("unique cross-Channel reviewer = (%#v, %#v, %v)", channel, reviewer, err)
	}
}

func TestOfferSelectorRequiresExactChannelAndNeverFallsBack(t *testing.T) {
	t.Parallel()
	head := agentSelectorHead(t, "selector-roster")
	alphaPeer := agentSelectorPeer(t, "no-fallback-alpha")
	betaPeer := agentSelectorPeer(t, "no-fallback-beta")
	channels := []decodedAgentOfferChannel{
		{id: agentSelectorChannel(t, "channel-beta"), alias: "beta", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{agentDecodedReviewer(t, betaPeer, "reviewer",
				model.ReachabilityReachable, true)}},
		{id: agentSelectorChannel(t, "channel-alpha"), alias: "alpha", rosterHead: head,
			reviewers: []decodedAgentOfferReviewer{agentDecodedReviewer(t, alphaPeer, "reviewer",
				model.ReachabilityUnreachable, false)}},
	}

	if _, _, err := selectAgentOfferReviewer(channels, "missing", "reviewer"); !errors.Is(err, ErrAgentSelectionChannelUnavailable) {
		t.Fatalf("missing explicit Channel error = %v", err)
	}
	if _, _, err := selectAgentOfferReviewer(channels, "alpha", "reviewer"); !errors.Is(err, ErrAgentSelectionParticipantUnavailable) {
		t.Fatalf("explicit ineligible reviewer error = %v", err)
	}
	channel, reviewer, err := selectAgentOfferReviewer(channels, "", "reviewer")
	if err != nil || channel.alias != "beta" || reviewer.PeerID() != betaPeer {
		t.Fatalf("eligible reviewer = (%#v, %#v, %v)", channel, reviewer, err)
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
	_, err = selector.Resolve(context.Background(), AgentOfferSelectionSpec{
		Profile: profile, ChannelAlias: "alpha", At: now,
	})
	if !errors.Is(err, ErrAgentSelectionInput) || reader.calls != 0 {
		t.Fatalf("missing participant selector = error %v calls %d", err, reader.calls)
	}
	invalidPeer, _ := model.ParsePeerID("not-a-libp2p-peer")
	if _, err := canonicalAgentPeerBytes(invalidPeer); err == nil {
		t.Fatal("invalid libp2p PeerID was accepted")
	}
	for _, alias := range []string{"auto", "team"} {
		if err := validateAgentCandidateAlias(alias); err != nil {
			t.Fatalf("effective alias %q error = %v", alias, err)
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
	if _, err := canonicalAgentPeerBytes(peer); err != nil {
		t.Fatal(err)
	}
	return decodedAgentOfferReviewer{
		projection: AgentOfferReviewer{peerID: peer, effectiveAlias: alias, reachability: reachability},
		eligible:   eligible,
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
