package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestReadWrongTopicReplayCandidateSelectsLocalSourceAndCountsTarget(t *testing.T) {
	t.Parallel()

	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "wrong-topic-source", nil)
	spec := fixture.offer(t, authority, "wrong-topic-source", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	owner := testkit.NewIdentity(t, "owner:accept-"+t.Name())
	target := testkit.NewSignedChannelForOwnerAt(t, "wrong-topic-target-"+t.Name(),
		owner, fixture.now.Add(-30*time.Minute))
	insertSignedChannelFixture(t, fixture.store.db, target, model.TopicJoined)

	candidate, err := fixture.store.ReadWrongTopicReplayCandidate(context.Background(),
		fixture.channel, target.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}
	scope := candidate.Publication.Event().Scope()
	if candidate.SourceChannelID != fixture.channel ||
		candidate.TargetChannelID != target.Channel().ID() ||
		scope.ChannelID() != fixture.channel ||
		scope.OriginPeerID() != fixture.node.PeerID() ||
		candidate.PublicationDigest != candidate.Publication.Digest() ||
		candidate.EventKey != candidate.Publication.Event().Key() ||
		candidate.EventDigest != candidate.Publication.Event().Digest() ||
		candidate.SourceChannelDigest.IsZero() || candidate.TargetChannelDigest.IsZero() ||
		candidate.TargetMutationCounts.Events != 0 || candidate.TargetMutationCounts.Works != 0 {
		t.Fatalf("wrong-topic candidate = %#v", candidate)
	}
	counts, err := fixture.store.ReadChannelMutationCounts(context.Background(), target.Channel().ID())
	if err != nil || counts != candidate.TargetMutationCounts {
		t.Fatalf("target counts = %#v, %v", counts, err)
	}
}
