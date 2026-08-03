package main

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

func TestQuerySampleStartsEveryBoundedWorkerBeforeWaiting(t *testing.T) {
	sample := []selector.ParticipantID{
		mustNetworkParticipant(t, "query-sample-a"),
		mustNetworkParticipant(t, "query-sample-b"),
	}
	entered := make(chan struct{}, len(sample))
	release := make(chan struct{})
	done := make(chan []selector.AuthenticatedVote, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		done <- querySample(ctx, sample, selector.SampleQuery{},
			func(ctx context.Context, _ selector.ParticipantID, _ selector.SampleQuery,
			) (selector.AuthenticatedVote, bool, error) {
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
				return selector.AuthenticatedVote{}, false, nil
			})
	}()
	for range sample {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("sample workers did not start concurrently")
		}
	}
	close(release)
	select {
	case votes := <-done:
		if len(votes) != 0 {
			t.Fatalf("votes = %d, want none", len(votes))
		}
	case <-ctx.Done():
		t.Fatal("sample workers did not join")
	}
}

func mustNetworkParticipant(t *testing.T, value string) selector.ParticipantID {
	t.Helper()
	participant, err := selector.NewParticipantID(value)
	if err != nil {
		t.Fatal(err)
	}
	return participant
}
