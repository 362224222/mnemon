package peer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGossipJoinContextCancelsAffectedTransitionWait(t *testing.T) {
	fixture := newGossipAuthorityTransitionFixture(t)
	transition := beginDrainedGossipAuthorityTransition(t, fixture)
	pending := true
	defer func() {
		if pending {
			_ = transition.Abort()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan error, 1)
	go func() {
		_, err := fixture.gossip.join(ctx, fixture.channel.ChannelID)
		joined <- err
	}()
	select {
	case err := <-joined:
		t.Fatalf("join returned while affecting transition was pending: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-joined:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrGossipTopic) {
			t.Fatalf("canceled join error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled join remained blocked on affecting transition")
	}
	select {
	case <-transition.Done():
		t.Fatal("canceled join finalized the authority transition")
	default:
	}
	if err := transition.Abort(); err != nil || !fixture.session.IsCurrent() {
		t.Fatalf("Abort after canceled join = %v, current=%t", err, fixture.session.IsCurrent())
	}
	pending = false
}

func TestGossipJoinContextCancelsClosingSessionWait(t *testing.T) {
	fixture := newGossipAuthorityTransitionFixture(t)
	requireGateAdmission(t, fixture.session.gate, context.Background())
	held := true
	defer func() {
		if held {
			fixture.session.gate.release()
		}
	}()
	closed := make(chan error, 1)
	go func() { closed <- fixture.session.Close() }()
	waitTopicSessionClosing(t, fixture.session)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan error, 1)
	go func() {
		_, err := fixture.gossip.join(ctx, fixture.channel.ChannelID)
		joined <- err
	}()
	select {
	case err := <-joined:
		t.Fatalf("join returned while prior session was closing: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-joined:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrGossipTopic) {
			t.Fatalf("join canceled behind closing session error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("join ignored cancellation behind a closing session")
	}
	select {
	case err := <-closed:
		t.Fatalf("session Close bypassed its held admission: %v", err)
	default:
	}
	fixture.session.gate.release()
	held = false
	if err := <-closed; err != nil {
		t.Fatalf("session Close after release = %v", err)
	}
}

func waitTopicSessionClosing(t *testing.T, session *TopicSession) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !session.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !session.closed.Load() {
		t.Fatal("TopicSession Close did not establish its closing fence")
	}
}
