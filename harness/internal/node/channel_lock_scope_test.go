package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelJoinWakesOnlyAffectedPeersOutsideManagerLock(t *testing.T) {
	ownerFixture := newDaemonFixture(t, true)
	joinerFixture := newDaemonFixture(t, true)
	owner := openChannelLockTestDaemon(t, ownerFixture)
	joiner := openChannelLockTestDaemon(t, joinerFixture)
	ownerWake := newChannelManagerWakeProbe(owner.channels.manager)
	joinerWake := newChannelManagerWakeProbe(joiner.channels.manager)
	owner.channels.manager.members = ownerWake
	joiner.channels.manager.members = joinerWake

	created, apiErr := owner.channels.manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(ownerFixture.profile,
			model.Sum([]byte("channel-lock-scope-create")), 0x41),
		ChannelCreateRequest{Name: "lock-scope"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	token, err := model.ParseEnrollmentToken(created.InviteToken)
	if err != nil {
		t.Fatal(err)
	}
	channelID := token.Payload().Descriptor().Descriptor().ID()
	if _, apiErr := joiner.channels.manager.ChannelJoin(context.Background(),
		RequestMetadata{Profile: joinerFixture.profile},
		ChannelJoinRequest{Token: created.InviteToken}); apiErr != nil {
		t.Fatal(apiErr)
	}

	ownerWake.assertExact(t, channelManagerWakeScope{channelID: channelID,
		peerID: joinerFixture.identity.PeerID()})
	joinerWake.assertExact(t, channelManagerWakeScope{channelID: channelID,
		peerID: ownerFixture.identity.PeerID()})

	ownerWake.reset()
	if _, apiErr := owner.channels.manager.ChannelAbandon(context.Background(),
		RequestMetadata{Profile: ownerFixture.profile},
		ChannelAbandonRequest{Channel: created.Channel.Alias,
			ConfirmChannel: created.Channel.Alias, Force: true}); apiErr != nil {
		t.Fatal(apiErr)
	}
	ownerWake.assertExact(t, channelManagerWakeScope{channelID: channelID,
		peerID: joinerFixture.identity.PeerID()})
}

func TestChannelCallbacksAndStatusDoNotDependOnManagerLock(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon := openChannelLockTestDaemon(t, fixture)
	manager := daemon.channels.manager
	clock := &channelManagerLockClock{manager: manager,
		at: fixture.profile.UpdatedAt().Add(time.Minute)}
	random := &channelManagerLockReader{manager: manager}
	manager.clock = clock
	manager.random = random

	created, apiErr := manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(fixture.profile,
			model.Sum([]byte("channel-lock-callback-create")), 0x42),
		ChannelCreateRequest{Name: "callback-boundary"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := manager.ChannelInvite(context.Background(),
		channelMutationTestMetadata(fixture.profile,
			model.Sum([]byte("channel-lock-callback-invite")), 0x43),
		ChannelInviteRequest{Channel: created.Channel.Alias, Uses: 1}); apiErr != nil {
		t.Fatal(apiErr)
	}
	clock.assertNeverLocked(t)
	random.assertNeverLocked(t)

	manager.mu.Lock()
	status := make(chan *APIError, 1)
	statusCtx, cancelStatus := context.WithCancel(context.Background())
	defer cancelStatus()
	go func() {
		_, apiErr := manager.ChannelStatus(statusCtx,
			RequestMetadata{Profile: fixture.profile})
		status <- apiErr
	}()
	select {
	case apiErr := <-status:
		manager.mu.Unlock()
		if apiErr != nil {
			t.Fatal(apiErr)
		}
	case <-time.After(time.Second):
		manager.mu.Unlock()
		cancelStatus()
		select {
		case apiErr := <-status:
			t.Fatalf("ChannelStatus waited for the mutation lock: %v", apiErr)
		case <-time.After(time.Second):
			t.Fatal("ChannelStatus did not stop after its context was cancelled")
		}
	}
}

func TestChannelInviteRejectsRosterChangedWhileTokenIsSigning(t *testing.T) {
	ownerFixture := newDaemonFixture(t, true)
	joinerFixture := newDaemonFixture(t, true)
	owner := openChannelLockTestDaemon(t, ownerFixture)
	joiner := openChannelLockTestDaemon(t, joinerFixture)
	created, apiErr := owner.channels.manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(ownerFixture.profile,
			model.Sum([]byte("channel-invite-stale-roster-create")), 0x51),
		ChannelCreateRequest{Name: "stale-roster"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	initial, err := model.ParseEnrollmentToken(created.InviteToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := joiner.channels.manager.ChannelJoin(context.Background(),
		RequestMetadata{Profile: joinerFixture.profile},
		ChannelJoinRequest{Token: created.InviteToken}); apiErr != nil {
		t.Fatal(apiErr)
	}

	manager := owner.channels.manager
	base := initial.Payload().Descriptor().Descriptor().CreatedAt()
	manager.clock = newChannelManagerScriptedClock(
		base.Add(20*time.Minute),
		base.Add(10*time.Minute),
		base.Add(30*time.Minute),
	)
	random := newChannelManagerBlockingReader()
	manager.random = random
	defer random.releaseFirst()
	metadata := channelMutationTestMetadata(ownerFixture.profile,
		model.Sum([]byte("channel-invite-stale-roster")), 0x52)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome := make(chan *APIError, 1)
	go func() {
		_, apiErr := manager.ChannelInvite(ctx, metadata,
			ChannelInviteRequest{Channel: created.Channel.Alias, Uses: 1})
		outcome <- apiErr
	}()
	random.waitForFirstRead(t)

	if _, apiErr := manager.ChannelRemove(ctx, RequestMetadata{Profile: ownerFixture.profile},
		ChannelRemoveRequest{Channel: created.Channel.Alias,
			Member: memberAlias(joinerFixture.identity.PeerID())}); apiErr != nil {
		t.Fatal(apiErr)
	}
	random.releaseFirst()
	if apiErr := <-outcome; apiErr == nil || apiErr.Code != CodeOperationMismatch {
		t.Fatalf("stale roster invite error = %#v", apiErr)
	}
	assertChannelInviteGrant(t, manager, created.Channel.Alias,
		initial.Payload().GrantID())
	assertChannelInviteOperationMissing(t, manager, metadata)
}

func TestChannelInviteRejectsNewOpenGrantCommittedWhileTokenIsSigning(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon := openChannelLockTestDaemon(t, fixture)
	manager := daemon.channels.manager
	created, apiErr := manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(fixture.profile,
			model.Sum([]byte("channel-invite-stale-open-create")), 0x53),
		ChannelCreateRequest{Name: "stale-open"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	initial, err := model.ParseEnrollmentToken(created.InviteToken)
	if err != nil {
		t.Fatal(err)
	}
	base := initial.Payload().Descriptor().Descriptor().CreatedAt()
	manager.clock = newChannelManagerScriptedClock(
		base.Add(20*time.Minute),
		base.Add(10*time.Minute),
		base.Add(30*time.Minute),
	)
	random := newChannelManagerBlockingReader()
	manager.random = random
	defer random.releaseFirst()
	staleMetadata := channelMutationTestMetadata(fixture.profile,
		model.Sum([]byte("channel-invite-stale-open")), 0x54)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	staleOutcome := make(chan *APIError, 1)
	go func() {
		_, apiErr := manager.ChannelInvite(ctx, staleMetadata,
			ChannelInviteRequest{Channel: created.Channel.Alias, Uses: 1})
		staleOutcome <- apiErr
	}()
	random.waitForFirstRead(t)

	fresh, apiErr := manager.ChannelInvite(ctx,
		channelMutationTestMetadata(fixture.profile,
			model.Sum([]byte("channel-invite-fresh-open")), 0x55),
		ChannelInviteRequest{Channel: created.Channel.Alias, Uses: 1})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	freshToken, err := model.ParseEnrollmentToken(fresh.InviteToken)
	if err != nil {
		t.Fatal(err)
	}
	random.releaseFirst()
	if apiErr := <-staleOutcome; apiErr == nil || apiErr.Code != CodeOperationMismatch {
		t.Fatalf("stale open grant invite error = %#v", apiErr)
	}
	assertChannelInviteGrant(t, manager, created.Channel.Alias,
		freshToken.Payload().GrantID())
	assertChannelInviteOperationMissing(t, manager, staleMetadata)
}

func assertChannelInviteGrant(t testing.TB, manager *ChannelManager, alias string,
	expected model.GrantID,
) {
	t.Helper()
	_, channel, apiErr := manager.selectChannel(context.Background(), alias)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	grant, found := channel.OpenGrant()
	if !found || grant.ID() != expected {
		t.Fatalf("current invite grant = (%s,%t), want %s",
			grant.ID().String(), found, expected.String())
	}
}

func assertChannelInviteOperationMissing(t testing.TB, manager *ChannelManager,
	metadata RequestMetadata,
) {
	t.Helper()
	operation := store.ChannelMutationOperation{Kind: store.ChannelMutationInvite,
		OperationKeyHash: metadata.OperationKeyHash, RequestDigest: metadata.RequestDigest}
	if _, found, err := manager.store.ReadChannelMutation(context.Background(), operation); err != nil || found {
		t.Fatalf("stale invite operation = (found %t,%v), want absent", found, err)
	}
}

func openChannelLockTestDaemon(t *testing.T, fixture daemonFixture) *Daemon {
	t.Helper()
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: wallClock{}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := daemon.Close(); err != nil {
			t.Errorf("close lock-boundary daemon: %v", err)
		}
	})
	return daemon
}

type channelManagerScriptedClock struct {
	mu       sync.Mutex
	values   []time.Time
	fallback time.Time
}

func newChannelManagerScriptedClock(values ...time.Time) *channelManagerScriptedClock {
	fallback := time.Time{}
	if len(values) != 0 {
		fallback = values[len(values)-1]
	}
	return &channelManagerScriptedClock{
		values: append([]time.Time(nil), values...), fallback: fallback}
}

func (clock *channelManagerScriptedClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if len(clock.values) != 0 {
		value := clock.values[0]
		clock.values = clock.values[1:]
		return value
	}
	clock.fallback = clock.fallback.Add(time.Second)
	return clock.fallback
}

type channelManagerBlockingReader struct {
	mu sync.Mutex

	calls       int
	firstRead   chan struct{}
	firstReturn chan struct{}
	release     sync.Once
}

func newChannelManagerBlockingReader() *channelManagerBlockingReader {
	return &channelManagerBlockingReader{
		firstRead: make(chan struct{}), firstReturn: make(chan struct{})}
}

func (reader *channelManagerBlockingReader) Read(target []byte) (int, error) {
	reader.mu.Lock()
	reader.calls++
	call := reader.calls
	for index := range target {
		target[index] = byte(call*32 + index + 1)
	}
	if call == 1 {
		close(reader.firstRead)
	}
	reader.mu.Unlock()
	if call == 1 {
		<-reader.firstReturn
	}
	return len(target), nil
}

func (reader *channelManagerBlockingReader) waitForFirstRead(t testing.TB) {
	t.Helper()
	select {
	case <-reader.firstRead:
	case <-time.After(2 * time.Second):
		t.Fatal("Channel invite did not reach token randomness")
	}
}

func (reader *channelManagerBlockingReader) releaseFirst() {
	reader.release.Do(func() { close(reader.firstReturn) })
}

type channelManagerWakeScope struct {
	channelID model.ChannelID
	peerID    model.PeerID
}

type channelManagerWakeProbe struct {
	manager *ChannelManager

	mu        sync.Mutex
	globals   int
	scopes    []channelManagerWakeScope
	underLock bool
}

func newChannelManagerWakeProbe(manager *ChannelManager) *channelManagerWakeProbe {
	return &channelManagerWakeProbe{manager: manager}
}

func (probe *channelManagerWakeProbe) Trigger() {
	probe.recordLock()
	probe.mu.Lock()
	probe.globals++
	probe.mu.Unlock()
}

func (probe *channelManagerWakeProbe) TriggerScope(channelID model.ChannelID,
	peerID model.PeerID,
) error {
	probe.recordLock()
	probe.mu.Lock()
	probe.scopes = append(probe.scopes,
		channelManagerWakeScope{channelID: channelID, peerID: peerID})
	probe.mu.Unlock()
	return nil
}

func (probe *channelManagerWakeProbe) recordLock() {
	available := probe.manager.mu.TryLock()
	if available {
		probe.manager.mu.Unlock()
	}
	probe.mu.Lock()
	probe.underLock = probe.underLock || !available
	probe.mu.Unlock()
}

func (probe *channelManagerWakeProbe) assertExact(t *testing.T,
	expected channelManagerWakeScope,
) {
	t.Helper()
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.globals != 0 || probe.underLock || len(probe.scopes) != 1 ||
		probe.scopes[0] != expected {
		t.Fatalf("member wake = globals %d, scopes %#v, under manager lock %t",
			probe.globals, probe.scopes, probe.underLock)
	}
}

func (probe *channelManagerWakeProbe) reset() {
	probe.mu.Lock()
	probe.globals = 0
	probe.scopes = nil
	probe.underLock = false
	probe.mu.Unlock()
}

type channelManagerLockClock struct {
	manager *ChannelManager

	mu        sync.Mutex
	at        time.Time
	underLock bool
}

func (clock *channelManagerLockClock) Now() time.Time {
	available := clock.manager.mu.TryLock()
	if available {
		clock.manager.mu.Unlock()
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.underLock = clock.underLock || !available
	clock.at = clock.at.Add(time.Second)
	return clock.at
}

func (clock *channelManagerLockClock) assertNeverLocked(t *testing.T) {
	t.Helper()
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if clock.underLock {
		t.Fatal("Clock.Now ran while the Channel manager mutation lock was held")
	}
}

type channelManagerLockReader struct {
	manager *ChannelManager

	mu        sync.Mutex
	next      byte
	underLock bool
}

func (reader *channelManagerLockReader) Read(target []byte) (int, error) {
	available := reader.manager.mu.TryLock()
	if available {
		reader.manager.mu.Unlock()
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.underLock = reader.underLock || !available
	for index := range target {
		reader.next++
		target[index] = reader.next
	}
	return len(target), nil
}

func (reader *channelManagerLockReader) assertNeverLocked(t *testing.T) {
	t.Helper()
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.underLock {
		t.Fatal("random identifier callback ran while the Channel manager mutation lock was held")
	}
}
