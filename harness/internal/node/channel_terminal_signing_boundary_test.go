package node

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestConcurrentChannelRemoveSignsOutsideManagerLockWithoutPersistingFork(t *testing.T) {
	ownerFixture := newDaemonFixture(t, true)
	joinerFixture := newDaemonFixture(t, true)
	owner := openChannelLockTestDaemon(t, ownerFixture)
	joiner := openChannelLockTestDaemon(t, joinerFixture)
	created, apiErr := owner.channels.manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(ownerFixture.profile,
			model.Sum([]byte("terminal-sign-remove-create")), 0x91),
		ChannelCreateRequest{Name: "terminal-sign-remove"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := joiner.channels.manager.ChannelJoin(context.Background(),
		RequestMetadata{Profile: joinerFixture.profile},
		ChannelJoinRequest{Token: created.InviteToken}); apiErr != nil {
		t.Fatal(apiErr)
	}

	barrier := installChannelSigningBarrier(owner.channels.manager, 2)
	request := ChannelRemoveRequest{Channel: created.Channel.Alias,
		Member: memberAlias(joinerFixture.identity.PeerID())}
	results := make(chan *APIError, 2)
	for range 2 {
		go func() {
			_, apiErr := owner.channels.manager.ChannelRemove(context.Background(),
				RequestMetadata{Profile: ownerFixture.profile}, request)
			results <- apiErr
		}()
	}
	barrier.waitAndRelease(t)
	for range 2 {
		if apiErr := <-results; apiErr != nil {
			t.Fatalf("concurrent remove error = %#v", apiErr)
		}
	}
	assertNoChannelSigningFork(t, owner, created.Channel.Alias, model.ChannelActive, 3)
}

func TestConcurrentOwnerLeaveSignsOutsideManagerLockAndReplaysWinner(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon := openChannelLockTestDaemon(t, fixture)
	created, apiErr := daemon.channels.manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(fixture.profile,
			model.Sum([]byte("terminal-sign-leave-create")), 0x92),
		ChannelCreateRequest{Name: "terminal-sign-leave"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	barrier := installChannelSigningBarrier(daemon.channels.manager, 2)
	metadata := channelMutationTestMetadata(fixture.profile,
		model.Sum([]byte("terminal-sign-leave")), 0x93)
	results := make(chan channelLeaveConcurrentResult, 2)
	for range 2 {
		go func() {
			response, apiErr := daemon.channels.manager.ChannelLeave(context.Background(),
				metadata, ChannelLeaveRequest{Channel: created.Channel.Alias})
			results <- channelLeaveConcurrentResult{response: response, apiErr: apiErr}
		}()
	}
	barrier.waitAndRelease(t)
	for range 2 {
		result := <-results
		if result.apiErr != nil || result.response.Status != "left" ||
			result.response.Channel.Membership != string(model.ChannelClosed) {
			t.Fatalf("concurrent owner leave = (%#v,%#v)", result.response, result.apiErr)
		}
	}
	assertNoChannelSigningFork(t, daemon, created.Channel.Alias, model.ChannelClosed, 2)
	db := openChannelSigningReadDB(t, daemon)
	var operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM channel_leave_operations`).Scan(
		&operations); err != nil || operations != 1 {
		t.Fatalf("owner leave operations = (%d,%v), want one", operations, err)
	}
}

func TestOwnerLeaveRetryReconcilesRuntimeAfterCommittedResponseLoss(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon := openChannelLockTestDaemon(t, fixture)
	created, apiErr := daemon.channels.manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(fixture.profile,
			model.Sum([]byte("terminal-response-loss-leave-create")), 0x94),
		ChannelCreateRequest{Name: "terminal-response-loss-leave"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	manager := daemon.channels.manager
	authority, durable, apiErr := manager.selectChannel(context.Background(),
		created.Channel.Alias)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	oldSession, err := manager.runtime.Session(durable.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}
	metadata := channelMutationTestMetadata(fixture.profile,
		model.Sum([]byte("terminal-response-loss-leave")), 0x95)
	operation, apiErr := channelLeaveOperation(metadata)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, err := manager.commitTerminalMember(context.Background(), authority.LocalPeerID(), durable,
		authority.LocalPeerID(), model.MemberLeft, &operation, manager.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if !oldSession.IsCurrent() {
		t.Fatal("owner runtime changed before the omitted response refresh")
	}

	response, apiErr := manager.ChannelLeave(context.Background(), metadata,
		ChannelLeaveRequest{Channel: created.Channel.Alias})
	if apiErr != nil || response.Status != "left" ||
		response.Channel.Membership != string(model.ChannelClosed) {
		t.Fatalf("owner leave retry = (%#v,%#v)", response, apiErr)
	}
	if oldSession.IsCurrent() || manager.runtime.HasCurrentSession(durable.Channel().ID()) {
		t.Fatal("owner leave retry did not retire stale runtime authority")
	}
	assertNoChannelSigningFork(t, daemon, created.Channel.Alias, model.ChannelClosed, 2)
	assertTerminalSigningRows(t, daemon, durable.Channel().ID(), 2, 1)
}

func TestChannelRemoveRetryReconcilesRuntimeAfterCommittedResponseLoss(t *testing.T) {
	ownerFixture := newDaemonFixture(t, true)
	joinerFixture := newDaemonFixture(t, true)
	owner := openChannelLockTestDaemon(t, ownerFixture)
	joiner := openChannelLockTestDaemon(t, joinerFixture)
	created, apiErr := owner.channels.manager.ChannelCreate(context.Background(),
		channelMutationTestMetadata(ownerFixture.profile,
			model.Sum([]byte("terminal-response-loss-remove-create")), 0x96),
		ChannelCreateRequest{Name: "terminal-response-loss-remove"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := joiner.channels.manager.ChannelJoin(context.Background(),
		RequestMetadata{Profile: joinerFixture.profile},
		ChannelJoinRequest{Token: created.InviteToken}); apiErr != nil {
		t.Fatal(apiErr)
	}
	manager := owner.channels.manager
	authority, durable, apiErr := manager.selectChannel(context.Background(),
		created.Channel.Alias)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	target := joinerFixture.identity.PeerID()
	selector := ""
	for _, binding := range durable.SelectorBindings() {
		if binding.PeerID() == target {
			selector = binding.EffectiveAlias()
			break
		}
	}
	if selector == "" {
		t.Fatal("joined member selector is unavailable")
	}
	oldSession, err := manager.runtime.Session(durable.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.commitTerminalMember(context.Background(), authority.LocalPeerID(), durable,
		target, model.MemberRevoked, nil, manager.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if !oldSession.IsCurrent() {
		t.Fatal("owner runtime changed before the omitted response refresh")
	}

	response, apiErr := manager.ChannelRemove(context.Background(),
		RequestMetadata{Profile: ownerFixture.profile},
		ChannelRemoveRequest{Channel: created.Channel.Alias, Member: selector})
	if apiErr != nil || response.Status != "removed" ||
		response.Channel.Membership != string(model.ChannelActive) {
		t.Fatalf("remove retry = (%#v,%#v)", response, apiErr)
	}
	if oldSession.IsCurrent() || !manager.runtime.HasCurrentSession(durable.Channel().ID()) {
		t.Fatal("remove retry did not rotate stale runtime authority")
	}
	assertNoChannelSigningFork(t, owner, created.Channel.Alias, model.ChannelActive, 3)
	assertTerminalSigningRows(t, owner, durable.Channel().ID(), 3, 0)
}

type channelLeaveConcurrentResult struct {
	response ChannelLeaveResponse
	apiErr   *APIError
}

type channelSigningBarrier struct {
	manager  *ChannelManager
	delegate eventpkg.PublicationSigner
	target   int

	mu        sync.Mutex
	calls     int
	probeErr  error
	arrived   chan struct{}
	release   chan struct{}
	readyOnce sync.Once
	closeOnce sync.Once
}

func installChannelSigningBarrier(manager *ChannelManager, target int) *channelSigningBarrier {
	barrier := &channelSigningBarrier{manager: manager,
		delegate: manager.identity.PublicationSigner(), target: target,
		arrived: make(chan struct{}), release: make(chan struct{})}
	manager.identity.signer = barrier
	return barrier
}

func (barrier *channelSigningBarrier) Sign(ctx context.Context, message []byte) ([]byte, error) {
	probeErr := probeChannelManagerLock(ctx, barrier.manager)
	barrier.mu.Lock()
	barrier.calls++
	barrier.probeErr = errors.Join(barrier.probeErr, probeErr)
	if barrier.calls == barrier.target {
		barrier.readyOnce.Do(func() { close(barrier.arrived) })
	}
	barrier.mu.Unlock()
	if probeErr != nil {
		return nil, probeErr
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-barrier.release:
		return barrier.delegate.Sign(ctx, message)
	}
}

func (barrier *channelSigningBarrier) waitAndRelease(t *testing.T) {
	t.Helper()
	select {
	case <-barrier.arrived:
	case <-time.After(3 * time.Second):
		barrier.closeOnce.Do(func() { close(barrier.release) })
		t.Fatal("two signing callbacks did not run concurrently")
	}
	barrier.mu.Lock()
	probeErr := barrier.probeErr
	barrier.mu.Unlock()
	barrier.closeOnce.Do(func() { close(barrier.release) })
	if probeErr != nil {
		t.Fatalf("Channel signing callback could not re-enter manager lock: %v", probeErr)
	}
}

func probeChannelManagerLock(ctx context.Context, manager *ChannelManager) error {
	acquired := make(chan struct{})
	go func() {
		manager.mu.Lock()
		manager.mu.Unlock()
		close(acquired)
	}()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-acquired:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("manager lock remained held during signing callback")
	}
}

func assertNoChannelSigningFork(t *testing.T, daemon *Daemon, alias string,
	status model.ChannelStatus, revision uint64,
) {
	t.Helper()
	db := openChannelSigningReadDB(t, daemon)
	var durableStatus string
	var durableRevision uint64
	var conflicts int
	err := db.QueryRow(`SELECT status,roster_head_revision,
		(SELECT COUNT(*) FROM channel_conflicts conflicts
			WHERE conflicts.channel_id=channels.channel_id)
		FROM channels WHERE local_alias=?`, alias).Scan(
		&durableStatus, &durableRevision, &conflicts)
	if err != nil || durableStatus != string(status) || durableRevision != revision || conflicts != 0 {
		t.Fatalf("terminal signing durable state = status %q revision %d conflicts %d err %v",
			durableStatus, durableRevision, conflicts, err)
	}
}

func openChannelSigningReadDB(t *testing.T, daemon *Daemon) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", daemon.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close signing-boundary read database: %v", err)
		}
	})
	return db
}

func assertTerminalSigningRows(t *testing.T, daemon *Daemon, channelID model.ChannelID,
	memberRecords, leaveOperations int,
) {
	t.Helper()
	db := openChannelSigningReadDB(t, daemon)
	var members, operations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM channel_members WHERE channel_id=?`,
		channelID.String()).Scan(&members); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM channel_leave_operations WHERE channel_id=?`,
		channelID.String()).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if members != memberRecords || operations != leaveOperations {
		t.Fatalf("terminal durable rows = members %d operations %d, want %d/%d",
			members, operations, memberRecords, leaveOperations)
	}
}
