package node

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type channelMutationClock struct{ at time.Time }

func (clock *channelMutationClock) Now() time.Time { return clock.at }

func TestChannelCreateAndInviteReplayExactTokenAcrossResponseLossAndRestart(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	clock := &channelMutationClock{at: fixture.profile.UpdatedAt().Add(time.Minute)}
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: clock, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if daemon != nil {
			_ = daemon.Close()
		}
	})

	createRequest, createMetadata, created := exerciseChannelCreateReplay(t,
		daemon.channels.manager, fixture.profile)
	clock.at = clock.at.Add(time.Minute)
	inviteRequest, inviteMetadata, invited := exerciseChannelInviteReplay(t,
		daemon.channels.manager, fixture.profile, created.Channel.Alias)
	assertChannelInviteRequestMismatch(t, daemon.channels.manager, inviteMetadata,
		created.Channel.Alias)

	path := daemon.store.Path()
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	daemon = nil
	assertChannelMutationSecretsAbsent(t, path, created.InviteToken,
		createMetadata.OperationKeySecret)
	assertChannelMutationSecretsAbsent(t, path, invited.InviteToken,
		inviteMetadata.OperationKeySecret)

	daemon, err = OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: clock, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	exerciseChannelMutationRestartReplay(t, daemon.channels.manager, fixture.profile,
		createRequest, createMetadata, created, inviteRequest, inviteMetadata, invited)
	corruptChannelInviteCommitment(t, path, inviteMetadata)
	if _, apiErr := daemon.channels.manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest); apiErr == nil || apiErr.Code != CodeInternal {
		t.Fatalf("corrupt durable token commitment error = %#v", apiErr)
	}
}

func exerciseChannelCreateReplay(t testing.TB, manager *ChannelManager, profile model.Profile,
) (ChannelCreateRequest, RequestMetadata, ChannelCreateResponse) {
	t.Helper()
	createRequest := ChannelCreateRequest{Name: "idempotent-channel"}
	createMetadata := channelMutationTestMetadata(profile,
		model.Sum([]byte("typed-create-request")), 0x51)
	created, apiErr := manager.ChannelCreate(context.Background(),
		createMetadata, createRequest)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	createReplay, apiErr := manager.ChannelCreate(context.Background(),
		createMetadata, createRequest)
	if apiErr != nil || createReplay.InviteToken != created.InviteToken ||
		createReplay.Channel.ChannelIDDigest != created.Channel.ChannelIDDigest {
		t.Fatalf("create response-loss replay = (%#v, %v)", createReplay, apiErr)
	}
	return createRequest, createMetadata, created
}

func exerciseChannelInviteReplay(t testing.TB, manager *ChannelManager, profile model.Profile,
	alias string,
) (ChannelInviteRequest, RequestMetadata, ChannelInviteResponse) {
	t.Helper()
	inviteRequest := ChannelInviteRequest{Channel: alias,
		ExpiresSeconds: 3600, Uses: 2}
	inviteMetadata := channelMutationTestMetadata(profile,
		model.Sum([]byte("typed-invite-request")), 0x61)
	invited, apiErr := manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	inviteReplay, apiErr := manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest)
	if apiErr != nil || inviteReplay.InviteToken != invited.InviteToken ||
		inviteReplay.Invite != invited.Invite {
		t.Fatalf("invite response-loss replay = (%#v, %v)", inviteReplay, apiErr)
	}
	return inviteRequest, inviteMetadata, invited
}

func assertChannelInviteRequestMismatch(t testing.TB, manager *ChannelManager,
	inviteMetadata RequestMetadata, alias string,
) {
	t.Helper()
	mismatch := inviteMetadata
	mismatch.RequestDigest = model.Sum([]byte("changed-invite-request"))
	if _, apiErr := manager.ChannelInvite(context.Background(), mismatch,
		ChannelInviteRequest{Channel: alias, ExpiresSeconds: 7200, Uses: 1}); apiErr == nil ||
		apiErr.Code != CodeOperationMismatch {
		t.Fatalf("same key changed digest error = %#v", apiErr)
	}
}

func exerciseChannelMutationRestartReplay(t testing.TB, manager *ChannelManager,
	profile model.Profile, createRequest ChannelCreateRequest, createMetadata RequestMetadata,
	created ChannelCreateResponse, inviteRequest ChannelInviteRequest,
	inviteMetadata RequestMetadata, invited ChannelInviteResponse,
) {
	t.Helper()
	createRestart, apiErr := manager.ChannelCreate(context.Background(),
		createMetadata, createRequest)
	if apiErr != nil || createRestart.InviteToken != created.InviteToken {
		t.Fatalf("create restart replay = (%#v, %v)", createRestart, apiErr)
	}
	inviteRestart, apiErr := manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest)
	if apiErr != nil || inviteRestart.InviteToken != invited.InviteToken {
		t.Fatalf("invite restart replay = (%#v, %v)", inviteRestart, apiErr)
	}
	leaveMetadata := channelMutationTestMetadata(profile,
		model.Sum([]byte("typed-leave-request")), 0x71)
	clear(leaveMetadata.OperationKeySecret)
	leaveMetadata.OperationKeySecret = nil
	wakes := &recordingChannelMemberWake{}
	manager.members = wakes
	left, apiErr := manager.ChannelLeave(context.Background(),
		leaveMetadata, ChannelLeaveRequest{Channel: created.Channel.Alias})
	if apiErr != nil || left.Status != "left" || left.Channel.Membership != "closed" {
		t.Fatalf("owner leave operation = (%#v, %v)", left, apiErr)
	}
	if globals, scopes := wakes.snapshot(); globals != 0 || scopes != 1 {
		t.Fatalf("owner leave wake scope = global %d scoped %d", globals, scopes)
	}
	leaveReplay, apiErr := manager.ChannelLeave(context.Background(),
		leaveMetadata, ChannelLeaveRequest{Channel: created.Channel.Alias})
	if apiErr != nil || leaveReplay.Status != left.Status ||
		leaveReplay.Channel.ChannelIDDigest != left.Channel.ChannelIDDigest {
		t.Fatalf("owner leave response-loss replay = (%#v, %v)", leaveReplay, apiErr)
	}
	if globals, scopes := wakes.snapshot(); globals != 0 || scopes != 2 {
		t.Fatalf("owner leave replay wake scope = global %d scoped %d", globals, scopes)
	}
}

func corruptChannelInviteCommitment(t testing.TB, path string,
	inviteMetadata RequestMetadata,
) {
	t.Helper()
	corrupt, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.Exec(`DROP TRIGGER channel_mutation_operations_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.Exec(`UPDATE channel_mutation_operations SET token_payload_digest=?
		WHERE operation_key_hash=?`, model.Sum([]byte("corrupt-token-payload")).Bytes(),
		inviteMetadata.OperationKeyHash.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := corrupt.Close(); err != nil {
		t.Fatal(err)
	}
}

type recordingChannelMemberWake struct {
	mu      sync.Mutex
	globals int
	scopes  int
}

func (wake *recordingChannelMemberWake) Trigger() {
	wake.mu.Lock()
	wake.globals++
	wake.mu.Unlock()
}

func (wake *recordingChannelMemberWake) TriggerScope(model.ChannelID, model.PeerID) error {
	wake.mu.Lock()
	wake.scopes++
	wake.mu.Unlock()
	return nil
}

func (wake *recordingChannelMemberWake) snapshot() (int, int) {
	wake.mu.Lock()
	defer wake.mu.Unlock()
	return wake.globals, wake.scopes
}

func channelMutationTestMetadata(profile model.Profile, requestDigest model.Digest, fill byte,
) RequestMetadata {
	secret := bytes.Repeat([]byte{fill}, model.EnrollmentSecretBytes)
	return RequestMetadata{Profile: profile, OperationKeyHash: model.Sum(secret),
		HasOperationKey: true, OperationKeySecret: secret,
		RequestDigest: requestDigest, HasRequestDigest: true}
}

func assertChannelMutationSecretsAbsent(t *testing.T, path, encoded string,
	operationSecret []byte,
) {
	t.Helper()
	token, err := model.ParseEnrollmentToken(encoded)
	if err != nil {
		t.Fatal(err)
	}
	secret := token.Payload().BearerSecret()
	for _, candidate := range []string{path, path + "-wal"} {
		raw, readErr := os.ReadFile(candidate)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		if bytes.Contains(raw, []byte(encoded)) || bytes.Contains(raw, secret) ||
			bytes.Contains(raw, operationSecret) ||
			bytes.Contains(raw, token.Payload().RevealCanonicalJSON().Bytes()) {
			t.Fatalf("Channel mutation secret leaked into %s", candidate)
		}
	}
}

// The Store counterpart covers rollback before commit. Each immediate replay
// covers commit-before-response/response loss, and the reopened daemon proves
// exact reconstruction from the pending key plus durable non-secret authority.
