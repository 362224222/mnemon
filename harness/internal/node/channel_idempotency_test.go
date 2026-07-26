package node

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
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

	createRequest := ChannelCreateRequest{Name: "idempotent-channel"}
	createMetadata := channelMutationTestMetadata(fixture.profile,
		model.Sum([]byte("typed-create-request")), 0x51)
	created, apiErr := daemon.channels.manager.ChannelCreate(context.Background(),
		createMetadata, createRequest)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	createReplay, apiErr := daemon.channels.manager.ChannelCreate(context.Background(),
		createMetadata, createRequest)
	if apiErr != nil || createReplay.InviteToken != created.InviteToken ||
		createReplay.Channel.ChannelIDDigest != created.Channel.ChannelIDDigest {
		t.Fatalf("create response-loss replay = (%#v, %v)", createReplay, apiErr)
	}

	clock.at = clock.at.Add(time.Minute)
	inviteRequest := ChannelInviteRequest{Channel: created.Channel.Alias,
		ExpiresSeconds: 3600, Uses: 2}
	inviteMetadata := channelMutationTestMetadata(fixture.profile,
		model.Sum([]byte("typed-invite-request")), 0x61)
	invited, apiErr := daemon.channels.manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	inviteReplay, apiErr := daemon.channels.manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest)
	if apiErr != nil || inviteReplay.InviteToken != invited.InviteToken ||
		inviteReplay.Invite != invited.Invite {
		t.Fatalf("invite response-loss replay = (%#v, %v)", inviteReplay, apiErr)
	}

	mismatch := inviteMetadata
	mismatch.RequestDigest = model.Sum([]byte("changed-invite-request"))
	if _, apiErr := daemon.channels.manager.ChannelInvite(context.Background(), mismatch,
		ChannelInviteRequest{Channel: created.Channel.Alias, ExpiresSeconds: 7200, Uses: 1}); apiErr == nil || apiErr.Code != CodeOperationMismatch {
		t.Fatalf("same key changed digest error = %#v", apiErr)
	}

	path := daemon.store.Path()
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	daemon = nil
	assertChannelMutationSecretsAbsent(t, path, created.InviteToken,
		createMetadata.OperationKeySecret)
	assertChannelMutationSecretsAbsent(t, path, invited.InviteToken,
		inviteMetadata.OperationKeySecret)

	restarted, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: clock, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	daemon = restarted
	createRestart, apiErr := daemon.channels.manager.ChannelCreate(context.Background(),
		createMetadata, createRequest)
	if apiErr != nil || createRestart.InviteToken != created.InviteToken {
		t.Fatalf("create restart replay = (%#v, %v)", createRestart, apiErr)
	}
	inviteRestart, apiErr := daemon.channels.manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest)
	if apiErr != nil || inviteRestart.InviteToken != invited.InviteToken {
		t.Fatalf("invite restart replay = (%#v, %v)", inviteRestart, apiErr)
	}

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
	if _, apiErr := daemon.channels.manager.ChannelInvite(context.Background(),
		inviteMetadata, inviteRequest); apiErr == nil || apiErr.Code != CodeInternal {
		t.Fatalf("corrupt durable token commitment error = %#v", apiErr)
	}
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
