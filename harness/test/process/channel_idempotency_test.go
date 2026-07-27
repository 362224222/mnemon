//go:build darwin || linux

package process_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	_ "modernc.org/sqlite"
)

type channelIdempotencyJournalWire struct {
	SchemaVersion     int     `json:"schema_version"`
	OperationKey      string  `json:"operation_key"`
	RequestDigest     string  `json:"request_digest"`
	ContextFileDigest *string `json:"context_file_digest"`
	CreatedAt         string  `json:"created_at"`
}

type channelIdempotencyJournal struct {
	base          string
	raw           []byte
	info          os.FileInfo
	operationHash model.Digest
	requestDigest model.Digest
}

// TestPublicChannelCreateAndInviteReplayAfterResponseLossAndRestart proves the
// public CLI commit/presentation boundary with ordinary mnemon-harness and
// mnemond processes. A read-only stdout descriptor loses each accepted
// response after the CLI has durably marked its operation terminal; the exact
// public command then recovers it after a daemon restart.
func TestPublicChannelCreateAndInviteReplayAfterResponseLossAndRestart(t *testing.T) {
	fixture := channelProcessSetupFixture(t)
	owner := fixture.nodes["A"]

	createRequest := localapi.ChannelCreateRequest{Name: "Replay"}
	createDigest, apiErr := localapi.ChannelCreateRequestDigest(createRequest)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	channelIdempotencyLoseResponse(t, fixture.harnessExecutable, owner, "create",
		"channel", "create", createRequest.Name, "--json")
	createTerminal := channelIdempotencyReadJournal(t, owner.nodeState,
		createDigest, ".terminal")

	op01StopProcessNode(t, owner)
	owner.autoMayRun = true
	createdResult := channelProcessRun(t, fixture.harnessExecutable, owner,
		"channel", "create", createRequest.Name, "--json")
	created, err := channelProcessDecode[localapi.ChannelCreateResponse](createdResult)
	if err != nil || created.SchemaVersion != 1 || created.Status != "created" ||
		created.Channel.Alias != "replay" || created.Channel.Name != createRequest.Name ||
		created.Channel.RosterRevision != 1 {
		t.Fatalf("public create recovery = (status=%q alias=%q revision=%d, %v)",
			created.Status, created.Channel.Alias, created.Channel.RosterRevision, err)
	}
	createPresented := channelIdempotencyReadJournal(t, owner.nodeState,
		createDigest, ".presented")
	channelIdempotencyAssertJournalTransition(t, createTerminal, createPresented)

	inviteRequest := localapi.ChannelInviteRequest{
		Channel: "replay", ExpiresSeconds: int64(time.Hour / time.Second), Uses: 2,
	}
	inviteDigest, apiErr := localapi.ChannelInviteRequestDigest(inviteRequest)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	channelIdempotencyLoseResponse(t, fixture.harnessExecutable, owner, "invite",
		"channel", "invite", "--channel", inviteRequest.Channel,
		"--expires", "1h", "--uses", "2", "--json")
	inviteTerminal := channelIdempotencyReadJournal(t, owner.nodeState,
		inviteDigest, ".terminal")

	op01StopProcessNode(t, owner)
	owner.autoMayRun = true
	invitedResult := channelProcessRun(t, fixture.harnessExecutable, owner,
		"channel", "invite", "--channel", inviteRequest.Channel,
		"--expires", "1h", "--uses", "2", "--json")
	invited, err := channelProcessDecode[localapi.ChannelInviteResponse](invitedResult)
	if err != nil || invited.SchemaVersion != 1 || invited.Status != "created" ||
		invited.Channel.Alias != "replay" || invited.Channel.RosterRevision != 1 ||
		invited.Invite.RemainingUses != inviteRequest.Uses || invited.Invite.Status != "open" {
		t.Fatalf("public invite recovery = (status=%q alias=%q revision=%d uses=%d, %v)",
			invited.Status, invited.Channel.Alias, invited.Channel.RosterRevision,
			invited.Invite.RemainingUses, err)
	}
	invitePresented := channelIdempotencyReadJournal(t, owner.nodeState,
		inviteDigest, ".presented")
	channelIdempotencyAssertJournalTransition(t, inviteTerminal, invitePresented)

	channelProcessWaitChannel(t, fixture.harnessExecutable, owner, "replay",
		func(view localapi.ChannelView) error {
			if view.Membership != "active" || view.RosterRevision != 1 ||
				len(view.Members) != 1 || !view.Owner.Local {
				return errors.New("replayed mutations changed the one-member Channel")
			}
			return nil
		})

	op01StopProcessNode(t, owner)
	channelIdempotencyAssertMutationCounts(t, owner.nodeState,
		createTerminal, createDigest, inviteTerminal, inviteDigest)
	channelIdempotencyAssertBearerAbsent(t, owner.workspace,
		created.InviteToken, invited.InviteToken)
}

func channelIdempotencyLoseResponse(t *testing.T, executable string,
	node *channelProcessNode, label string, arguments ...string,
) {
	t.Helper()
	sinkPath := filepath.Join(node.workspace, ".response-loss-"+label)
	sentinel := []byte("read-only response-loss sink\n")
	if err := os.WriteFile(sinkPath, sentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	sink, err := os.Open(sinkPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stderr := newSetupProcessOutput(commandOutputMax)
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Dir = node.workspace
	command.Env = append([]string(nil), node.environment...)
	command.Stdin = nil
	command.Stdout = sink
	command.Stderr = stderr
	command.WaitDelay = 250 * time.Millisecond
	runErr := command.Run()
	var exitErr *exec.ExitError
	if ctx.Err() != nil || !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 1 ||
		stderr.overflowed() || len(stderr.bytes()) != 0 {
		t.Fatalf("%s response-loss process = context=%v exit=%v stderr=%s overflow=%t",
			label, ctx.Err(), runErr, setupProcessFingerprint(stderr.bytes()),
			stderr.overflowed())
	}
	raw, err := os.ReadFile(sinkPath)
	if err != nil || !bytes.Equal(raw, sentinel) {
		t.Fatalf("%s response reached the read-only sink: %v", label, err)
	}
}

func channelIdempotencyReadJournal(t *testing.T, nodeState string,
	requestDigest model.Digest, suffix string,
) channelIdempotencyJournal {
	t.Helper()
	operations := filepath.Join(nodeState, "operations")
	entries, err := os.ReadDir(operations)
	if err != nil {
		t.Fatal(err)
	}
	var matches []channelIdempotencyJournal
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}
		journal, matchesRequest := channelIdempotencyParseJournal(
			t, operations, entry, requestDigest, suffix,
		)
		if matchesRequest {
			matches = append(matches, journal)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("%s journals for typed request digest = %d, want 1", suffix, len(matches))
	}
	return matches[0]
}

func channelIdempotencyParseJournal(t *testing.T, operations string, entry fs.DirEntry,
	requestDigest model.Digest, suffix string,
) (channelIdempotencyJournal, bool) {
	t.Helper()
	path := filepath.Join(operations, entry.Name())
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 {
		t.Fatalf("inspect %s journal: %v", suffix, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire channelIdempotencyJournalWire
	if err := decoder.Decode(&wire); err != nil {
		t.Fatalf("decode %s journal: %v", suffix, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("%s journal contains trailing content", suffix)
	}
	canonical, err := model.CanonicalMarshal(wire)
	if err != nil || !bytes.Equal(raw, canonical) || wire.SchemaVersion != 1 ||
		wire.ContextFileDigest != nil {
		t.Fatalf("%s journal is not a closed Channel mutation record", suffix)
	}
	digest, err := model.ParseDigest(wire.RequestDigest)
	if err != nil {
		t.Fatalf("%s journal request digest is invalid", suffix)
	}
	if digest != requestDigest {
		return channelIdempotencyJournal{}, false
	}
	key, err := base64.RawURLEncoding.DecodeString(wire.OperationKey)
	if err != nil || len(key) != 32 ||
		base64.RawURLEncoding.EncodeToString(key) != wire.OperationKey {
		t.Fatalf("%s journal operation key is invalid", suffix)
	}
	operationHash := model.Sum(key)
	clear(key)
	if _, err := time.Parse(time.RFC3339Nano, wire.CreatedAt); err != nil {
		t.Fatalf("%s journal creation time is invalid", suffix)
	}
	return channelIdempotencyJournal{
		base: strings.TrimSuffix(entry.Name(), suffix), raw: raw, info: info,
		operationHash: operationHash, requestDigest: digest,
	}, true
}

func channelIdempotencyAssertJournalTransition(t *testing.T,
	terminal, presented channelIdempotencyJournal,
) {
	t.Helper()
	if terminal.base != presented.base || terminal.operationHash != presented.operationHash ||
		terminal.requestDigest != presented.requestDigest ||
		!bytes.Equal(terminal.raw, presented.raw) ||
		!os.SameFile(terminal.info, presented.info) {
		t.Fatal("response recovery did not preserve the exact operation journal identity")
	}
}

func channelIdempotencyAssertMutationCounts(t *testing.T, nodeState string,
	create channelIdempotencyJournal, createDigest model.Digest,
	invite channelIdempotencyJournal, inviteDigest model.Digest,
) {
	t.Helper()
	databasePath := filepath.Join(nodeState, "node.db")
	database, err := sql.Open("sqlite", "file:"+databasePath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var channels, grants, openGrants, closedGrants, mutations int
	for _, query := range []struct {
		statement string
		target    *int
	}{
		{"SELECT COUNT(*) FROM channels", &channels},
		{"SELECT COUNT(*) FROM enrollment_grants", &grants},
		{"SELECT COUNT(*) FROM enrollment_grants WHERE status='open'", &openGrants},
		{"SELECT COUNT(*) FROM enrollment_grants WHERE status='closed'", &closedGrants},
		{"SELECT COUNT(*) FROM channel_mutation_operations", &mutations},
	} {
		if err := database.QueryRow(query.statement).Scan(query.target); err != nil {
			t.Fatal(err)
		}
	}
	if channels != 1 || grants != 2 || openGrants != 1 || closedGrants != 1 ||
		mutations != 2 {
		t.Fatalf("durable mutation cardinality = channels %d grants %d (%d open/%d closed) "+
			"operations %d", channels, grants, openGrants, closedGrants, mutations)
	}

	rows, err := database.Query(`SELECT kind,operation_key_hash,request_digest
		FROM channel_mutation_operations ORDER BY kind`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]struct {
		operation model.Digest
		request   model.Digest
	}{
		"create": {create.operationHash, createDigest},
		"invite": {invite.operationHash, inviteDigest},
	}
	seen := make(map[string]bool, len(want))
	for rows.Next() {
		var kind string
		var operationRaw, requestRaw []byte
		if err := rows.Scan(&kind, &operationRaw, &requestRaw); err != nil {
			t.Fatal(err)
		}
		expected, ok := want[kind]
		if !ok || seen[kind] || !bytes.Equal(operationRaw, expected.operation.Bytes()) ||
			!bytes.Equal(requestRaw, expected.request.Bytes()) {
			t.Fatal("durable mutation operation differs from the public replay journal")
		}
		seen[kind] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(want) {
		t.Fatalf("durable mutation kinds = %d, want %d", len(seen), len(want))
	}
}

func channelIdempotencyAssertBearerAbsent(t *testing.T, root string, encoded ...string) {
	t.Helper()
	var needles [][]byte
	for _, value := range encoded {
		token, err := model.ParseEnrollmentToken(value)
		if err != nil {
			t.Fatal(err)
		}
		bearer := token.Payload().BearerSecret()
		needles = append(needles, []byte(value), bearer,
			token.Payload().RevealCanonicalJSON().Bytes())
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, needle := range needles {
			if len(needle) != 0 && bytes.Contains(raw, needle) {
				return fmt.Errorf("plaintext enrollment bearer persisted in %s",
					filepath.Base(path))
			}
		}
		return nil
	})
	for _, needle := range needles {
		clear(needle)
	}
	if err != nil {
		t.Fatal(err)
	}
}
