package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelCreateOperationIsAtomicConcurrentRestartableAndSecretFree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	owner := testkit.NewIdentity(t, "operation-create-owner")
	at := time.Date(2026, 7, 26, 8, 0, 0, 0, time.UTC)
	insertChannelTestNode(t, st.db, owner, at)
	operation := ChannelMutationOperation{Kind: ChannelMutationCreate,
		OperationKeyHash: model.Sum([]byte("create-operation-key")),
		RequestDigest:    model.Sum([]byte("canonical-create-request"))}
	cancelledFixture := testkit.NewSignedChannelForOwnerAt(t,
		"operation-create-cancelled", owner, at)
	cancelledGrantID, _ := model.ParseGrantID("grant-operation-create-cancelled")
	cancelledToken := storeTestEnrollmentToken(t, cancelledFixture.Descriptor(), owner,
		cancelledGrantID, "operation-create-cancelled", at, 7)
	cancelledOperation := ChannelMutationOperation{Kind: ChannelMutationCreate,
		OperationKeyHash: model.Sum([]byte("cancelled-create-operation-key")),
		RequestDigest:    model.Sum([]byte("cancelled-create-request"))}
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.CreateChannel(cancelledCtx, CreateChannelSpec{
		Channel: cancelledFixture.Channel(), Genesis: cancelledFixture.OwnerMember().Member(),
		Token: cancelledToken, Operation: &cancelledOperation}); err == nil {
		t.Fatal("cancelled pre-commit create unexpectedly succeeded")
	}
	var precommitChannels, precommitOperations int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channels`).Scan(&precommitChannels); err != nil ||
		precommitChannels != 0 {
		t.Fatalf("cancelled pre-commit Channel count = %d, %v", precommitChannels, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_mutation_operations`).
		Scan(&precommitOperations); err != nil || precommitOperations != 0 {
		t.Fatalf("cancelled pre-commit operation count = %d, %v", precommitOperations, err)
	}

	type outcome struct {
		result CreateChannelResult
		err    error
		token  model.EnrollmentToken
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	type candidate struct {
		spec  CreateChannelSpec
		token model.EnrollmentToken
	}
	candidates := make([]candidate, 2)
	for index := range candidates {
		fixture := testkit.NewSignedChannelForOwnerAt(t,
			"operation-create-candidate-"+string(rune('a'+index)), owner,
			at.Add(time.Duration(index)*time.Minute))
		grantID, _ := model.ParseGrantID(
			"grant-operation-create-" + string(rune('a'+index)))
		token := storeTestEnrollmentToken(t, fixture.Descriptor(), owner, grantID,
			"operation-create-"+string(rune('a'+index)), fixture.Channel().CreatedAt(), 7)
		candidates[index] = candidate{spec: CreateChannelSpec{Channel: fixture.Channel(),
			Genesis: fixture.OwnerMember().Member(), Token: token, Operation: &operation}, token: token}
	}
	var ready sync.WaitGroup
	ready.Add(2)
	for _, candidate := range candidates {
		candidate := candidate
		go func() {
			ready.Done()
			<-start
			result, err := st.CreateChannel(ctx, candidate.spec)
			outcomes <- outcome{result: result, err: err, token: candidate.token}
		}()
	}
	ready.Wait()
	close(start)
	first, second := <-outcomes, <-outcomes
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent create errors = (%v, %v)", first.err, second.err)
	}
	if first.result.Created == second.result.Created ||
		first.result.Mutation.IsZero() || second.result.Mutation.IsZero() ||
		first.result.Channel.ID() != second.result.Channel.ID() ||
		first.result.GrantID != second.result.GrantID {
		t.Fatalf("concurrent create outcomes = %#v / %#v", first.result, second.result)
	}
	var channelCount, operationCount int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channels`).Scan(&channelCount); err != nil ||
		channelCount != 1 {
		t.Fatalf("Channel count = %d, %v", channelCount, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_mutation_operations`).
		Scan(&operationCount); err != nil || operationCount != 1 {
		t.Fatalf("operation count = %d, %v", operationCount, err)
	}
	var payloadDigestBytes []byte
	if err := st.db.QueryRow(`SELECT token_payload_digest FROM channel_mutation_operations
		WHERE operation_key_hash=?`, operation.OperationKeyHash.Bytes()).
		Scan(&payloadDigestBytes); err != nil {
		t.Fatal(err)
	}

	mismatch := operation
	mismatch.RequestDigest = model.Sum([]byte("changed-create-request"))
	if _, _, err := st.ReadChannelMutation(ctx, mismatch); !errors.Is(err, ErrChannelMutationMismatch) {
		t.Fatalf("same key with changed digest error = %v", err)
	}
	winnerToken := first.token
	if first.result.GrantID != first.token.Payload().GrantID() {
		winnerToken = second.token
	}
	payloadDigest, err := model.DigestFromBytes(payloadDigestBytes)
	if err != nil || payloadDigest != winnerToken.Payload().Digest() {
		t.Fatalf("durable payload commitment = %s, %v",
			payloadDigest.String(), err)
	}
	assertEnrollmentCredentialAbsent(t, path, winnerToken)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	replay, found, err := restarted.ReadChannelMutation(ctx, operation)
	if err != nil || !found || !replay.Replayed() ||
		replay.Channel().ID() != first.result.Channel.ID() ||
		replay.GrantID() != first.result.GrantID ||
		replay.TokenPayloadDigest() != winnerToken.Payload().Digest() {
		t.Fatalf("restart replay = (%#v, %v, %v)", replay, found, err)
	}
}

func TestChannelInviteOperationReplaysOneGrantAndRejectsCrossKindReuse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	fixture := testkit.NewSignedChannel(t, "operation-invite")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	initial := inviteTestGrant(t, fixture, "operation-invite-initial",
		fixture.Channel().CreatedAt(), 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: initial.Token}); err != nil {
		t.Fatal(err)
	}
	at := fixture.Channel().CreatedAt().Add(time.Minute)
	operation := ChannelMutationOperation{Kind: ChannelMutationInvite,
		OperationKeyHash: model.Sum([]byte("invite-operation-key")),
		RequestDigest:    model.Sum([]byte("canonical-invite-request"))}
	first := inviteTestGrant(t, fixture, "operation-invite-first", at, 2)
	second := inviteTestGrant(t, fixture, "operation-invite-second", at, 2)
	results := make(chan RotateChannelInviteResult, 2)
	failures := make(chan error, 2)
	start := make(chan struct{})
	for _, candidate := range []inviteTestCredential{first, second} {
		candidate := candidate
		go func() {
			<-start
			result, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
				ChannelID: fixture.Channel().ID(), Token: candidate.Token,
				At: at, Operation: &operation})
			results <- result
			failures <- err
		}()
	}
	close(start)
	left, right := <-results, <-results
	if err, other := <-failures, <-failures; err != nil || other != nil {
		t.Fatalf("concurrent invite errors = (%v, %v)", err, other)
	}
	if left.Created == right.Created || left.GrantID != right.GrantID ||
		left.Mutation.IsZero() || right.Mutation.IsZero() {
		t.Fatalf("concurrent invite outcomes = %#v / %#v", left, right)
	}
	var grants, open, operations int
	if err := st.db.QueryRow(`SELECT COUNT(*),SUM(status='open') FROM enrollment_grants`).
		Scan(&grants, &open); err != nil || grants != 2 || open != 1 {
		t.Fatalf("grant cardinality = total %d open %d, %v", grants, open, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_mutation_operations`).
		Scan(&operations); err != nil || operations != 1 {
		t.Fatalf("operation cardinality = %d, %v", operations, err)
	}
	crossKind := operation
	crossKind.Kind = ChannelMutationCreate
	if _, _, err := st.ReadChannelMutation(ctx, crossKind); !errors.Is(err, ErrChannelMutationMismatch) {
		t.Fatalf("same key with changed kind error = %v", err)
	}
	winner := first.Token
	if left.GrantID != first.ID() {
		winner = second.Token
	}
	assertEnrollmentCredentialAbsent(t, path, winner)
}

// Crash matrix exercised above:
//   - before commit: cancellation leaves neither operation nor mutation, so retry is fresh;
//   - after commit/before response: the operation row and mutation share one transaction;
//   - response loss/retry: the same key+digest reads the committed semantic result;
//   - restart retry: only the key hash, request digest, result identities, public
//     addresses and verifier are needed; no bearer token or secret is durable.
