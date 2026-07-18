package peer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelEnrollmentHandshakeCommitsResponseLossReplayAndAtomicInstall(t *testing.T) {
	createdAt := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	acceptedAt := createdAt.Add(time.Minute)
	ownerFixture := testkit.NewSignedChannelAt(t, "peer-enrollment-live", createdAt)
	ownerIdentity := ownerFixture.Owner()
	joinerIdentity := testkit.NewIdentity(t, "peer-enrollment-live-joiner")
	ownerPath := filepath.Join(t.TempDir(), "owner", "node.db")
	ownerStore := createEnrollmentTestStore(t, ownerPath, ownerIdentity, createdAt)
	defer ownerStore.Close()
	joinerStore := openEnrollmentTestStore(t, joinerIdentity, createdAt)
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-live")
	token := enrollmentTestToken(t, ownerFixture, grantID, "peer-enrollment-live")
	if _, err := ownerStore.CreateChannel(context.Background(), store.CreateChannelSpec{
		Channel: ownerFixture.Channel(), Genesis: ownerFixture.OwnerMember().Member(), Token: token,
	}); err != nil {
		t.Fatal(err)
	}

	ownerHost := newEnrollmentTestHost(t, ownerIdentity)
	defer ownerHost.Close()
	joinerHost := newEnrollmentTestHost(t, joinerIdentity)
	defer joinerHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: ownerHost.ID(),
		Addrs: ownerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Store: ownerStore, Signer: enrollmentTestSigner{privateKey: enrollmentPrivateKey(t, ownerIdentity)},
		Clock:  fixedEnrollmentClock{at: acceptedAt},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x31}, model.EnrollmentNonceBytes*4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerHost.SetStreamHandler(ChannelProtocol, ownerProtocol.Handler(ctx))
	spec := JoinChannelSpec{Token: token, DisplayLabel: joinerIdentity.DisplayName(),
		AdvertisedMultiaddrs: joinerIdentity.Multiaddrs(),
		LocalAlias:           "review-team"}

	// The owner commits, but the first joiner cannot persist the response. A
	// retry with the same request and fresh nonce must recover the one receipt.
	firstClient, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{
		Store: failingEnrollmentInstallStore{delegate: joinerStore,
			err: errors.New("injected response loss before install")},
		Clock:  fixedEnrollmentClock{at: acceptedAt.Add(time.Second)},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, model.EnrollmentNonceBytes+channelRequestIDBytes)),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstStream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	if result, err := firstClient.Join(ctx, firstStream, spec); err == nil || result.Installed {
		t.Fatalf("lost-response join = (%#v, %v)", result, err)
	}

	retryClient, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{Store: joinerStore,
		Clock: fixedEnrollmentClock{at: acceptedAt.Add(2 * time.Second)},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x42},
			(model.EnrollmentNonceBytes+channelRequestIDBytes)*2))})
	if err != nil {
		t.Fatal(err)
	}
	retryStream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	installed, err := retryClient.Join(ctx, retryStream, spec)
	if err != nil || !installed.Installed || installed.Status != store.ChannelEnrollmentAccepted ||
		installed.Channel.ID() != ownerFixture.Channel().ID() ||
		installed.Channel.LocalAlias() != spec.LocalAlias || installed.Roster.Head().Revision() != 2 {
		t.Fatalf("recovered join = (%#v, %v)", installed, err)
	}
	current, ok := installed.Roster.CurrentMember(joinerIdentity.PeerID())
	if !ok || current.Status() != model.MemberActive || current.OriginEpoch() != joinerIdentity.OriginEpoch() {
		t.Fatalf("installed local member = (%#v, %t)", current, ok)
	}

	// A further byte-equivalent operation is a local replica replay, not a
	// second membership, grant use, receipt, or binding installation.
	replayStream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	replayed, err := retryClient.Join(ctx, replayStream, spec)
	if err != nil || replayed.Installed || replayed.Status != store.ChannelEnrollmentReplayed ||
		replayed.Roster.Head() != installed.Roster.Head() {
		t.Fatalf("installed replay = (%#v, %v)", replayed, err)
	}
}

func TestChannelEnrollmentRecoversOwnerCommitAfterAcceptedResponseLoss(t *testing.T) {
	createdAt := time.Date(2026, 7, 18, 1, 30, 0, 0, time.UTC)
	acceptedAt := createdAt.Add(time.Minute)
	ownerFixture := testkit.NewSignedChannelAt(t, "peer-enrollment-response-loss", createdAt)
	ownerIdentity := ownerFixture.Owner()
	joinerIdentity := testkit.NewIdentity(t, "peer-enrollment-response-loss-joiner")
	ownerPath := filepath.Join(t.TempDir(), "owner", "node.db")
	ownerStore := createEnrollmentTestStore(t, ownerPath, ownerIdentity, createdAt)
	defer ownerStore.Close()
	joinerPath := filepath.Join(t.TempDir(), "joiner", "node.db")
	joinerStore := createEnrollmentTestStore(t, joinerPath, joinerIdentity, createdAt)
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-response-loss")
	token := enrollmentTestToken(t, ownerFixture, grantID, "peer-enrollment-response-loss")
	if _, err := ownerStore.CreateChannel(context.Background(), store.CreateChannelSpec{
		Channel: ownerFixture.Channel(), Genesis: ownerFixture.OwnerMember().Member(), Token: token,
	}); err != nil {
		t.Fatal(err)
	}

	ownerHost := newEnrollmentTestHost(t, ownerIdentity)
	defer ownerHost.Close()
	joinerHost := newEnrollmentTestHost(t, joinerIdentity)
	defer joinerHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: ownerHost.ID(),
		Addrs: ownerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	barrier := &committedEnrollmentOwnerStore{delegate: ownerStore,
		committed: make(chan struct{}), resume: make(chan struct{})}
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Store: barrier, Signer: enrollmentTestSigner{privateKey: enrollmentPrivateKey(t, ownerIdentity)},
		Clock:  fixedEnrollmentClock{at: acceptedAt},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x33}, model.EnrollmentNonceBytes*4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerHost.SetStreamHandler(ChannelProtocol, ownerProtocol.Handler(ctx))
	firstClient, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{
		Store: joinerStore, Clock: fixedEnrollmentClock{at: acceptedAt.Add(time.Second)},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x43}, model.EnrollmentNonceBytes+channelRequestIDBytes)),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstSpec := JoinChannelSpec{Token: token, DisplayLabel: joinerIdentity.DisplayName(),
		AdvertisedMultiaddrs: joinerIdentity.Multiaddrs(), LocalAlias: "response-loss-team"}
	firstStream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	type joinResult struct {
		result store.InstallJoinedChannelResult
		err    error
	}
	resultC := make(chan joinResult, 1)
	go func() {
		result, joinErr := firstClient.Join(ctx, firstStream, firstSpec)
		resultC <- joinResult{result: result, err: joinErr}
	}()
	select {
	case <-barrier.committed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := firstStream.Reset(); err != nil && !errors.Is(err, network.ErrReset) {
		t.Fatalf("reset committed response stream: %v", err)
	}
	close(barrier.resume)
	first := <-resultC
	if !errors.Is(first.err, ErrChannelEnrollmentOutcomeUnknown) || first.result.Installed {
		t.Fatalf("lost Accepted response = (%#v,%v)", first.result, first.err)
	}
	if err := joinerStore.Close(); err != nil {
		t.Fatal(err)
	}
	joinerDB, err := sql.Open("sqlite", joinerPath)
	if err != nil {
		t.Fatal(err)
	}
	var state string
	if err := joinerDB.QueryRow(`SELECT state FROM channel_join_reservations`).Scan(&state); err != nil ||
		state != "commit_unknown" {
		_ = joinerDB.Close()
		t.Fatalf("response-loss reservation state = (%q,%v)", state, err)
	}
	if err := joinerDB.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(context.Background(), joinerPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	retryClient, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{
		Store: reopened, Clock: fixedEnrollmentClock{at: acceptedAt.Add(2 * time.Second)},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x44}, model.EnrollmentNonceBytes+channelRequestIDBytes)),
	})
	if err != nil {
		t.Fatal(err)
	}
	retryStream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	retrySpec := firstSpec
	retrySpec.DisplayLabel = "renamed-after-restart"
	retrySpec.AdvertisedMultiaddrs = []string{"/ip4/127.0.0.1/tcp/45555"}
	installed, err := retryClient.Join(ctx, retryStream, retrySpec)
	if err != nil || !installed.Installed || installed.Roster.Head().Revision() != 2 {
		t.Fatalf("response-loss recovery = (%#v,%v)", installed, err)
	}
	assertEnrollmentDatabaseCounts(t, ownerPath, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
	})
}

func TestChannelEnrollmentClientRejectsWrongSecureOwner(t *testing.T) {
	createdAt := time.Date(2026, 7, 18, 2, 0, 0, 0, time.UTC)
	ownerFixture := testkit.NewSignedChannelAt(t, "peer-enrollment-owner", createdAt)
	joiner := testkit.NewIdentity(t, "peer-enrollment-owner-joiner")
	wrongOwner := testkit.NewIdentity(t, "peer-enrollment-wrong-owner")
	joinerStore := openEnrollmentTestStore(t, joiner, createdAt)
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-owner")
	token := enrollmentTestToken(t, ownerFixture, grantID, "peer-enrollment-owner")
	joinerHost := newEnrollmentTestHost(t, joiner)
	defer joinerHost.Close()
	wrongHost := newEnrollmentTestHost(t, wrongOwner)
	defer wrongHost.Close()
	wrongHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, _ = io.Copy(io.Discard, stream)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: wrongHost.ID(),
		Addrs: wrongHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	client, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{Store: joinerStore,
		Clock:  fixedEnrollmentClock{at: createdAt.Add(time.Minute)},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x51}, model.EnrollmentNonceBytes))})
	if err != nil {
		t.Fatal(err)
	}
	stream := openEnrollmentTestStream(t, ctx, joinerHost, wrongHost.ID())
	_, err = client.Join(ctx, stream, JoinChannelSpec{Token: token, DisplayLabel: joiner.DisplayName(),
		AdvertisedMultiaddrs: joiner.Multiaddrs(), LocalAlias: "wrong-owner"})
	var failure *ChannelProtocolFailure
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorWrongOwner || failure.Retryable() {
		t.Fatalf("wrong secure owner error = %#v", err)
	}
}

func TestChannelEnrollmentOwnerRejectsUnsupportedVersionBeforeStore(t *testing.T) {
	fixture := testkit.NewSignedChannel(t, "peer-enrollment-version")
	joiner := testkit.NewIdentity(t, "peer-enrollment-version-joiner")
	ownerHost := newEnrollmentTestHost(t, fixture.Owner())
	defer ownerHost.Close()
	joinerHost := newEnrollmentTestHost(t, joiner)
	defer joinerHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: ownerHost.ID(),
		Addrs: ownerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Store:  unexpectedEnrollmentOwnerStore{called: called},
		Signer: enrollmentTestSigner{privateKey: enrollmentPrivateKey(t, fixture.Owner())},
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerHost.SetStreamHandler(ChannelProtocol, ownerProtocol.Handler(ctx))
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-version")
	identity, err := model.EnrollmentJoinIdentityDigest(fixture.Channel().ID(), grantID,
		joiner.PeerID(), joiner.PublicKey(), joiner.OriginEpoch())
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: fixture.Channel().ID(), GrantID: grantID,
		EnrollmentRequestID: requestID,
		JoinerNonce:         bytes.Repeat([]byte{0x55}, model.EnrollmentNonceBytes),
		SupportedVersions:   []uint8{2}, OriginEpoch: joiner.OriginEpoch(),
		DisplayLabel: joiner.DisplayName(), AdvertisedMultiaddrs: joiner.Multiaddrs()})
	if err != nil {
		t.Fatal(err)
	}
	frameRequestID, err := ParseChannelRequestID("channel-request-303132333435363738393a3b3c3d3e3f")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewChannelFrame(frameRequestID, init)
	if err != nil {
		t.Fatal(err)
	}
	stream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	defer stream.Close()
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadChannelFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	failure, ok := response.Payload().(ProtocolError)
	if response.RequestID() != frameRequestID || !ok || failure.Code() != ChannelErrorIncompatibleProtocol ||
		failure.Retryable() {
		t.Fatalf("unsupported-version response = %#v", response)
	}
	select {
	case <-called:
		t.Fatal("unsupported version reached owner Store")
	default:
	}
}

func TestChannelEnrollmentMapsOnlyTypedStableStoreFailures(t *testing.T) {
	tests := []struct {
		cause      error
		code       ChannelProtocolErrorCode
		retryAfter time.Duration
	}{
		{store.ErrChannelEnrollmentOwner, ChannelErrorWrongOwner, 0},
		{store.ErrChannelEnrollmentProof, ChannelErrorBadProof, 0},
		{store.ErrChannelEnrollmentTokenExpired, ChannelErrorTokenExpired, 0},
		{store.ErrChannelEnrollmentTokenClosed, ChannelErrorTokenClosed, 0},
		{store.ErrChannelEnrollmentTokenExhausted, ChannelErrorTokenExhausted, 0},
		{store.ErrChannelFull, ChannelErrorChannelFull, 0},
		{store.ErrChannelEnrollmentChannelClosed, ChannelErrorChannelClosed, 0},
		{store.ErrChannelEnrollmentMemberRevoked, ChannelErrorMemberRevoked, 0},
		{store.ErrChannelEnrollmentStale, ChannelErrorRosterGap, channelEnrollmentGapRetry},
		{store.ErrChannelEnrollmentConflict, ChannelErrorRosterConflict, 0},
		{store.ErrChannelEnrollmentUnavailable, ChannelErrorInvalidToken, 0},
	}
	for _, test := range tests {
		code, retryAfter, ok := channelStoreFailure(fmtWrappedEnrollmentError{test.cause})
		if !ok || code != test.code || retryAfter != test.retryAfter {
			t.Errorf("channelStoreFailure(%v) = (%q,%s,%t)", test.cause, code, retryAfter, ok)
		}
	}
	if code, retryAfter, ok := channelStoreFailure(errors.New("sqlite path and secret detail")); ok || code != "" || retryAfter != 0 {
		t.Fatalf("untyped Store failure leaked to wire = (%q,%s,%t)", code, retryAfter, ok)
	}
	if code, retryAfter, ok := channelStoreFailure(context.DeadlineExceeded); ok || code != "" || retryAfter != 0 {
		t.Fatalf("ambiguous Store timeout leaked as a rejection = (%q,%s,%t)", code, retryAfter, ok)
	}
}

func TestChannelEnrollmentConstructorsRejectMissingControlAuthority(t *testing.T) {
	if owner, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{}); owner != nil || err == nil {
		t.Fatalf("empty owner = (%#v,%v)", owner, err)
	}
	if client, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{}); client != nil || err == nil {
		t.Fatalf("empty client = (%#v,%v)", client, err)
	}
	var failure *ChannelProtocolFailure
	err := enrollmentTransportFailure(io.EOF)
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorOwnerUnreachable ||
		!failure.Retryable() || failure.RetryAfter() != channelEnrollmentBusyRetry ||
		!errors.Is(err, ErrChannelEnrollmentProtocol) {
		t.Fatalf("transport failure = %#v", err)
	}
	err = enrollmentTransportFailure(channelFrameError("authenticated malformed response", nil))
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorRosterConflict || failure.Retryable() {
		t.Fatalf("malformed authenticated response = %#v", err)
	}
	if err := enrollmentPrecommitTransportFailure(canceledEnrollmentContext(), io.EOF); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled precommit transport = %v", err)
	}
	if err := enrollmentOutcomeUnknown(network.ErrReset); !errors.Is(err, ErrChannelEnrollmentOutcomeUnknown) {
		t.Fatalf("post-proof response loss = %v", err)
	}
}

func canceledEnrollmentContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

type fixedEnrollmentClock struct{ at time.Time }

func (clock fixedEnrollmentClock) Now() time.Time { return clock.at }

type enrollmentTestSigner struct{ privateKey ed25519.PrivateKey }

func (signer enrollmentTestSigner) Sign(ctx context.Context, message []byte) ([]byte, error) {
	if ctx == nil || ctx.Err() != nil || len(signer.privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("test signer unavailable")
	}
	return ed25519.Sign(signer.privateKey, message), nil
}

type committedEnrollmentOwnerStore struct {
	delegate  *store.Store
	committed chan struct{}
	resume    chan struct{}
	once      sync.Once
}

type unexpectedEnrollmentOwnerStore struct{ called chan struct{} }

func (unexpected unexpectedEnrollmentOwnerStore) PrepareChannelEnrollment(context.Context,
	store.PrepareChannelEnrollmentSpec,
) (store.PrepareChannelEnrollmentResult, error) {
	select {
	case unexpected.called <- struct{}{}:
	default:
	}
	return store.PrepareChannelEnrollmentResult{}, store.ErrChannelEnrollmentInput
}

func (unexpected unexpectedEnrollmentOwnerStore) AcceptChannelEnrollment(context.Context,
	store.AcceptChannelEnrollmentSpec,
) (store.AcceptChannelEnrollmentResult, error) {
	select {
	case unexpected.called <- struct{}{}:
	default:
	}
	return store.AcceptChannelEnrollmentResult{}, store.ErrChannelEnrollmentInput
}

func (barrier *committedEnrollmentOwnerStore) PrepareChannelEnrollment(ctx context.Context,
	spec store.PrepareChannelEnrollmentSpec,
) (store.PrepareChannelEnrollmentResult, error) {
	return barrier.delegate.PrepareChannelEnrollment(ctx, spec)
}

func (barrier *committedEnrollmentOwnerStore) AcceptChannelEnrollment(ctx context.Context,
	spec store.AcceptChannelEnrollmentSpec,
) (store.AcceptChannelEnrollmentResult, error) {
	result, err := barrier.delegate.AcceptChannelEnrollment(ctx, spec)
	if err == nil {
		barrier.once.Do(func() {
			close(barrier.committed)
			<-barrier.resume
		})
	}
	return result, err
}

type failingEnrollmentInstallStore struct {
	delegate *store.Store
	err      error
}

func (failure failingEnrollmentInstallStore) PrepareJoinedChannel(ctx context.Context,
	spec store.PrepareJoinedChannelSpec,
) (store.PrepareJoinedChannelResult, error) {
	return failure.delegate.PrepareJoinedChannel(ctx, spec)
}

func (failure failingEnrollmentInstallStore) MarkJoinedChannelCommitUnknown(ctx context.Context,
	requestID model.EnrollmentRequestID, peerID model.PeerID, attempt uint64, at time.Time,
) error {
	return failure.delegate.MarkJoinedChannelCommitUnknown(ctx, requestID, peerID, attempt, at)
}

func (failure failingEnrollmentInstallStore) ReleaseJoinedChannelReservation(ctx context.Context,
	requestID model.EnrollmentRequestID, peerID model.PeerID, attempt uint64,
) error {
	return failure.delegate.ReleaseJoinedChannelReservation(ctx, requestID, peerID, attempt)
}

func (failure failingEnrollmentInstallStore) InstallJoinedChannel(context.Context,
	store.InstallJoinedChannelSpec,
) (store.InstallJoinedChannelResult, error) {
	return store.InstallJoinedChannelResult{}, failure.err
}

type fmtWrappedEnrollmentError struct{ cause error }

func (wrapped fmtWrappedEnrollmentError) Error() string { return "wrapped durable failure" }
func (wrapped fmtWrappedEnrollmentError) Unwrap() error { return wrapped.cause }

func openEnrollmentTestStore(t *testing.T, identity testkit.Identity, at time.Time) *store.Store {
	t.Helper()
	workspace := t.TempDir()
	st := createEnrollmentTestStore(t, filepath.Join(workspace, "node", "node.db"), identity, at)
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close enrollment Store: %v", err)
		}
	})
	return st
}

func createEnrollmentTestStore(t *testing.T, path string, identity testkit.Identity,
	at time.Time,
) *store.Store {
	t.Helper()
	workspace := filepath.Dir(filepath.Dir(path))
	st, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(),
		OriginEpoch: identity.OriginEpoch(), NextOriginSequence: 1,
		ActiveAssetRevision: "asset-r5-enrollment", CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-" + identity.PeerID().String(), WorkspaceRoot: workspace,
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("credential-" + identity.PeerID().String())),
		ActiveAssetRevision: "asset-r5-enrollment", HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: false, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), nodeValue, profile); err != nil {
		t.Fatal(err)
	}
	return st
}

func assertEnrollmentDatabaseCounts(t *testing.T, path string, expected map[string]int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for table, want := range expected {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != want {
			t.Errorf("%s row count = (%d,%v), want %d", table, count, err, want)
		}
	}
}

func enrollmentTestToken(t *testing.T, fixture *testkit.SignedChannel, grantID model.GrantID,
	seed string,
) model.EnrollmentToken {
	t.Helper()
	secret := model.Sum([]byte("peer-enrollment-token-secret:" + seed)).Bytes()
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: fixture.Descriptor(), OwnerMultiaddrs: fixture.Owner().Multiaddrs(),
		GrantID: grantID, BearerSecret: secret, ExpiresAt: fixture.Channel().CreatedAt().Add(time.Hour),
		MaxUses:            fixture.Channel().MemberLimit() - 1,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentTokenSigningMessage(fixture.Channel().ID(), payload.Digest())
	if err != nil {
		t.Fatal(err)
	}
	token, err := model.AttachEnrollmentTokenSignature(payload,
		ed25519.Sign(enrollmentPrivateKey(t, fixture.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func enrollmentPrivateKey(t *testing.T, identity testkit.Identity) ed25519.PrivateKey {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil || len(raw) != ed25519.PrivateKeySize {
		t.Fatalf("raw enrollment private key = %d bytes, %v", len(raw), err)
	}
	return ed25519.PrivateKey(append([]byte(nil), raw...))
}

func newEnrollmentTestHost(t *testing.T, identity testkit.Identity) host.Host {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	nodeHost, err := libp2p.New(libp2p.Identity(privateKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	return nodeHost
}

func openEnrollmentTestStream(t *testing.T, ctx context.Context, local host.Host,
	remote libp2ppeer.ID,
) network.Stream {
	t.Helper()
	stream, err := local.NewStream(ctx, remote, ChannelProtocol)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}
