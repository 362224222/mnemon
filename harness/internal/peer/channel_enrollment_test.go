package peer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
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
	ownerHost := newEnrollmentTestHostAtIdentityAddress(t, ownerIdentity)
	defer ownerHost.Close()
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-live")
	token := enrollmentTestTokenForHost(t, ownerFixture, grantID,
		"peer-enrollment-live", ownerHost)
	if _, err := ownerStore.CreateChannel(context.Background(), store.CreateChannelSpec{
		Channel: ownerFixture.Channel(), Genesis: ownerFixture.OwnerMember().Member(), Token: token,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime := newTestMeshRuntime(t, ctx, joinerIdentity, readMeshRuntimeAuthority(t, joinerStore))
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Controller: enrollmentOwnerTestControllerFor(t, ownerStore, ownerIdentity),
		Clock:      fixedEnrollmentClock{at: acceptedAt},
		Random:     bytes.NewReader(bytes.Repeat([]byte{0x31}, model.EnrollmentNonceBytes*4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	registerEnrollmentTestDispatcher(t, ctx, ownerHost, ownerProtocol)
	spec := JoinChannelSpec{Token: token, DisplayLabel: joinerIdentity.DisplayName(),
		AdvertisedMultiaddrs: joinerIdentity.Multiaddrs(),
		LocalAlias:           "review-team"}

	// The owner commits, but the first joiner cannot persist the response. A
	// retry with the same request and fresh nonce must recover the one receipt.
	firstSession := &enrollmentJoinStoreSession{store: joinerStore,
		installErr: errors.New("injected response loss before install")}
	if result, err := runtime.JoinChannel(ctx, spec, firstSession); err == nil || result.Installed() {
		t.Fatalf("lost-response join = (%#v, %v)", result, err)
	}

	installed, err := runtime.JoinChannel(ctx, spec,
		&enrollmentJoinStoreSession{store: joinerStore})
	if err != nil || !installed.Installed() || installed.Status() != ChannelEnrollmentAccepted ||
		installed.Channel().ID() != ownerFixture.Channel().ID() ||
		installed.Channel().LocalAlias() != spec.LocalAlias || installed.Roster().Head().Revision() != 2 {
		t.Fatalf("recovered join = (%#v, %v)", installed, err)
	}
	current, ok := installed.Roster().CurrentMember(joinerIdentity.PeerID())
	if !ok || current.Status() != model.MemberActive || current.OriginEpoch() != joinerIdentity.OriginEpoch() {
		t.Fatalf("installed local member = (%#v, %t)", current, ok)
	}

	// A further byte-equivalent operation is a local replica replay, not a
	// second membership, grant use, receipt, or binding installation.
	replayed, err := runtime.JoinChannel(ctx, spec,
		&enrollmentJoinStoreSession{store: joinerStore})
	if err != nil || replayed.Installed() || replayed.Status() != ChannelEnrollmentReplayed ||
		replayed.Roster().Head() != installed.Roster().Head() {
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
	ownerHost := newEnrollmentTestHostAtIdentityAddress(t, ownerIdentity)
	defer ownerHost.Close()
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-response-loss")
	token := enrollmentTestTokenForHost(t, ownerFixture, grantID,
		"peer-enrollment-response-loss", ownerHost)
	if _, err := ownerStore.CreateChannel(context.Background(), store.CreateChannelSpec{
		Channel: ownerFixture.Channel(), Genesis: ownerFixture.OwnerMember().Member(), Token: token,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime := newTestMeshRuntime(t, ctx, joinerIdentity, readMeshRuntimeAuthority(t, joinerStore))
	barrier := &committedEnrollmentOwnerStore{delegate: ownerStore,
		committed: make(chan struct{}), resume: make(chan struct{})}
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Controller: enrollmentOwnerTestControllerFor(t, barrier, ownerIdentity),
		Clock:      fixedEnrollmentClock{at: acceptedAt},
		Random:     bytes.NewReader(bytes.Repeat([]byte{0x33}, model.EnrollmentNonceBytes*4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	registerEnrollmentTestDispatcher(t, ctx, ownerHost, ownerProtocol)
	firstSpec := JoinChannelSpec{Token: token, DisplayLabel: joinerIdentity.DisplayName(),
		AdvertisedMultiaddrs: joinerIdentity.Multiaddrs(), LocalAlias: "response-loss-team"}
	type joinResult struct {
		result ChannelJoinResult
		err    error
	}
	resultC := make(chan joinResult, 1)
	go func() {
		result, joinErr := runtime.JoinChannel(ctx, firstSpec,
			&enrollmentJoinStoreSession{store: joinerStore})
		resultC <- joinResult{result: result, err: joinErr}
	}()
	select {
	case <-barrier.committed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := ownerHost.Network().ClosePeer(runtime.managedRuntimeHost().ID()); err != nil {
		t.Fatalf("close committed response connection: %v", err)
	}
	close(barrier.resume)
	first := <-resultC
	if !errors.Is(first.err, ErrChannelEnrollmentOutcomeUnknown) || first.result.Installed() {
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
	retrySpec := firstSpec
	retrySpec.DisplayLabel = "renamed-after-restart"
	retrySpec.AdvertisedMultiaddrs = []string{"/ip4/127.0.0.1/tcp/45555"}
	installed, err := runtime.JoinChannel(ctx, retrySpec,
		&enrollmentJoinStoreSession{store: reopened})
	if err != nil || !installed.Installed() || installed.Roster().Head().Revision() != 2 {
		t.Fatalf("response-loss recovery = (%#v,%v)", installed, err)
	}
	assertEnrollmentDatabaseCounts(t, ownerPath, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
	})
}

func TestChannelEnrollmentCommitUnknownRetryReleasesOnAuthenticatedProtocolError(t *testing.T) {
	createdAt := time.Date(2026, 7, 18, 2, 0, 0, 0, time.UTC)
	ownerFixture := testkit.NewSignedChannelAt(t, "peer-enrollment-stable-reject", createdAt)
	ownerIdentity := ownerFixture.Owner()
	joinerIdentity := testkit.NewIdentity(t, "peer-enrollment-stable-reject-joiner")
	joinerPath := filepath.Join(t.TempDir(), "joiner", "node.db")
	joinerStore := createEnrollmentTestStore(t, joinerPath, joinerIdentity, createdAt)
	defer joinerStore.Close()
	ownerHost := newEnrollmentTestHostAtIdentityAddress(t, ownerIdentity)
	defer ownerHost.Close()
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-stable-reject")
	token := enrollmentTestTokenForHost(t, ownerFixture, grantID,
		"peer-enrollment-stable-reject", ownerHost)
	joinAt := createdAt.Add(time.Minute)
	prepared, err := joinerStore.PrepareJoinedChannel(context.Background(), store.PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: joinerIdentity.PeerID(), LocalPublicKey: joinerIdentity.PublicKey(),
		Descriptor: ownerFixture.Descriptor(), GrantID: grantID, LocalAlias: "stable-reject-team",
		At: joinAt})
	if err != nil {
		t.Fatal(err)
	}
	if err := joinerStore.MarkJoinedChannelCommitUnknown(context.Background(), prepared.RequestID,
		joinerIdentity.PeerID(), prepared.Attempt, joinAt); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime := newTestMeshRuntime(t, ctx, joinerIdentity, readMeshRuntimeAuthority(t, joinerStore))
	reject := ChannelRequestHandlerFunc(func(_ context.Context, stream network.Stream,
		frame ChannelFrame,
	) error {
		failure, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorTokenClosed})
		if err != nil {
			return err
		}
		response, err := NewChannelFrame(frame.RequestID(), failure)
		if err != nil {
			return err
		}
		return WriteChannelFrame(stream, response)
	})
	dispatcher, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: reject})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	_, err = runtime.JoinChannel(ctx, JoinChannelSpec{Token: token,
		DisplayLabel: joinerIdentity.DisplayName(), AdvertisedMultiaddrs: joinerIdentity.Multiaddrs(),
		LocalAlias: "stable-reject-team"}, &enrollmentJoinStoreSession{store: joinerStore})
	var failure *ChannelProtocolFailure
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorTokenClosed {
		t.Fatalf("commit-unknown stable rejection error = %v", err)
	}
	assertEnrollmentDatabaseCounts(t, joinerPath, map[string]int{"channel_join_reservations": 0,
		"channels": 0})
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

type enrollmentJoinTestStore interface {
	PrepareJoinedChannel(context.Context,
		store.PrepareJoinedChannelSpec,
	) (store.PrepareJoinedChannelResult, error)
	MarkJoinedChannelCommitUnknown(context.Context, model.EnrollmentRequestID,
		model.PeerID, uint64, time.Time,
	) error
	ReleaseJoinedChannelReservation(context.Context, model.EnrollmentRequestID,
		model.PeerID, uint64,
	) error
	InstallJoinedChannel(context.Context,
		store.InstallJoinedChannelSpec,
	) (store.InstallJoinedChannelResult, error)
}

// enrollmentJoinStoreSession is a test-only composition adapter. Production
// peer code never receives a Store, attempt number, or install specification.
type enrollmentJoinStoreSession struct {
	store        enrollmentJoinTestStore
	installErr   error
	afterInstall func(VerifiedChannelEnrollment)

	mu       sync.Mutex
	begun    bool
	marked   bool
	terminal bool
	control  ChannelJoinPrepareControl
	prepared store.PrepareJoinedChannelResult
}

func (session *enrollmentJoinStoreSession) BeginChannelJoin(ctx context.Context,
	control ChannelJoinPrepareControl,
) (PreparedChannelJoin, error) {
	session.mu.Lock()
	if session.begun {
		session.mu.Unlock()
		return PreparedChannelJoin{}, errors.New("test join session already used")
	}
	session.begun = true
	session.mu.Unlock()
	prepared, err := session.store.PrepareJoinedChannel(ctx, store.PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: control.AuthenticatedLocalPeerID,
		LocalPublicKey:           append([]byte(nil), control.LocalPublicKey...),
		Descriptor:               control.Descriptor, GrantID: control.GrantID, LocalAlias: control.LocalAlias,
		At: control.At})
	if err != nil {
		return PreparedChannelJoin{}, enrollmentJoinTestControlFailure(err)
	}
	session.mu.Lock()
	session.control = control
	session.control.LocalPublicKey = append([]byte(nil), control.LocalPublicKey...)
	session.prepared = prepared
	session.mu.Unlock()
	return NewPreparedChannelJoin(prepared.RequestID, prepared.OriginEpoch,
		prepared.Reserved, prepared.CommitUnknown)
}

func (session *enrollmentJoinStoreSession) MarkChannelJoinCommitUnknown(ctx context.Context,
	at time.Time,
) error {
	control, prepared, err := session.prepareCallback(false)
	if err != nil {
		return err
	}
	if markErr := session.store.MarkJoinedChannelCommitUnknown(ctx, prepared.RequestID,
		control.AuthenticatedLocalPeerID, prepared.Attempt, at); markErr != nil {
		return enrollmentJoinTestControlFailure(markErr)
	}
	session.mu.Lock()
	session.marked = true
	session.mu.Unlock()
	return nil
}

func (session *enrollmentJoinStoreSession) ReleaseChannelJoinReservation(ctx context.Context) error {
	control, prepared, err := session.prepareCallback(true)
	if err != nil {
		return err
	}
	return session.store.ReleaseJoinedChannelReservation(ctx, prepared.RequestID,
		control.AuthenticatedLocalPeerID, prepared.Attempt)
}

func (session *enrollmentJoinStoreSession) InstallAcceptedChannelJoin(ctx context.Context,
	accepted VerifiedChannelEnrollment, at time.Time,
) (ChannelJoinResult, error) {
	control, prepared, err := session.prepareCallback(true)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	session.mu.Lock()
	marked := session.marked
	session.mu.Unlock()
	if prepared.Reserved && !marked {
		return ChannelJoinResult{}, errors.New("test join session installed before commit_unknown")
	}
	if session.installErr != nil {
		return ChannelJoinResult{}, session.installErr
	}
	status, err := enrollmentJoinTestStoreStatus(accepted.Status())
	if err != nil {
		return ChannelJoinResult{}, err
	}
	result, err := session.store.InstallJoinedChannel(ctx, store.InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: accepted.AuthenticatedOwnerPeerID(), OwnerOutcome: status,
		LocalAlias: control.LocalAlias, Descriptor: accepted.Descriptor(),
		Transcript: accepted.Transcript(), Receipt: accepted.Receipt(),
		Members: accepted.Roster().Members(), At: at})
	if err != nil {
		return ChannelJoinResult{}, enrollmentJoinTestControlFailure(err)
	}
	if session.afterInstall != nil {
		session.afterInstall(accepted)
	}
	peerStatus, err := enrollmentOwnerTestStatus(result.Status)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	return NewChannelJoinResult(ChannelJoinResultSpec{Installed: result.Installed,
		Status: peerStatus, Channel: result.Channel, Roster: result.Roster})
}

func (session *enrollmentJoinStoreSession) prepareCallback(terminal bool) (
	ChannelJoinPrepareControl, store.PrepareJoinedChannelResult, error,
) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if !session.begun || session.terminal {
		return ChannelJoinPrepareControl{}, store.PrepareJoinedChannelResult{},
			errors.New("test join session callback out of order")
	}
	if terminal {
		session.terminal = true
	}
	return session.control, session.prepared, nil
}

func enrollmentJoinTestStoreStatus(status ChannelEnrollmentStatus) (store.ChannelEnrollmentStatus, error) {
	switch status {
	case ChannelEnrollmentAccepted:
		return store.ChannelEnrollmentAccepted, nil
	case ChannelEnrollmentReplayed:
		return store.ChannelEnrollmentReplayed, nil
	case ChannelEnrollmentMemberRevoked:
		return store.ChannelEnrollmentMemberRevoked, nil
	case ChannelEnrollmentChannelClosed:
		return store.ChannelEnrollmentChannelClosed, nil
	default:
		return "", ErrChannelEnrollmentProtocol
	}
}

func enrollmentJoinTestControlFailure(cause error) error {
	var code ChannelProtocolErrorCode
	switch {
	case errors.Is(cause, store.ErrNodeChannelLimit):
		code = ChannelErrorNodeChannelLimit
	case errors.Is(cause, store.ErrChannelJoinConflict), errors.Is(cause, store.ErrChannelJoinInput),
		errors.Is(cause, store.ErrChannelAuthorityInvariant):
		code = ChannelErrorRosterConflict
	default:
		return cause
	}
	failure, err := NewChannelJoinControlFailure(code)
	if err != nil {
		return cause
	}
	return failure
}

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
	return enrollmentTestTokenForAddresses(t, fixture, grantID, seed,
		fixture.Owner().Multiaddrs())
}

func enrollmentTestTokenForHost(t *testing.T, fixture *testkit.SignedChannel,
	grantID model.GrantID, seed string, ownerHost host.Host,
) model.EnrollmentToken {
	t.Helper()
	addresses := ownerHost.Addrs()
	raw := make([]string, len(addresses))
	for index, address := range addresses {
		raw[index] = address.String()
	}
	sort.Strings(raw)
	return enrollmentTestTokenForAddresses(t, fixture, grantID, seed, raw)
}

func enrollmentTestTokenForAddresses(t *testing.T, fixture *testkit.SignedChannel,
	grantID model.GrantID, seed string, ownerMultiaddrs []string,
) model.EnrollmentToken {
	t.Helper()
	secret := model.Sum([]byte("peer-enrollment-token-secret:" + seed)).Bytes()
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: fixture.Descriptor(), OwnerMultiaddrs: append([]string(nil), ownerMultiaddrs...),
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

func newEnrollmentTestHostAtIdentityAddress(t *testing.T, identity testkit.Identity) host.Host {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	nodeHost, err := libp2p.New(libp2p.Identity(privateKey),
		libp2p.ListenAddrStrings(identity.Multiaddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	return nodeHost
}

func registerEnrollmentTestDispatcher(t *testing.T, ctx context.Context, ownerHost host.Host,
	owner *ChannelEnrollmentOwner,
) {
	t.Helper()
	dispatcher, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: owner})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dispatcher.Close(); err != nil {
			t.Errorf("close Channel dispatcher: %v", err)
		}
	})
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
