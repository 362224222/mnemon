package peer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactReceiverResumesDurablePublishingWithoutNetwork(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       store.ArtifactStageState
		putStage    bool
		putFinal    bool
		publishLost bool
		wantSettles int
	}{
		{name: "prepared stage", state: store.ArtifactStagePublishing,
			putStage: true, wantSettles: 1},
		{name: "publish response loss", state: store.ArtifactStagePublishing,
			putStage: true, publishLost: true, wantSettles: 1},
		{name: "ready replay", state: store.ArtifactStageReady,
			putFinal: true, wantSettles: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newArtifactReceiverPublishFixture(t, test.name, test.state)
			if test.putStage {
				fixture.putStage(t)
			}
			if test.putFinal {
				fixture.putFinal(t)
			}
			if test.publishLost {
				if err := fixture.stage.Publish(context.Background(), fixture.closure); err != nil {
					t.Fatal(err)
				}
			}
			receiver := fixture.receiver(t)
			if err := receiver.runCycle(context.Background()); err != nil {
				t.Fatal(err)
			}
			fixture.assertFinal(t)
			fixture.backend.mu.Lock()
			readyCalls := fixture.backend.readyCalls
			readCalls := fixture.backend.publishReadCalls
			fixture.backend.mu.Unlock()
			if readyCalls != test.wantSettles || readCalls != 1 ||
				receiver.Snapshot().Ready != 1 ||
				receiver.Snapshot().ManifestPulls != 0 ||
				receiver.Snapshot().BlockPulls != 0 {
				t.Fatalf("recovery calls ready/read = %d/%d snapshot %#v",
					readyCalls, readCalls, receiver.Snapshot())
			}
		})
	}
}

func TestArtifactReceiverPublishingRecoveryFailsClosed(t *testing.T) {
	t.Run("missing stage and final", func(t *testing.T) {
		fixture := newArtifactReceiverPublishFixture(
			t, "missing-publish", store.ArtifactStagePublishing)
		assertArtifactReceiverPublishFatal(t, fixture.receiver(t),
			ArtifactReceiverFatalCASInvariant)
	})

	t.Run("corrupt stage shadows valid final", func(t *testing.T) {
		fixture := newArtifactReceiverPublishFixture(
			t, "corrupt-publish", store.ArtifactStagePublishing)
		fixture.putStage(t)
		fixture.putFinal(t)
		fixture.corruptStagedObject(t, fixture.content.manifest.ManifestDigest())
		assertArtifactReceiverPublishFatal(t, fixture.receiver(t),
			ArtifactReceiverFatalCASInvariant)
	})

	t.Run("wrong owner", func(t *testing.T) {
		fixture := newArtifactReceiverPublishFixture(
			t, "wrong-owner", store.ArtifactStagePublishing)
		otherID, err := model.ParseInboxID("inbox-artifact-receiver-different-owner")
		if err != nil {
			t.Fatal(err)
		}
		other, err := artifactdomain.NewInboxStageOwner(otherID, 1)
		if err != nil {
			t.Fatal(err)
		}
		fixture.backend.beginOwner = other
		assertArtifactReceiverPublishFatal(t, fixture.receiver(t),
			ArtifactReceiverFatalStoreInvariant)
	})

	t.Run("recovery fence", func(t *testing.T) {
		fixture := newArtifactReceiverPublishFixture(
			t, "wrong-fence", store.ArtifactStagePublishing)
		fixture.backend.publishReadErrors = []error{store.ErrArtifactStageFence}
		assertArtifactReceiverPublishFatal(t, fixture.receiver(t),
			ArtifactReceiverFatalStoreInvariant)
	})
}

func TestArtifactReceiverClassifiesPublishAcceptanceFenceChanges(t *testing.T) {
	for _, test := range []struct {
		name      string
		storeErr  error
		wantErr   error
		wantStale uint64
	}{
		{name: "terminally settled", storeErr: store.ErrPeerInboxArtifactStale,
			wantErr: errArtifactReceiverClaimStale, wantStale: 1},
		{name: "transient authority", storeErr: store.ErrPeerInboxArtifactAuthority,
			wantErr: errArtifactReceiverAuthorityChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newArtifactReceiverPublishFixture(
				t, test.name, store.ArtifactStagePublishing)
			fixture.backend.acceptErrors = []error{test.storeErr}
			receiver := fixture.receiver(t)
			claim := fixture.backend.claims[0]
			err := receiver.acceptArtifactPublish(
				context.Background(), &claim, fixture.owner)
			if !errors.Is(err, test.wantErr) ||
				receiver.Snapshot().StaleClaims != test.wantStale ||
				receiver.Snapshot().FatalCode != ArtifactReceiverFatalNone {
				t.Fatalf("accept classification = %v snapshot %#v",
					err, receiver.Snapshot())
			}
		})
	}
}

type artifactReceiverPublishFixture struct {
	at      time.Time
	content artifactReceiverTestContent
	closure artifactdomain.Closure
	owner   artifactdomain.StageOwner
	cas     *artifactdomain.CAS
	stage   *artifactdomain.Stage
	backend *artifactReceiverTestBackend
}

func newArtifactReceiverPublishFixture(t testing.TB, name string,
	state store.ArtifactStageState,
) artifactReceiverPublishFixture {
	t.Helper()
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-"+name+"-origin")
	channelID := artifactReceiverChannelID(t, name)
	content := artifactReceiverContentForTest(t, name, []byte("durable publish bytes"))
	claim := artifactReceiverClaimForTest(t, name, origin, channelID,
		[]model.Digest{content.manifest.RootDigest()})
	closure, err := artifactdomain.BuildImportedClosure(
		context.Background(), []artifactdomain.Manifest{content.manifest}, at)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := artifactdomain.NewInboxStageOwner(claim.fence.inboxID, 1)
	if err != nil {
		t.Fatal(err)
	}
	cas := newArtifactReceiverTestCAS(t)
	stage, err := cas.OpenStage(owner)
	if err != nil {
		t.Fatal(err)
	}
	backend := &artifactReceiverTestBackend{
		claims: []artifactReceiverClaim{claim}, beginState: store.ArtifactStagePublishing,
		beginOwner: owner,
		publishCheckpoint: artifactReceiverPublishCheckpoint{
			closure: storeArtifactReceiverClosure(closure), state: state,
		},
	}
	return artifactReceiverPublishFixture{at: at, content: content, closure: closure,
		owner: owner, cas: cas, stage: stage, backend: backend}
}

func (fixture artifactReceiverPublishFixture) receiver(t testing.TB) *ArtifactReceiver {
	t.Helper()
	client := &artifactReceiverTestClient{
		manifest: func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
			t.Fatal("durable publishing recovery fetched a Manifest")
			return Manifest{}, ErrArtifactClientTransport
		},
		block: func(context.Context, model.PeerID, GetBlock) (Block, error) {
			t.Fatal("durable publishing recovery fetched a block")
			return Block{}, ErrArtifactClientTransport
		},
	}
	return newArtifactReceiverForTest(t, fixture.backend, client, fixture.cas,
		10*time.Second, 1, &artifactReceiverTestClock{at: fixture.at})
}

func (fixture artifactReceiverPublishFixture) putStage(t testing.TB) {
	t.Helper()
	if _, err := fixture.stage.Put(fixture.content.manifest.ManifestDigest(),
		fixture.content.manifest.CanonicalJSON().Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.stage.Put(
		model.Sum(fixture.content.blockBytes), fixture.content.blockBytes); err != nil {
		t.Fatal(err)
	}
}

func (fixture artifactReceiverPublishFixture) putFinal(t testing.TB) {
	t.Helper()
	putArtifactReceiverContent(t, fixture.cas, fixture.content)
}

func (fixture artifactReceiverPublishFixture) assertFinal(t testing.TB) {
	t.Helper()
	if err := fixture.cas.VerifyClosure(context.Background(), fixture.closure); err != nil {
		t.Fatalf("final recovered closure = %v", err)
	}
	if _, err := fixture.stage.Read(fixture.content.manifest.ManifestDigest(),
		artifactdomain.MaxManifestBytes); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published stage remains readable: %v", err)
	}
}

func (fixture artifactReceiverPublishFixture) corruptStagedObject(
	t testing.TB, digest model.Digest,
) {
	t.Helper()
	name := strings.TrimPrefix(digest.String(), "sha256:")
	var object string
	err := filepath.WalkDir(filepath.Join(fixture.cas.Root(), ".staging"),
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.IsDir() && entry.Name() == name {
				object = path
			}
			return nil
		})
	if err != nil || object == "" {
		t.Fatalf("find staged object = (%q, %v)", object, err)
	}
	content := fixture.content.manifest.CanonicalJSON().Bytes()
	content[0] ^= 0xff
	if err := os.WriteFile(object, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertArtifactReceiverPublishFatal(t testing.TB, receiver *ArtifactReceiver,
	code ArtifactReceiverFatalCode,
) {
	t.Helper()
	err := <-runArtifactReceiverForTest(receiver, context.Background())
	if !errors.Is(err, ErrArtifactReceiverInvariant) ||
		receiver.Snapshot().FatalCode != code {
		t.Fatalf("publishing recovery fatal = %v snapshot %#v", err, receiver.Snapshot())
	}
}

type artifactReceiverResponseLossStore struct {
	ArtifactReceiverStore

	mu              sync.Mutex
	loseFirstStage  bool
	loseFirstAccept bool
	loseFirstReady  bool
	beginResults    []store.PeerInboxArtifactStageRegistration
	beginSpecs      []store.BeginPeerInboxArtifactStageSpec
	stageResults    []store.PeerInboxArtifactStage
	stageSpecs      []store.PreparePeerInboxArtifactPublishSpec
	acceptResults   []store.PeerInboxArtifactStage
	acceptSpecs     []store.AcceptPeerInboxArtifactPublishSpec
	readyResults    []store.PeerInboxArtifactSettlement
}

func (st *artifactReceiverResponseLossStore) BeginPeerInboxArtifactStage(ctx context.Context,
	spec store.BeginPeerInboxArtifactStageSpec,
) (store.PeerInboxArtifactStageRegistration, error) {
	result, err := st.ArtifactReceiverStore.BeginPeerInboxArtifactStage(ctx, spec)
	st.mu.Lock()
	st.beginResults = append(st.beginResults, result)
	st.beginSpecs = append(st.beginSpecs, spec)
	st.mu.Unlock()
	return result, err
}

func (st *artifactReceiverResponseLossStore) PreparePeerInboxArtifactPublish(ctx context.Context,
	spec store.PreparePeerInboxArtifactPublishSpec,
) (store.PeerInboxArtifactStage, error) {
	result, err := st.ArtifactReceiverStore.PreparePeerInboxArtifactPublish(ctx, spec)
	st.mu.Lock()
	st.stageResults = append(st.stageResults, result)
	st.stageSpecs = append(st.stageSpecs, spec)
	lose := err == nil && st.loseFirstStage && len(st.stageResults) == 1
	st.mu.Unlock()
	if lose {
		return result, errors.New("injected Stage response loss after commit")
	}
	return result, err
}

func (st *artifactReceiverResponseLossStore) AcceptPeerInboxArtifactPublish(ctx context.Context,
	spec store.AcceptPeerInboxArtifactPublishSpec,
) (store.PeerInboxArtifactStage, error) {
	result, err := st.ArtifactReceiverStore.AcceptPeerInboxArtifactPublish(ctx, spec)
	st.mu.Lock()
	st.acceptResults = append(st.acceptResults, result)
	st.acceptSpecs = append(st.acceptSpecs, spec)
	lose := err == nil && st.loseFirstAccept && len(st.acceptResults) == 1
	st.mu.Unlock()
	if lose {
		return result, errors.New("injected Accept response loss after commit")
	}
	return result, err
}

func (st *artifactReceiverResponseLossStore) MarkPeerInboxArtifactReady(ctx context.Context,
	spec store.MarkPeerInboxArtifactReadySpec,
) (store.PeerInboxArtifactSettlement, error) {
	result, err := st.ArtifactReceiverStore.MarkPeerInboxArtifactReady(ctx, spec)
	st.mu.Lock()
	st.readyResults = append(st.readyResults, result)
	lose := err == nil && st.loseFirstReady && len(st.readyResults) == 1
	st.mu.Unlock()
	if lose {
		return result, errors.New("injected Ready response loss after commit")
	}
	return result, err
}

func (st *artifactReceiverResponseLossStore) snapshot() ([]store.PeerInboxArtifactStage,
	[]store.PreparePeerInboxArtifactPublishSpec, []store.PeerInboxArtifactSettlement,
) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]store.PeerInboxArtifactStage(nil), st.stageResults...),
		append([]store.PreparePeerInboxArtifactPublishSpec(nil), st.stageSpecs...),
		append([]store.PeerInboxArtifactSettlement(nil), st.readyResults...)
}

func (st *artifactReceiverResponseLossStore) beginSnapshot() ([]store.PeerInboxArtifactStageRegistration, []store.BeginPeerInboxArtifactStageSpec) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]store.PeerInboxArtifactStageRegistration(nil), st.beginResults...),
		append([]store.BeginPeerInboxArtifactStageSpec(nil), st.beginSpecs...)
}

func (st *artifactReceiverResponseLossStore) acceptSnapshot() (
	[]store.PeerInboxArtifactStage, []store.AcceptPeerInboxArtifactPublishSpec,
) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]store.PeerInboxArtifactStage(nil), st.acceptResults...),
		append([]store.AcceptPeerInboxArtifactPublishSpec(nil), st.acceptSpecs...)
}

type artifactReceiverRecordingClient struct {
	delegate ArtifactReceiverClient
	cas      *artifactdomain.CAS
	store    *artifactReceiverResponseLossStore
	cancel   context.CancelFunc

	mu                sync.Mutex
	cancelBeforeBlock int
	manifestRequests  []model.Digest
	blockRequests     []model.Digest
	observedDurable   bool
}

func (client *artifactReceiverRecordingClient) GetManifest(ctx context.Context,
	peerID model.PeerID, request GetManifest,
) (Manifest, error) {
	client.mu.Lock()
	client.manifestRequests = append(client.manifestRequests, request.RootDigest())
	client.mu.Unlock()
	return client.delegate.GetManifest(ctx, peerID, request)
}

func (client *artifactReceiverRecordingClient) GetBlock(ctx context.Context,
	peerID model.PeerID, request GetBlock,
) (Block, error) {
	client.mu.Lock()
	client.blockRequests = append(client.blockRequests, request.BlockDigest())
	call := len(client.blockRequests)
	cancelBefore := client.cancelBeforeBlock
	var first model.Digest
	if len(client.blockRequests) > 1 {
		first = client.blockRequests[0]
	}
	client.mu.Unlock()
	if cancelBefore > 0 && call == cancelBefore {
		if client.cas == nil || client.store == nil || first.IsZero() {
			return Block{}, errors.New("partial-restart cancellation has no prior CAS block")
		}
		results, _ := client.store.beginSnapshot()
		if len(results) == 0 {
			return Block{}, errors.New("partial-restart stage was not durably begun")
		}
		stage, err := client.cas.OpenStage(results[len(results)-1].Owner())
		if err != nil {
			return Block{}, fmt.Errorf("partial-restart open stage: %w", err)
		}
		if _, err := stage.Read(first, artifactdomain.BlockSize); err != nil {
			return Block{}, fmt.Errorf(
				"partial-restart prior staged block is not durable: %w", err)
		}
		client.mu.Lock()
		client.observedDurable = true
		client.mu.Unlock()
		if client.cancel != nil {
			client.cancel()
		}
		return Block{}, context.Canceled
	}
	return client.delegate.GetBlock(ctx, peerID, request)
}

func (client *artifactReceiverRecordingClient) snapshot() ([]model.Digest, []model.Digest, bool) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]model.Digest(nil), client.manifestRequests...),
		append([]model.Digest(nil), client.blockRequests...), client.observedDurable
}
