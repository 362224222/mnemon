package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type recordingArtifactCapturer struct {
	delegate *artifact.Capturer
	calls    int
	closure  artifact.Closure
	fail     bool
}

func (capturer *recordingArtifactCapturer) Capture(ctx context.Context, paths []string,
	sink artifact.ObjectSink,
) (artifact.Closure, error) {
	capturer.calls++
	if capturer.fail {
		return artifact.Closure{}, errors.New("live workspace capture must not run")
	}
	closure, err := capturer.delegate.Capture(ctx, paths, sink)
	if err == nil {
		capturer.closure = closure
	}
	return closure, err
}

type artifactCaptureStoreHarness struct {
	delegate      *store.Store
	order         []string
	lastBegin     store.OperationArtifactStageResult
	beforePrepare func(store.PrepareOperationArtifactPublishSpec)
	afterPrepare  func(store.PrepareOperationArtifactPublishSpec)
	prepareLoss   int
	committedLoss int
	markLoss      int
}

func (harness *artifactCaptureStoreHarness) CheckpointOperationCapture(ctx context.Context,
	id model.OperationID, owner string, at time.Time, checkpoint model.JSON,
) (bool, error) {
	harness.order = append(harness.order, "empty")
	return harness.delegate.CheckpointOperationCapture(ctx, id, owner, at, checkpoint)
}

func (harness *artifactCaptureStoreHarness) BeginOperationArtifactStage(ctx context.Context,
	spec store.BeginOperationArtifactStageSpec,
) (store.OperationArtifactStageResult, error) {
	harness.order = append(harness.order, "begin")
	result, err := harness.delegate.BeginOperationArtifactStage(ctx, spec)
	if err == nil {
		harness.lastBegin = result
	}
	return result, err
}

func (harness *artifactCaptureStoreHarness) PrepareOperationArtifactPublish(ctx context.Context,
	spec store.PrepareOperationArtifactPublishSpec,
) (store.OperationArtifactStageResult, error) {
	harness.order = append(harness.order, "prepare")
	if harness.beforePrepare != nil {
		harness.beforePrepare(spec)
	}
	result, err := harness.delegate.PrepareOperationArtifactPublish(ctx, spec)
	if err == nil && harness.afterPrepare != nil {
		harness.afterPrepare(spec)
	}
	if err == nil && harness.prepareLoss > 0 {
		harness.prepareLoss--
		return result, context.DeadlineExceeded
	}
	return result, err
}

func (harness *artifactCaptureStoreHarness) ReadOperationArtifactPublish(ctx context.Context,
	spec store.ReadOperationArtifactPublishSpec,
) (store.OperationArtifactPublishCheckpoint, error) {
	harness.order = append(harness.order, "read")
	return harness.delegate.ReadOperationArtifactPublish(ctx, spec)
}

func (harness *artifactCaptureStoreHarness) ReadCommittedOperationArtifactPublish(
	ctx context.Context, spec store.ReadCommittedOperationArtifactPublishSpec,
) (store.OperationArtifactPublishCheckpoint, bool, error) {
	harness.order = append(harness.order, "committed")
	result, found, err := harness.delegate.ReadCommittedOperationArtifactPublish(ctx, spec)
	if err == nil && harness.committedLoss > 0 {
		harness.committedLoss--
		return result, found, context.DeadlineExceeded
	}
	return result, found, err
}

func (harness *artifactCaptureStoreHarness) MarkOperationArtifactReady(ctx context.Context,
	spec store.MarkOperationArtifactReadySpec,
) (store.OperationArtifactStageResult, error) {
	harness.order = append(harness.order, "mark")
	result, err := harness.delegate.MarkOperationArtifactReady(ctx, spec)
	if err == nil && harness.markLoss > 0 {
		harness.markLoss--
		return result, context.DeadlineExceeded
	}
	return result, err
}

type artifactCaptureClock struct {
	values []time.Time
	calls  int
}

func (clock *artifactCaptureClock) Now() time.Time {
	if clock.calls >= len(clock.values) {
		clock.calls++
		return time.Time{}
	}
	value := clock.values[clock.calls]
	clock.calls++
	return value
}

type artifactCaptureFixture struct {
	t           *testing.T
	at          time.Time
	leaseUntil  time.Time
	workspace   string
	casPath     string
	storePath   string
	store       *store.Store
	cas         *artifact.CAS
	reservation store.ManagedOperationReservation
}

func TestArtifactCaptureCoordinatorPreparesFreshClosureWithoutPublishing(t *testing.T) {
	fixture := newArtifactCaptureFixture(t, "fresh", time.Hour)
	fixture.write("result.txt", []byte("fresh staged Artifact"))
	capturer := fixture.capturer(fixture.at.Add(4 * time.Second))
	storeHarness := &artifactCaptureStoreHarness{delegate: fixture.store}
	storeHarness.beforePrepare = func(spec store.PrepareOperationArtifactPublishSpec) {
		for _, root := range spec.Closure.Roots {
			if _, err := fixture.cas.Read(root.ManifestDigest,
				artifact.MaxManifestBytes); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("manifest reached final CAS before Prepare: %v", err)
			}
		}
		for _, block := range spec.Closure.Blocks {
			if _, err := fixture.cas.Read(block.Digest,
				artifact.BlockSize); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("block reached final CAS before Prepare: %v", err)
			}
		}
	}
	clock := &artifactCaptureClock{values: fixture.times(5, 6)}
	coordinator := fixture.coordinator(capturer, storeHarness, clock)

	result, apiErr := coordinator.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if result.Replayed || capturer.calls != 1 || len(result.Roots) != 1 ||
		strings.Join(storeHarness.order, ",") != "begin,prepare" {
		t.Fatalf("fresh capture = (%#v), calls=%d order=%v",
			result, capturer.calls, storeHarness.order)
	}
	if err := fixture.cas.VerifyClosure(context.Background(), capturer.closure); err == nil {
		t.Fatal("unaccepted capture reached final CAS")
	}
	root := capturer.closure.Roots()[0]
	if _, err := fixture.store.GetVerifiedArtifactRoot(
		context.Background(), root.RootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("unaccepted root authority: %v", err)
	}
	fixture.requirePhysicalStages(1)
}

func TestArtifactCaptureCoordinatorRecoversPrepareLossAndPartialPublishAfterRestart(t *testing.T) {
	fixture := newArtifactCaptureFixture(t, "prepare-loss", time.Hour)
	fixture.write("result.txt", []byte("restart from the durable closure"))
	capturer := fixture.capturer(fixture.at.Add(4 * time.Second))
	firstStore := &artifactCaptureStoreHarness{delegate: fixture.store, prepareLoss: 1}
	first := fixture.coordinator(capturer, firstStore,
		&artifactCaptureClock{values: fixture.times(5, 6)})

	if _, apiErr := first.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"}); apiErr == nil || apiErr.Code != CodeOperationPending ||
		!apiErr.Retryable {
		t.Fatalf("Prepare response loss = %#v", apiErr)
	}
	if capturer.calls != 1 || strings.Join(firstStore.order, ",") != "begin,prepare" {
		t.Fatalf("pre-loss capture calls/order = %d/%v", capturer.calls, firstStore.order)
	}
	root := capturer.closure.Roots()[0]
	if _, err := fixture.store.GetVerifiedArtifactRoot(
		context.Background(), root.RootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("publishing root authority = %v", err)
	}

	stage, err := fixture.cas.OpenStage(firstStore.lastBegin.Fence().Owner())
	if err != nil {
		t.Fatal(err)
	}
	block := capturer.closure.Blocks()[0]
	content, err := stage.Read(block.Digest, artifact.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.cas.Put(block.Digest, content); err != nil {
		t.Fatalf("seed partial final publication: %v", err)
	}
	if _, err := fixture.cas.Read(root.ManifestDigest,
		artifact.MaxManifestBytes); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial publication unexpectedly included manifest: %v", err)
	}

	fixture.requirePreparedReplay("restart recovery", true)
	if err := fixture.cas.VerifyClosure(context.Background(), capturer.closure); err == nil {
		t.Fatal("prepared replay reached final CAS")
	}
	if _, err := fixture.store.GetVerifiedArtifactRoot(
		context.Background(), root.RootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("prepared replay root authority: %v", err)
	}
	fixture.requirePhysicalStages(1)
}

func TestArtifactCaptureCoordinatorDoesNotPublishBeforeOperationCommit(t *testing.T) {
	fixture := newArtifactCaptureFixture(t, "mark-loss", time.Hour)
	fixture.write("result.txt", []byte("published before Mark response loss"))
	capturer := fixture.capturer(fixture.at.Add(4 * time.Second))
	firstStore := &artifactCaptureStoreHarness{delegate: fixture.store}
	first := fixture.coordinator(capturer, firstStore,
		&artifactCaptureClock{values: fixture.times(5, 6, 7)})

	if _, apiErr := first.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if apiErr := first.PublishAccepted(context.Background(),
		fixture.reservation.Operation.ID()); apiErr == nil ||
		apiErr.Code != CodeInternal {
		t.Fatalf("pre-commit publish = %#v", apiErr)
	}
	if strings.Join(firstStore.order, ",") != "begin,prepare,committed" {
		t.Fatalf("pre-commit order = %v", firstStore.order)
	}
	root := capturer.closure.Roots()[0]
	if _, err := fixture.store.GetVerifiedArtifactRoot(
		context.Background(), root.RootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("pre-commit publish created authority: %v", err)
	}

	fixture.requirePreparedReplay("pre-commit replay", false)
	fixture.requirePhysicalStages(1)
}

func TestArtifactCaptureCoordinatorKeepsPreparedBytesStagedAfterLeaseExpiry(t *testing.T) {
	fixture := newArtifactCaptureFixture(t, "lease-expiry", 20*time.Second)
	fixture.write("result.txt", []byte("bytes can outlive an expired publisher"))
	capturer := fixture.capturer(fixture.at.Add(4 * time.Second))
	storeHarness := &artifactCaptureStoreHarness{delegate: fixture.store}
	clock := &artifactCaptureClock{values: []time.Time{
		fixture.at.Add(5 * time.Second),
		fixture.at.Add(10 * time.Second),
	}}
	coordinator := fixture.coordinator(capturer, storeHarness, clock)

	if _, apiErr := coordinator.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if strings.Join(storeHarness.order, ",") != "begin,prepare" {
		t.Fatalf("expired publication order = %v", storeHarness.order)
	}
	if err := fixture.cas.VerifyClosure(context.Background(), capturer.closure); err == nil {
		t.Fatal("prepared bytes reached final CAS")
	}
	root := capturer.closure.Roots()[0]
	if _, err := fixture.store.GetVerifiedArtifactRoot(
		context.Background(), root.RootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("expired publisher created final authority: %v", err)
	}

	retry := fixture.coordinator(&recordingArtifactCapturer{fail: true}, storeHarness,
		&artifactCaptureClock{values: []time.Time{fixture.leaseUntil.Add(time.Second)}})
	if _, apiErr := retry.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"}); apiErr == nil || apiErr.Code != CodeOperationPending {
		t.Fatalf("stale lease retry = %#v", apiErr)
	}
}

func TestArtifactCaptureCoordinatorKeepsPreparedCorruptionPending(t *testing.T) {
	fixture := newArtifactCaptureFixture(t, "prepared-corruption", time.Hour)
	fixture.write("result.txt", []byte("prepared bytes become unavailable"))
	capturer := fixture.capturer(fixture.at.Add(4 * time.Second))
	storeHarness := &artifactCaptureStoreHarness{delegate: fixture.store}
	storeHarness.afterPrepare = func(store.PrepareOperationArtifactPublishSpec) {
		generations, err := os.ReadDir(filepath.Join(fixture.casPath, ".staging"))
		if err != nil || len(generations) != 1 {
			t.Fatalf("prepared stage generations = (%v,%v)", generations, err)
		}
		stagePath := filepath.Join(fixture.casPath, ".staging", generations[0].Name())
		objects, err := os.ReadDir(stagePath)
		if err != nil || len(objects) == 0 {
			t.Fatalf("prepared staged objects = (%v,%v)", objects, err)
		}
		if err := os.Remove(filepath.Join(stagePath, objects[0].Name())); err != nil {
			t.Fatal(err)
		}
	}
	coordinator := fixture.coordinator(capturer, storeHarness,
		&artifactCaptureClock{values: fixture.times(5, 6, 7)})

	if _, apiErr := coordinator.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"}); apiErr != nil {
		t.Fatal(apiErr)
	}
	root := capturer.closure.Roots()[0]
	if _, err := fixture.store.GetVerifiedArtifactRoot(
		context.Background(), root.RootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("prepared corruption created final authority: %v", err)
	}
}

func TestArtifactCaptureCoordinatorEmptyCaptureUsesNoPhysicalStage(t *testing.T) {
	fixture := newArtifactCaptureFixture(t, "empty", time.Hour)
	capturer := &recordingArtifactCapturer{fail: true}
	storeHarness := &artifactCaptureStoreHarness{delegate: fixture.store}
	coordinator := fixture.coordinator(capturer, storeHarness,
		&artifactCaptureClock{values: fixture.times(5)})

	result, apiErr := coordinator.Checkpoint(context.Background(), fixture.reservation, nil)
	if apiErr != nil || result.Replayed || result.Roots == nil || len(result.Roots) != 0 ||
		result.Checkpoint.String() != `{"roots":[]}` || capturer.calls != 0 ||
		strings.Join(storeHarness.order, ",") != "empty" {
		t.Fatalf("empty capture = (%#v,%#v), live=%d order=%v",
			result, apiErr, capturer.calls, storeHarness.order)
	}
	fixture.requireNoPhysicalStages()
}

func TestArtifactCaptureCoordinatorClassifiesDurableFencesAsRetryable(t *testing.T) {
	operation, err := model.ParseOperationID("operation-artifact-fence-classification")
	if err != nil {
		t.Fatal(err)
	}
	for _, durableErr := range []error{
		store.ErrOperationFence,
		store.ErrOperationPending,
		store.ErrArtifactStageFence,
		context.Canceled,
	} {
		apiErr := mapArtifactStoreError(durableErr, operation)
		if apiErr.Code != CodeOperationPending || !apiErr.Retryable {
			t.Fatalf("durable fence %v mapped to %#v", durableErr, apiErr)
		}
	}
}

func newArtifactCaptureFixture(t *testing.T, seed string,
	leaseDuration time.Duration,
) *artifactCaptureFixture {
	t.Helper()
	at := time.Date(2026, 7, 26, 18, 0, 0, 0, time.UTC)
	workspace := t.TempDir()
	nodeState := t.TempDir()
	storePath := filepath.Join(nodeState, "node.db")
	st, err := store.Open(context.Background(), storePath)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &artifactCaptureFixture{t: t, at: at, workspace: workspace,
		casPath:   filepath.Join(nodeState, "objects", "sha256"),
		storePath: storePath, store: st}
	t.Cleanup(func() {
		if fixture.store != nil {
			if err := fixture.store.Close(); err != nil {
				t.Errorf("close Artifact capture Store: %v", err)
			}
		}
	})

	peer, err := model.ParsePeerID("peer-artifact-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := model.ParseOriginEpoch("epoch-artifact-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	node, err := model.NewNode(model.NodeSpec{PeerID: peer, OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: "asset-r5-artifact",
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-artifact-" + seed, WorkspaceRoot: workspace,
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("credential-" + seed)),
		ActiveAssetRevision: "asset-r5-artifact",
		HandlingBudget:      model.DefaultHandlingBudget().JSON(),
		Enabled:             false, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	activationAt := at.Add(time.Second)
	desiredSpec := profile.Spec()
	desiredSpec.Enabled = true
	desiredSpec.UpdatedAt = activationAt
	desired, err := model.NewProfile(desiredSpec)
	if err != nil {
		t.Fatal(err)
	}
	activated, err := st.ActivateProfile(context.Background(), desired,
		profile.UpdatedAt(), activationAt)
	if err != nil {
		t.Fatal(err)
	}
	fixture.leaseUntil = at.Add(leaseDuration)
	reservation, err := st.ReserveManagedOperation(context.Background(),
		store.ManagedOperationSpec{
			Profile: activated.Profile, ClientKeyHash: model.Sum([]byte("client-" + seed)),
			RequestDigest: model.Sum([]byte("request-" + seed)),
			Kind:          model.OperationTeamworkOffer, LeaseOwner: "owner-" + seed,
			At: at.Add(2 * time.Second), LeaseUntil: fixture.leaseUntil,
		})
	if err != nil {
		t.Fatal(err)
	}
	fixture.reservation = reservation
	fixture.cas, err = artifact.NewCAS(fixture.casPath)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *artifactCaptureFixture) coordinator(capturer ArtifactCapturer,
	st ArtifactCaptureStore, clock ServiceClock,
) *ArtifactCaptureCoordinator {
	fixture.t.Helper()
	coordinator, err := NewArtifactCaptureCoordinator(capturer, fixture.cas, st, clock)
	if err != nil {
		fixture.t.Fatal(err)
	}
	return coordinator
}

func (fixture *artifactCaptureFixture) capturer(at time.Time) *recordingArtifactCapturer {
	fixture.t.Helper()
	capturer, err := artifact.NewCapturer(fixture.workspace, func() time.Time { return at })
	if err != nil {
		fixture.t.Fatal(err)
	}
	return &recordingArtifactCapturer{delegate: capturer}
}

func (fixture *artifactCaptureFixture) write(name string, content []byte) {
	fixture.t.Helper()
	if err := os.WriteFile(filepath.Join(fixture.workspace, name), content, 0o600); err != nil {
		fixture.t.Fatal(err)
	}
}

func (fixture *artifactCaptureFixture) times(seconds ...int) []time.Time {
	result := make([]time.Time, len(seconds))
	for index, second := range seconds {
		result[index] = fixture.at.Add(time.Duration(second) * time.Second)
	}
	return result
}

func (fixture *artifactCaptureFixture) restart() {
	fixture.t.Helper()
	if err := fixture.store.Close(); err != nil {
		fixture.t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := store.OpenExisting(context.Background(), fixture.storePath)
	if err != nil {
		fixture.t.Fatal(err)
	}
	fixture.store = restarted
	fixture.cas, err = artifact.NewCAS(fixture.casPath)
	if err != nil {
		fixture.t.Fatal(err)
	}
}

func (fixture *artifactCaptureFixture) requirePreparedReplay(label string, restart bool) {
	fixture.t.Helper()
	if restart {
		fixture.restart()
	}
	if err := os.Remove(filepath.Join(fixture.workspace, "result.txt")); err != nil {
		fixture.t.Fatal(err)
	}
	capturer := fixture.capturer(fixture.at.Add(10 * time.Second))
	capturer.fail = true
	storeHarness := &artifactCaptureStoreHarness{delegate: fixture.store}
	coordinator := fixture.coordinator(capturer, storeHarness,
		&artifactCaptureClock{values: fixture.times(10, 11)})
	result, apiErr := coordinator.Checkpoint(context.Background(), fixture.reservation,
		[]string{"result.txt"})
	if apiErr != nil || !result.Replayed || capturer.calls != 0 ||
		strings.Join(storeHarness.order, ",") != "begin,read" {
		fixture.t.Fatalf("%s = (%#v,%#v), live=%d order=%v",
			label, result, apiErr, capturer.calls, storeHarness.order)
	}
}

func (fixture *artifactCaptureFixture) requireNoPhysicalStages() {
	fixture.requirePhysicalStages(0)
}

func (fixture *artifactCaptureFixture) requirePhysicalStages(want int) {
	fixture.t.Helper()
	entries, err := os.ReadDir(filepath.Join(fixture.casPath, ".staging"))
	if err != nil {
		fixture.t.Fatal(err)
	}
	if len(entries) != want {
		fixture.t.Fatalf("physical Artifact stage count = %d, want %d: %v",
			len(entries), want, entries)
	}
}
