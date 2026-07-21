package peer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactReceiverRunStartupPeriodicAndCoalescedTrigger(t *testing.T) {
	t.Run("startup and periodic", func(t *testing.T) {
		backend := &artifactReceiverTestBackend{}
		receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{},
			newArtifactReceiverTestCAS(t), 10*time.Millisecond, 4, nil)
		ctx, cancel := context.WithCancel(context.Background())
		done := runArtifactReceiverForTest(receiver, ctx)
		waitArtifactReceiverCondition(t, func() bool { return receiver.Snapshot().Cycles >= 2 },
			"startup and periodic cycles")
		cancel()
		if err := waitArtifactReceiverResult(t, done); err != nil {
			t.Fatal(err)
		}
		if snapshot := receiver.Snapshot(); snapshot.State != ArtifactReceiverStopped ||
			snapshot.ClaimScans < uint64(HermeticLimits().InboxWorkers) {
			t.Fatalf("periodic snapshot = %#v", snapshot)
		}
	})

	t.Run("manual triggers coalesce", func(t *testing.T) {
		entered := make(chan struct{}, 8)
		release := make(chan struct{})
		backend := &artifactReceiverTestBackend{}
		backend.claimHook = func(ctx context.Context, call int) error {
			entered <- struct{}{}
			if call <= HermeticLimits().InboxWorkers {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{},
			newArtifactReceiverTestCAS(t), 10*time.Second, 4, nil)
		ctx, cancel := context.WithCancel(context.Background())
		done := runArtifactReceiverForTest(receiver, ctx)
		for index := 0; index < HermeticLimits().InboxWorkers; index++ {
			waitArtifactReceiverSignal(t, entered, "blocked startup claim")
		}
		for index := 0; index < 100; index++ {
			receiver.Trigger()
		}
		close(release)
		waitArtifactReceiverCondition(t, func() bool { return receiver.Snapshot().Cycles >= 2 },
			"coalesced trigger cycle")
		time.Sleep(30 * time.Millisecond)
		if cycles := receiver.Snapshot().Cycles; cycles != 2 {
			t.Fatalf("coalesced triggers produced %d cycles, want 2", cycles)
		}
		cancel()
		if err := waitArtifactReceiverResult(t, done); err != nil {
			t.Fatal(err)
		}
	})
}

func TestArtifactReceiverClaimsAndPullsAtHermeticWorkerConcurrency(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-concurrency-origin")
	channelID := artifactReceiverChannelID(t, "concurrency")
	content := artifactReceiverContentForTest(t, "concurrency", []byte("shared receiver bytes"))
	claims := make([]artifactReceiverClaim, HermeticLimits().InboxWorkers)
	for index := range claims {
		claims[index] = artifactReceiverClaimForTest(t, fmt.Sprintf("concurrency-%d", index),
			origin, channelID, []model.Digest{content.manifest.RootDigest()})
	}
	backend := &artifactReceiverTestBackend{claims: claims}
	started := make(chan struct{}, len(claims))
	release := make(chan struct{})
	client := &artifactReceiverTestClient{}
	client.manifest = func(ctx context.Context, source model.PeerID,
		request GetManifest,
	) (Manifest, error) {
		if source != origin.PeerID() || request.ChannelID() != channelID ||
			request.RootDigest() != content.manifest.RootDigest() {
			return Manifest{}, ErrArtifactClientResponse
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return Manifest{}, ctx.Err()
		}
		return artifactReceiverManifestFrame(t, content.manifest), nil
	}
	client.block = func(context.Context, model.PeerID, GetBlock) (Block, error) {
		return artifactReceiverBlockFrame(t, content.blockBytes), nil
	}
	receiver := newArtifactReceiverForTest(t, backend, client, newArtifactReceiverTestCAS(t),
		10*time.Second, HermeticLimits().InboxWorkers, &artifactReceiverTestClock{at: at})
	done := make(chan error, 1)
	go func() { done <- receiver.runCycle(context.Background()) }()
	for range claims {
		waitArtifactReceiverSignal(t, started, "concurrent Manifest pull")
	}
	if snapshot := receiver.Snapshot(); snapshot.InFlightClaims != len(claims) ||
		snapshot.InFlightPulls != len(claims) ||
		snapshot.MaximumInFlightPulls > HermeticLimits().PeerArtifactPulls {
		t.Fatalf("bounded concurrency snapshot = %#v", snapshot)
	}
	close(release)
	if err := waitArtifactReceiverResult(t, done); err != nil {
		t.Fatal(err)
	}
	if snapshot := receiver.Snapshot(); snapshot.Ready != uint64(len(claims)) ||
		snapshot.MaximumInFlightClaims != len(claims) || snapshot.MaximumInClosureBuild != 1 {
		t.Fatalf("completed concurrency snapshot = %#v", snapshot)
	}
	backend.mu.Lock()
	sources := append([]model.PeerID(nil), backend.sources...)
	backend.mu.Unlock()
	if len(sources) < len(claims) || len(sources) > 2*len(claims) {
		t.Fatalf("durable direct-source receipts = %d, want [%d,%d]",
			len(sources), len(claims), 2*len(claims))
	}
	for _, source := range sources {
		if source != origin.PeerID() {
			t.Fatalf("durable direct-source receipt = %s, want origin %s", source, origin.PeerID())
		}
	}
}

func TestArtifactReceiverSlowBlockDoesNotHoldClosureBuildGate(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-closure-gate-origin")
	channelID := artifactReceiverChannelID(t, "closure-gate")
	slow := artifactReceiverContentForTest(t, "closure-gate-slow", []byte("slow block"))
	fast := artifactReceiverContentForTest(t, "closure-gate-fast", []byte("fast block"))
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
		artifactReceiverClaimForTest(t, "closure-gate-slow", origin, channelID,
			[]model.Digest{slow.manifest.RootDigest()}),
		artifactReceiverClaimForTest(t, "closure-gate-fast", origin, channelID,
			[]model.Digest{fast.manifest.RootDigest()}),
	}}
	allowFastManifest := make(chan struct{})
	slowBlockEntered := make(chan struct{}, 1)
	fastBlockEntered := make(chan struct{}, 1)
	releaseSlowBlock := make(chan struct{})
	client := &artifactReceiverTestClient{
		manifest: func(ctx context.Context, _ model.PeerID, request GetManifest) (Manifest, error) {
			switch request.RootDigest() {
			case slow.manifest.RootDigest():
				return artifactReceiverManifestFrame(t, slow.manifest), nil
			case fast.manifest.RootDigest():
				select {
				case <-allowFastManifest:
					return artifactReceiverManifestFrame(t, fast.manifest), nil
				case <-ctx.Done():
					return Manifest{}, ctx.Err()
				}
			default:
				return Manifest{}, ErrArtifactClientResponse
			}
		},
		block: func(ctx context.Context, _ model.PeerID, request GetBlock) (Block, error) {
			switch request.BlockDigest() {
			case model.Sum(slow.blockBytes):
				slowBlockEntered <- struct{}{}
				select {
				case <-releaseSlowBlock:
					return artifactReceiverBlockFrame(t, slow.blockBytes), nil
				case <-ctx.Done():
					return Block{}, ctx.Err()
				}
			case model.Sum(fast.blockBytes):
				fastBlockEntered <- struct{}{}
				return artifactReceiverBlockFrame(t, fast.blockBytes), nil
			default:
				return Block{}, ErrArtifactClientResponse
			}
		},
	}
	receiver := newArtifactReceiverForTest(t, backend, client, newArtifactReceiverTestCAS(t),
		10*time.Second, 2, &artifactReceiverTestClock{at: at})
	done := make(chan error, 1)
	go func() { done <- receiver.runCycle(context.Background()) }()
	waitArtifactReceiverSignal(t, slowBlockEntered, "slow block pull")
	close(allowFastManifest)
	waitArtifactReceiverSignal(t, fastBlockEntered,
		"unrelated block pull while the slow block remains in flight")
	close(releaseSlowBlock)
	if err := waitArtifactReceiverResult(t, done); err != nil {
		t.Fatal(err)
	}
	if snapshot := receiver.Snapshot(); snapshot.Ready != 2 ||
		snapshot.MaximumInClosureBuild != 1 || snapshot.MaximumInFlightPulls < 2 {
		t.Fatalf("closure gate concurrency snapshot = %#v", snapshot)
	}
}

func TestArtifactReceiverFullCacheReverifiesCASWithoutNetwork(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-cache-origin")
	channelID := artifactReceiverChannelID(t, "cache")
	content := artifactReceiverContentForTest(t, "cache", []byte("fully cached bytes"))
	cas := newArtifactReceiverTestCAS(t)
	putArtifactReceiverContent(t, cas, content)
	root := artifactReceiverVerifiedRoot(content.manifest, at.Add(-time.Minute))
	backend := &artifactReceiverTestBackend{
		claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "cache", origin,
			channelID, []model.Digest{content.manifest.RootDigest()})},
		roots: map[model.Digest]artifactReceiverCachedRoot{content.manifest.RootDigest(): root},
	}
	client := &artifactReceiverTestClient{
		manifest: func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
			t.Fatal("full cache unexpectedly fetched a Manifest")
			return Manifest{}, ErrArtifactClientTransport
		},
		block: func(context.Context, model.PeerID, GetBlock) (Block, error) {
			t.Fatal("full cache unexpectedly fetched a block")
			return Block{}, ErrArtifactClientTransport
		},
	}
	receiver := newArtifactReceiverForTest(t, backend, client, cas, 10*time.Second, 4,
		&artifactReceiverTestClock{at: at})
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	log := append([]string(nil), backend.log...)
	checkpoint := backend.lastCheckpoint
	sources := append([]model.PeerID(nil), backend.sources...)
	backend.mu.Unlock()
	if fmt.Sprint(log) != fmt.Sprint([]string{"checkpoint", "ready"}) ||
		len(checkpoint.Roots) != 1 || len(checkpoint.Blocks) != 1 || len(sources) != 0 {
		t.Fatalf("cache durable order/closure = %v / %#v", log, checkpoint)
	}
	if snapshot := receiver.Snapshot(); snapshot.ManifestCacheHits != 1 ||
		snapshot.BlockCacheHits != 1 || snapshot.ManifestPulls != 0 ||
		snapshot.BlockPulls != 0 || snapshot.Ready != 1 {
		t.Fatalf("full cache snapshot = %#v", snapshot)
	}
}

func TestArtifactReceiverDurableStagedCacheResumesWithoutNetwork(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-staged-cache-origin")
	channelID := artifactReceiverChannelID(t, "staged-cache")
	content := artifactReceiverContentForTest(t, "staged-cache", []byte("staged cache bytes"))
	cas := newArtifactReceiverTestCAS(t)
	putArtifactReceiverContent(t, cas, content)
	root := artifactReceiverCachedRoot{rootDigest: content.manifest.RootDigest(),
		manifest: content.manifest.CanonicalJSON(), manifestDigest: content.manifest.ManifestDigest(),
		totalBytes: content.manifest.TotalBytes(), createdAt: at.Add(-time.Minute)}
	backend := &artifactReceiverTestBackend{
		claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "staged-cache", origin,
			channelID, []model.Digest{content.manifest.RootDigest()})},
		roots: map[model.Digest]artifactReceiverCachedRoot{content.manifest.RootDigest(): root},
	}
	client := &artifactReceiverTestClient{
		manifest: func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
			t.Fatal("durable staged cache unexpectedly fetched a Manifest")
			return Manifest{}, ErrArtifactClientTransport
		},
		block: func(context.Context, model.PeerID, GetBlock) (Block, error) {
			t.Fatal("durable staged cache unexpectedly fetched a block")
			return Block{}, ErrArtifactClientTransport
		},
	}
	receiver := newArtifactReceiverForTest(t, backend, client, cas, 10*time.Second, 4,
		&artifactReceiverTestClock{at: at})
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	stages := backend.checkpointCalls
	ready := backend.readyCalls
	backend.mu.Unlock()
	if snapshot := receiver.Snapshot(); snapshot.ManifestCacheHits != 1 ||
		snapshot.BlockCacheHits != 1 || snapshot.ManifestPulls != 0 ||
		snapshot.BlockPulls != 0 || snapshot.Ready != 1 || stages != 1 || ready != 1 {
		t.Fatalf("durable staged resume = stages %d ready %d snapshot %#v",
			stages, ready, snapshot)
	}
}

func TestArtifactReceiverZeroRootClaimMarksReadyDirectly(t *testing.T) {
	origin := testkit.NewIdentity(t, "artifact-receiver-zero-root-origin")
	channelID := artifactReceiverChannelID(t, "zero-root")
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
		artifactReceiverClaimForTest(t, "zero-root", origin, channelID, nil),
	}}
	receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{},
		newArtifactReceiverTestCAS(t), 10*time.Second, 4, nil)
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	log := append([]string(nil), backend.log...)
	backend.mu.Unlock()
	if fmt.Sprint(log) != fmt.Sprint([]string{"ready"}) ||
		receiver.Snapshot().Ready != 1 || receiver.Snapshot().Checkpoints != 0 ||
		receiver.Snapshot().ManifestPulls+receiver.Snapshot().BlockPulls != 0 {
		t.Fatalf("zero-root receive = log %v snapshot %#v", log, receiver.Snapshot())
	}
}

func TestArtifactReceiverRejectsClaimScopeAndRootDriftAsStoreFatal(t *testing.T) {
	origin := testkit.NewIdentity(t, "artifact-receiver-claim-drift-origin")
	other := testkit.NewIdentity(t, "artifact-receiver-claim-drift-other")
	channelID := artifactReceiverChannelID(t, "claim-drift")
	content := artifactReceiverContentForTest(t, "claim-drift", []byte("claim drift bytes"))
	claim := artifactReceiverClaimForTest(t, "claim-drift", origin, channelID,
		[]model.Digest{content.manifest.RootDigest()})
	claim.originPeerID = other.PeerID()
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{claim}}
	receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{},
		newArtifactReceiverTestCAS(t), 10*time.Second, 4, nil)
	err := <-runArtifactReceiverForTest(receiver, context.Background())
	if !errors.Is(err, ErrArtifactReceiverInvariant) ||
		receiver.Snapshot().FatalCode != ArtifactReceiverFatalStoreInvariant {
		t.Fatalf("claim drift = %v, snapshot %#v", err, receiver.Snapshot())
	}
}

func TestArtifactReceiverCollectsAllManifestsBeforeUniqueBlocks(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-multiroot-origin")
	channelID := artifactReceiverChannelID(t, "multiroot")
	shared := []byte("one shared block")
	first := artifactReceiverContentForTest(t, "multiroot-a", shared)
	second := artifactReceiverContentForTest(t, "multiroot-b", shared)
	roots := []model.Digest{first.manifest.RootDigest(), second.manifest.RootDigest()}
	sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })
	manifests := map[model.Digest]artifactdomain.Manifest{
		first.manifest.RootDigest(): first.manifest, second.manifest.RootDigest(): second.manifest,
	}
	var mu sync.Mutex
	log := make([]string, 0, 3)
	blockRequests := make([]GetBlock, 0, 1)
	client := &artifactReceiverTestClient{}
	client.manifest = func(_ context.Context, _ model.PeerID, request GetManifest) (Manifest, error) {
		mu.Lock()
		log = append(log, "manifest:"+request.RootDigest().String())
		mu.Unlock()
		return artifactReceiverManifestFrame(t, manifests[request.RootDigest()]), nil
	}
	client.block = func(_ context.Context, _ model.PeerID, request GetBlock) (Block, error) {
		mu.Lock()
		log = append(log, "block:"+request.BlockDigest().String())
		blockRequests = append(blockRequests, request)
		mu.Unlock()
		return artifactReceiverBlockFrame(t, shared), nil
	}
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
		artifactReceiverClaimForTest(t, "multiroot", origin, channelID, roots),
	}}
	receiver := newArtifactReceiverForTest(t, backend, client, newArtifactReceiverTestCAS(t),
		10*time.Second, 4, &artifactReceiverTestClock{at: at})
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	gotLog := append([]string(nil), log...)
	gotBlocks := append([]GetBlock(nil), blockRequests...)
	mu.Unlock()
	if len(gotLog) != 3 || len(gotBlocks) != 1 ||
		gotLog[0][:9] != "manifest:" || gotLog[1][:9] != "manifest:" ||
		gotLog[2][:6] != "block:" {
		t.Fatalf("network order/shared block calls = %v / %d", gotLog, len(gotBlocks))
	}
	wantOwner := roots[0]
	if gotBlocks[0].RootDigest() != wantOwner || gotBlocks[0].ChannelID() != channelID {
		t.Fatalf("deterministic block authority = %#v, want root %s", gotBlocks[0], wantOwner)
	}
}

func TestArtifactReceiverResumesPartialCASAcrossWorkerRestart(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-resume-origin")
	channelID := artifactReceiverChannelID(t, "resume")
	firstBytes := bytes.Repeat([]byte{'a'}, artifactdomain.BlockSize)
	secondBytes := []byte("must be resumed")
	manifest := artifactReceiverTwoBlockManifest(t, "resume", firstBytes, secondBytes)
	cas := newArtifactReceiverTestCAS(t)
	if _, err := cas.Put(model.Sum(firstBytes), firstBytes); err != nil {
		t.Fatal(err)
	}
	claim := artifactReceiverClaimForTest(t, "resume", origin, channelID,
		[]model.Digest{manifest.RootDigest()})
	var blockCalls atomic.Int32
	client := &artifactReceiverTestClient{}
	client.manifest = func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
		return artifactReceiverManifestFrame(t, manifest), nil
	}
	client.block = func(_ context.Context, _ model.PeerID, request GetBlock) (Block, error) {
		blockCalls.Add(1)
		if request.BlockDigest() != model.Sum(secondBytes) {
			t.Fatalf("partial CAS fetched existing block %s", request.BlockDigest())
		}
		return artifactReceiverBlockFrame(t, secondBytes), nil
	}
	firstBackend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{claim}}
	firstReceiver := newArtifactReceiverForTest(t, firstBackend, client, cas, 10*time.Second, 4,
		&artifactReceiverTestClock{at: at})
	if err := firstReceiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if blockCalls.Load() != 1 {
		t.Fatalf("initial partial resume block calls = %d, want 1", blockCalls.Load())
	}

	secondBackend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{claim}}
	secondReceiver := newArtifactReceiverForTest(t, secondBackend, client, cas, 10*time.Second, 4,
		&artifactReceiverTestClock{at: at.Add(time.Second)})
	if err := secondReceiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if blockCalls.Load() != 1 || secondReceiver.Snapshot().ManifestPulls != 1 ||
		secondReceiver.Snapshot().BlockPulls != 0 ||
		secondReceiver.Snapshot().BlockCacheHits != 2 {
		t.Fatalf("restart did not reuse CAS: calls=%d snapshot=%#v",
			blockCalls.Load(), secondReceiver.Snapshot())
	}
}

func TestArtifactReceiverStopsPullingWhenLocalAuthorityChanges(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-authority-change-origin")
	channelID := artifactReceiverChannelID(t, "authority-change")
	firstBytes := bytes.Repeat([]byte{'a'}, artifactdomain.BlockSize)
	secondBytes := []byte("must never be pulled after revocation")
	manifest := artifactReceiverTwoBlockManifest(t, "authority-change", firstBytes, secondBytes)
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
		artifactReceiverClaimForTest(t, "authority-change", origin, channelID,
			[]model.Digest{manifest.RootDigest()}),
	}}
	var blockCalls atomic.Int32
	client := &artifactReceiverTestClient{
		manifest: func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
			return artifactReceiverManifestFrame(t, manifest), nil
		},
		block: func(_ context.Context, _ model.PeerID, request GetBlock) (Block, error) {
			if call := blockCalls.Add(1); call != 1 {
				t.Fatalf("block RPC after local authority loss: call %d for %s", call,
					request.BlockDigest())
			}
			if request.BlockDigest() != model.Sum(firstBytes) {
				t.Fatalf("first block RPC = %s, want %s", request.BlockDigest(),
					model.Sum(firstBytes))
			}
			backend.mu.Lock()
			backend.probeError = store.ErrPeerInboxArtifactAuthority
			backend.mu.Unlock()
			return artifactReceiverBlockFrame(t, firstBytes), nil
		},
	}
	var reconciles atomic.Int32
	receiver := newArtifactReceiverForTest(t, backend, client, newArtifactReceiverTestCAS(t),
		10*time.Second, 4, &artifactReceiverTestClock{at: at},
		&artifactReceiverTestReconciler{reconcile: func(context.Context,
			model.ChannelID, model.PeerID,
		) error {
			reconciles.Add(1)
			return nil
		}})
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	retries := append([]artifactReceiverTestRetry(nil), backend.retries...)
	checkpoints := backend.checkpointCalls
	ready := backend.readyCalls
	backend.mu.Unlock()
	if blockCalls.Load() != 1 || reconciles.Load() != 1 || checkpoints != 1 || ready != 0 ||
		len(retries) != 1 ||
		retries[0].diagnostic != store.PeerInboxArtifactRetryNotAuthorized {
		t.Fatalf("authority-loss outcome: blocks=%d reconciles=%d checkpoints=%d ready=%d retries=%#v",
			blockCalls.Load(), reconciles.Load(), checkpoints, ready, retries)
	}
}

func TestArtifactReceiverConvertsSettlementRenewalAuthorityLoss(t *testing.T) {
	at := artifactReceiverTestTime()
	clock := &artifactReceiverTestClock{at: at}
	origin := testkit.NewIdentity(t, "artifact-receiver-settlement-authority-origin")
	channelID := artifactReceiverChannelID(t, "settlement-authority")
	content := artifactReceiverContentForTest(t, "settlement-authority",
		[]byte("settlement authority bytes"))
	backend := &artifactReceiverTestBackend{
		claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t,
			"settlement-authority", origin, channelID,
			[]model.Digest{content.manifest.RootDigest()})},
		leaseDuration: 120 * time.Second,
		renewError:    store.ErrPeerInboxArtifactAuthority,
	}
	backend.readRootFn = func(context.Context, artifactReceiverFence, model.Digest,
		time.Time,
	) (artifactReceiverCachedRoot, bool, error) {
		clock.Advance(70 * time.Second)
		return artifactReceiverCachedRoot{}, false, store.ErrPeerInboxArtifactLimit
	}
	var reconciles atomic.Int32
	receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{},
		newArtifactReceiverTestCAS(t), 10*time.Second, 4, clock,
		&artifactReceiverTestReconciler{reconcile: func(context.Context,
			model.ChannelID, model.PeerID,
		) error {
			reconciles.Add(1)
			return nil
		}})
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	retries := append([]artifactReceiverTestRetry(nil), backend.retries...)
	quarantines := append([]store.PeerInboxArtifactPermanentDiagnostic(nil),
		backend.quarantines...)
	backend.mu.Unlock()
	if reconciles.Load() != 1 || len(retries) != 1 || len(quarantines) != 0 ||
		retries[0].diagnostic != store.PeerInboxArtifactRetryNotAuthorized ||
		receiver.Snapshot().FatalCode != ArtifactReceiverFatalNone {
		t.Fatalf("settlement authority conversion: reconciles=%d retries=%#v quarantines=%#v snapshot=%#v",
			reconciles.Load(), retries, quarantines, receiver.Snapshot())
	}
}

func TestArtifactReceiverReadyAuthorityResourceAndStaleOutcomes(t *testing.T) {
	t.Run("ready authority reconciles then retries", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-ready-authority-origin")
		channelID := artifactReceiverChannelID(t, "ready-authority")
		content := artifactReceiverContentForTest(t, "ready-authority", []byte("authority bytes"))
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "ready-authority",
				origin, channelID, []model.Digest{content.manifest.RootDigest()})},
			readyErrors: []error{store.ErrPeerInboxArtifactAuthority},
		}
		client := artifactReceiverContentClient(t, content)
		var reconciles atomic.Int32
		reconciler := &artifactReceiverTestReconciler{reconcile: func(reconcileContext context.Context,
			gotChannel model.ChannelID, gotPeer model.PeerID,
		) error {
			if gotChannel != channelID || gotPeer != origin.PeerID() {
				t.Fatalf("authority reconcile scope = %s/%s", gotChannel, gotPeer)
			}
			deadline, ok := reconcileContext.Deadline()
			remaining := time.Until(deadline)
			if !ok || remaining <= 0 || remaining > HermeticLimits().ChannelRequestTimeout {
				t.Fatalf("authority reconcile deadline = %v, present=%t", remaining, ok)
			}
			reconciles.Add(1)
			return nil
		}}
		receiver := newArtifactReceiverForTest(t, backend, client,
			newArtifactReceiverTestCAS(t), 10*time.Second, 4,
			&artifactReceiverTestClock{at: at}, reconciler)
		if err := receiver.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		backend.mu.Lock()
		retries := append([]artifactReceiverTestRetry(nil), backend.retries...)
		log := append([]string(nil), backend.log...)
		backend.mu.Unlock()
		if reconciles.Load() != 1 || len(retries) != 1 ||
			retries[0].diagnostic != store.PeerInboxArtifactRetryNotAuthorized ||
			fmt.Sprint(log) != fmt.Sprint([]string{"checkpoint", "ready", "retry"}) {
			t.Fatalf("authority race = reconciles %d retries %#v log %v",
				reconciles.Load(), retries, log)
		}
	})

	t.Run("reconciliation deadline failure still releases the claim", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-reconcile-timeout-origin")
		channelID := artifactReceiverChannelID(t, "reconcile-timeout")
		content := artifactReceiverContentForTest(t, "reconcile-timeout", []byte("timeout bytes"))
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "reconcile-timeout",
				origin, channelID, []model.Digest{content.manifest.RootDigest()})},
			readyErrors: []error{store.ErrPeerInboxArtifactAuthority},
		}
		reconciler := &artifactReceiverTestReconciler{reconcile: func(ctx context.Context,
			_ model.ChannelID, _ model.PeerID,
		) error {
			deadline, ok := ctx.Deadline()
			remaining := time.Until(deadline)
			if !ok || remaining <= 0 || remaining > HermeticLimits().ChannelRequestTimeout {
				t.Fatalf("reconciliation timeout deadline = %v, present=%t", remaining, ok)
			}
			return context.DeadlineExceeded
		}}
		receiver := newArtifactReceiverForTest(t, backend,
			artifactReceiverContentClient(t, content), newArtifactReceiverTestCAS(t),
			10*time.Second, 4, &artifactReceiverTestClock{at: at}, reconciler)
		if err := receiver.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		backend.mu.Lock()
		retries := append([]artifactReceiverTestRetry(nil), backend.retries...)
		backend.mu.Unlock()
		snapshot := receiver.Snapshot()
		if len(retries) != 1 ||
			retries[0].diagnostic != store.PeerInboxArtifactRetryNotAuthorized ||
			snapshot.Reconciliations != 1 || snapshot.ReconciliationFailures != 1 {
			t.Fatalf("reconciliation timeout outcome = retries %#v snapshot %#v", retries, snapshot)
		}
	})

	t.Run("outer cancellation during reconciliation never settles", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-reconcile-cancel-origin")
		channelID := artifactReceiverChannelID(t, "reconcile-cancel")
		content := artifactReceiverContentForTest(t, "reconcile-cancel", []byte("cancel bytes"))
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "reconcile-cancel",
				origin, channelID, []model.Digest{content.manifest.RootDigest()})},
			readyErrors: []error{store.ErrPeerInboxArtifactAuthority},
		}
		ctx, cancel := context.WithCancel(context.Background())
		reconciler := &artifactReceiverTestReconciler{reconcile: func(context.Context,
			model.ChannelID, model.PeerID,
		) error {
			cancel()
			return context.Canceled
		}}
		receiver := newArtifactReceiverForTest(t, backend,
			artifactReceiverContentClient(t, content), newArtifactReceiverTestCAS(t),
			10*time.Second, 4, &artifactReceiverTestClock{at: at}, reconciler)
		if err := receiver.runCycle(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		backend.mu.Lock()
		settlements := len(backend.retries) + len(backend.quarantines)
		backend.mu.Unlock()
		if settlements != 0 || receiver.Snapshot().Retries != 0 ||
			receiver.Snapshot().Quarantines != 0 {
			t.Fatalf("reconciliation cancellation settled claim: %d / %#v",
				settlements, receiver.Snapshot())
		}
	})

	t.Run("CAS resource exhaustion retries", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-resource-origin")
		channelID := artifactReceiverChannelID(t, "resource")
		content := artifactReceiverContentForTest(t, "resource", []byte("resource bytes"))
		backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
			artifactReceiverClaimForTest(t, "resource", origin, channelID,
				[]model.Digest{content.manifest.RootDigest()}),
		}}
		cas := &artifactReceiverFailingCAS{CAS: newArtifactReceiverTestCAS(t),
			putError: fmt.Errorf("write CAS temp: %w", syscall.ENOSPC)}
		receiver := newArtifactReceiverForTest(t, backend,
			artifactReceiverContentClient(t, content), cas, 10*time.Second, 4,
			&artifactReceiverTestClock{at: at})
		if err := receiver.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		backend.mu.Lock()
		retries := append([]artifactReceiverTestRetry(nil), backend.retries...)
		backend.mu.Unlock()
		if len(retries) != 1 ||
			retries[0].diagnostic != store.PeerInboxArtifactRetryResourceExhausted {
			t.Fatalf("resource retry = %#v", retries)
		}
	})

	t.Run("ready stale is benign", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-ready-stale-origin")
		channelID := artifactReceiverChannelID(t, "ready-stale")
		content := artifactReceiverContentForTest(t, "ready-stale", []byte("ready stale bytes"))
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "ready-stale",
				origin, channelID, []model.Digest{content.manifest.RootDigest()})},
			readyErrors: []error{store.ErrPeerInboxArtifactStale},
		}
		receiver := newArtifactReceiverForTest(t, backend,
			artifactReceiverContentClient(t, content), newArtifactReceiverTestCAS(t),
			10*time.Second, 4, &artifactReceiverTestClock{at: at})
		if err := receiver.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if snapshot := receiver.Snapshot(); snapshot.StaleClaims != 1 ||
			snapshot.Ready+snapshot.Retries+snapshot.Quarantines != 0 {
			t.Fatalf("ready stale snapshot = %#v", snapshot)
		}
	})
}

func TestArtifactReceiverRemoteFailureAndLimitMatrix(t *testing.T) {
	tests := []struct {
		name            string
		clientError     error
		manifest        func(testing.TB, model.Digest) Manifest
		wantRetry       store.PeerInboxArtifactRetryDiagnostic
		wantPermanent   store.PeerInboxArtifactPermanentDiagnostic
		wantDelay       time.Duration
		wantReconciles  int
		readError       error
		responseRootBad bool
	}{
		{name: "busy minimum", clientError: &ArtifactRemoteFailure{code: ArtifactErrorBusy},
			wantRetry: store.PeerInboxArtifactRetryBusy, wantDelay: time.Second},
		{name: "busy maximum", clientError: &ArtifactRemoteFailure{code: ArtifactErrorBusy,
			retryable: true, retryAfter: 10 * time.Minute},
			wantRetry: store.PeerInboxArtifactRetryBusy, wantDelay: 300 * time.Second},
		{name: "not authorized", clientError: &ArtifactRemoteFailure{code: ArtifactErrorNotAuthorized},
			wantRetry: store.PeerInboxArtifactRetryNotAuthorized, wantDelay: time.Second,
			wantReconciles: 1},
		{name: "source corrupt", clientError: &ArtifactRemoteFailure{code: ArtifactErrorCorrupt},
			wantPermanent: store.PeerInboxArtifactDigestMismatch},
		{name: "transport", clientError: ErrArtifactClientTransport,
			wantRetry: store.PeerInboxArtifactRetryTransportUnavailable, wantDelay: time.Second},
		{name: "EOF", clientError: io.EOF,
			wantRetry: store.PeerInboxArtifactRetryTransportUnavailable, wantDelay: time.Second},
		{name: "reset", clientError: syscall.ECONNRESET,
			wantRetry: store.PeerInboxArtifactRetryTransportUnavailable, wantDelay: time.Second},
		{name: "timeout", clientError: context.DeadlineExceeded,
			wantRetry: store.PeerInboxArtifactRetryTimeout, wantDelay: time.Second},
		{name: "protocol", clientError: ErrArtifactClientResponse,
			wantPermanent: store.PeerInboxArtifactProtocolInvalid},
		{name: "typed manifest invalid", clientError: ErrArtifactClientManifestInvalid,
			wantPermanent: store.PeerInboxArtifactManifestInvalid},
		{name: "typed digest mismatch", clientError: ErrArtifactClientDigestMismatch,
			wantPermanent: store.PeerInboxArtifactDigestMismatch},
		{name: "manifest", manifest: func(t testing.TB, root model.Digest) Manifest {
			invalid, err := model.NewJSON([]byte(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			return Manifest{rootDigest: root, manifestDigest: model.Sum(invalid.Bytes()),
				manifest: invalid}
		},
			wantPermanent: store.PeerInboxArtifactManifestInvalid},
		{name: "digest", responseRootBad: true,
			wantPermanent: store.PeerInboxArtifactDigestMismatch},
		{name: "Store limit", readError: store.ErrPeerInboxArtifactLimit,
			wantPermanent: store.PeerInboxArtifactLimitExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			at := artifactReceiverTestTime()
			origin := testkit.NewIdentity(t, "artifact-receiver-matrix-"+test.name)
			channelID := artifactReceiverChannelID(t, "matrix-"+artifactReceiverSafeName(test.name))
			content := artifactReceiverContentForTest(t, "matrix-"+artifactReceiverSafeName(test.name),
				[]byte("matrix bytes"))
			claim := artifactReceiverClaimForTest(t, "matrix-"+artifactReceiverSafeName(test.name),
				origin, channelID, []model.Digest{content.manifest.RootDigest()})
			backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{claim}}
			if test.readError != nil {
				backend.readRootFn = func(context.Context, artifactReceiverFence, model.Digest,
					time.Time,
				) (artifactReceiverCachedRoot, bool, error) {
					return artifactReceiverCachedRoot{}, false, test.readError
				}
			}
			client := &artifactReceiverTestClient{}
			client.manifest = func(requestContext context.Context, _ model.PeerID,
				_ GetManifest,
			) (Manifest, error) {
				if test.name == "timeout" {
					deadline, ok := requestContext.Deadline()
					remaining := time.Until(deadline)
					if !ok || remaining < 29*time.Second || remaining > 31*time.Second {
						t.Fatalf("receiver request deadline = %v, present=%t", remaining, ok)
					}
				}
				if test.clientError != nil {
					return Manifest{}, test.clientError
				}
				if test.manifest != nil {
					return test.manifest(t, content.manifest.RootDigest()), nil
				}
				if test.responseRootBad {
					other := artifactReceiverContentForTest(t, "matrix-other-"+
						artifactReceiverSafeName(test.name), []byte("other bytes"))
					return artifactReceiverManifestFrame(t, other.manifest), nil
				}
				return artifactReceiverManifestFrame(t, content.manifest), nil
			}
			var reconciles atomic.Int32
			reconciler := &artifactReceiverTestReconciler{reconcile: func(context.Context,
				model.ChannelID, model.PeerID,
			) error {
				reconciles.Add(1)
				return errors.New("reconciliation remains independently unavailable")
			}}
			receiver := newArtifactReceiverForTest(t, backend, client,
				newArtifactReceiverTestCAS(t), 10*time.Second, 4,
				&artifactReceiverTestClock{at: at}, reconciler)
			if err := receiver.runCycle(context.Background()); err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			retries := append([]artifactReceiverTestRetry(nil), backend.retries...)
			quarantines := append([]store.PeerInboxArtifactPermanentDiagnostic(nil),
				backend.quarantines...)
			backend.mu.Unlock()
			if test.wantRetry.Valid() {
				if len(retries) != 1 || retries[0].diagnostic != test.wantRetry ||
					retries[0].after != test.wantDelay || len(quarantines) != 0 {
					t.Fatalf("retry outcome = %#v, quarantines=%v", retries, quarantines)
				}
			} else if len(quarantines) != 1 || quarantines[0] != test.wantPermanent ||
				len(retries) != 0 {
				t.Fatalf("permanent outcome = retries %#v, quarantines %v", retries, quarantines)
			}
			if got := int(reconciles.Load()); got != test.wantReconciles {
				t.Fatalf("reconciles = %d, want %d", got, test.wantReconciles)
			}
		})
	}
}

func TestArtifactReceiverAggregateAndSharedDigestLimitsPrecedeBlockPulls(t *testing.T) {
	tests := []struct {
		name      string
		manifests []artifactdomain.Manifest
		want      store.PeerInboxArtifactPermanentDiagnostic
	}{
		{name: "aggregate", manifests: []artifactdomain.Manifest{
			artifactReceiverLargeManifest(t, "aggregate-a", 160),
			artifactReceiverLargeManifest(t, "aggregate-b", 160),
		}, want: store.PeerInboxArtifactLimitExceeded},
		{name: "shared digest length", manifests: artifactReceiverConflictingLengthManifests(t),
			want: store.PeerInboxArtifactDigestMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			at := artifactReceiverTestTime()
			origin := testkit.NewIdentity(t, "artifact-receiver-limit-"+test.name)
			channelID := artifactReceiverChannelID(t, "limit-"+
				artifactReceiverSafeName(test.name))
			roots := make([]model.Digest, len(test.manifests))
			byRoot := make(map[model.Digest]artifactdomain.Manifest, len(test.manifests))
			for index, manifest := range test.manifests {
				roots[index] = manifest.RootDigest()
				byRoot[manifest.RootDigest()] = manifest
			}
			sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })
			backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
				artifactReceiverClaimForTest(t, "limit-"+artifactReceiverSafeName(test.name),
					origin, channelID, roots),
			}}
			client := &artifactReceiverTestClient{}
			client.manifest = func(_ context.Context, _ model.PeerID,
				request GetManifest,
			) (Manifest, error) {
				return artifactReceiverManifestFrame(t, byRoot[request.RootDigest()]), nil
			}
			client.block = func(context.Context, model.PeerID, GetBlock) (Block, error) {
				t.Fatal("block pull began before aggregate Manifest validation")
				return Block{}, ErrArtifactClientResponse
			}
			receiver := newArtifactReceiverForTest(t, backend, client,
				newArtifactReceiverTestCAS(t), 10*time.Second, 4,
				&artifactReceiverTestClock{at: at})
			if err := receiver.runCycle(context.Background()); err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			got := append([]store.PeerInboxArtifactPermanentDiagnostic(nil), backend.quarantines...)
			backend.mu.Unlock()
			if len(got) != 1 || got[0] != test.want || receiver.Snapshot().BlockPulls != 0 {
				t.Fatalf("pre-block limit outcome = %v, snapshot %#v", got, receiver.Snapshot())
			}
		})
	}
}

func TestArtifactReceiverRenewsAndUsesOnlyReplacementFence(t *testing.T) {
	at := artifactReceiverTestTime()
	clock := &artifactReceiverTestClock{at: at}
	origin := testkit.NewIdentity(t, "artifact-receiver-renew-origin")
	channelID := artifactReceiverChannelID(t, "renew")
	content := artifactReceiverContentForTest(t, "renew", []byte("renewed bytes"))
	backend := &artifactReceiverTestBackend{
		claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "renew", origin,
			channelID, []model.Digest{content.manifest.RootDigest()})},
		leaseDuration: 120 * time.Second,
		checkpointHook: func() {
			clock.Advance(70 * time.Second)
		},
	}
	client := &artifactReceiverTestClient{}
	client.manifest = func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
		clock.Advance(70 * time.Second)
		return artifactReceiverManifestFrame(t, content.manifest), nil
	}
	client.block = func(context.Context, model.PeerID, GetBlock) (Block, error) {
		return artifactReceiverBlockFrame(t, content.blockBytes), nil
	}
	cas := &artifactReceiverAdvancingCAS{CAS: newArtifactReceiverTestCAS(t), clock: clock,
		verifyAdvance: 70 * time.Second}
	receiver := newArtifactReceiverForTest(t, backend, client, cas,
		10*time.Second, 4, clock)
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	renewed := append([]artifactReceiverFence(nil), backend.renewed...)
	readyFences := append([]artifactReceiverFence(nil), backend.readyFences...)
	backend.mu.Unlock()
	if len(renewed) < 3 || len(readyFences) != 1 ||
		!readyFences[0].leaseUntil.Equal(renewed[len(renewed)-1].leaseUntil) ||
		!readyFences[0].leaseUntil.Equal(at.Add(330*time.Second)) {
		t.Fatalf("renewed/ready fences = %#v / %#v", renewed, readyFences)
	}
	if receiver.Snapshot().Renewals == 0 {
		t.Fatalf("renewal snapshot = %#v", receiver.Snapshot())
	}
}

func TestArtifactReceiverCancellationNeverSettlesClaim(t *testing.T) {
	origin := testkit.NewIdentity(t, "artifact-receiver-cancel-origin")
	channelID := artifactReceiverChannelID(t, "cancel")
	content := artifactReceiverContentForTest(t, "cancel", []byte("cancel bytes"))
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
		artifactReceiverClaimForTest(t, "cancel", origin, channelID,
			[]model.Digest{content.manifest.RootDigest()}),
	}}
	entered := make(chan struct{}, 1)
	client := &artifactReceiverTestClient{}
	client.manifest = func(ctx context.Context, _ model.PeerID, _ GetManifest) (Manifest, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return Manifest{}, ctx.Err()
	}
	receiver := newArtifactReceiverForTest(t, backend, client, newArtifactReceiverTestCAS(t),
		10*time.Second, 4, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runArtifactReceiverForTest(receiver, ctx)
	waitArtifactReceiverSignal(t, entered, "blocked receiver request")
	cancel()
	if err := waitArtifactReceiverResult(t, done); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	settlements := len(backend.retries) + len(backend.quarantines) + len(backend.readyFences)
	backend.mu.Unlock()
	if settlements != 0 || receiver.Snapshot().State != ArtifactReceiverStopped {
		t.Fatalf("cancel settlements=%d snapshot=%#v", settlements, receiver.Snapshot())
	}
}

func TestArtifactReceiverTreatsStaleAsBenignAndCASCorruptionAsFatal(t *testing.T) {
	t.Run("stale fence", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-stale-origin")
		channelID := artifactReceiverChannelID(t, "stale")
		content := artifactReceiverContentForTest(t, "stale", []byte("stale bytes"))
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "stale", origin,
				channelID, []model.Digest{content.manifest.RootDigest()})},
			leaseDuration: 30 * time.Second, renewError: store.ErrPeerInboxArtifactStale,
		}
		receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{},
			newArtifactReceiverTestCAS(t), 10*time.Second, 4,
			&artifactReceiverTestClock{at: at})
		if err := receiver.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if snapshot := receiver.Snapshot(); snapshot.StaleClaims != 1 ||
			snapshot.Ready+snapshot.Retries+snapshot.Quarantines != 0 {
			t.Fatalf("stale snapshot = %#v", snapshot)
		}
	})

	t.Run("verified CAS corruption", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-corrupt-origin")
		channelID := artifactReceiverChannelID(t, "corrupt")
		content := artifactReceiverContentForTest(t, "corrupt", []byte("corrupt bytes"))
		cas := newArtifactReceiverTestCAS(t)
		putArtifactReceiverContent(t, cas, content)
		objectPath := artifactReceiverCASObjectPath(cas, content.manifest.ManifestDigest())
		if err := os.Chmod(objectPath, 0o644); err != nil {
			t.Fatal(err)
		}
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "corrupt", origin,
				channelID, []model.Digest{content.manifest.RootDigest()})},
			roots: map[model.Digest]artifactReceiverCachedRoot{
				content.manifest.RootDigest(): artifactReceiverVerifiedRoot(content.manifest,
					at.Add(-time.Minute)),
			},
		}
		receiver := newArtifactReceiverForTest(t, backend, &artifactReceiverTestClient{}, cas,
			10*time.Second, 4, &artifactReceiverTestClock{at: at})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := <-runArtifactReceiverForTest(receiver, ctx)
		if !errors.Is(err, ErrArtifactReceiverInvariant) ||
			receiver.Snapshot().FatalCode != ArtifactReceiverFatalCASInvariant {
			t.Fatalf("CAS corruption = %v, snapshot %#v", err, receiver.Snapshot())
		}
	})

	t.Run("verified CAS block missing", func(t *testing.T) {
		at := artifactReceiverTestTime()
		origin := testkit.NewIdentity(t, "artifact-receiver-missing-block-origin")
		channelID := artifactReceiverChannelID(t, "missing-block")
		content := artifactReceiverContentForTest(t, "missing-block", []byte("missing block bytes"))
		cas := newArtifactReceiverTestCAS(t)
		if _, err := cas.Put(content.manifest.ManifestDigest(),
			content.manifest.CanonicalJSON().Bytes()); err != nil {
			t.Fatal(err)
		}
		backend := &artifactReceiverTestBackend{
			claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "missing-block", origin,
				channelID, []model.Digest{content.manifest.RootDigest()})},
			roots: map[model.Digest]artifactReceiverCachedRoot{
				content.manifest.RootDigest(): artifactReceiverVerifiedRoot(content.manifest,
					at.Add(-time.Minute)),
			},
		}
		client := &artifactReceiverTestClient{block: func(context.Context, model.PeerID,
			GetBlock,
		) (Block, error) {
			t.Fatal("verified root missing block was fetched from network")
			return Block{}, ErrArtifactClientTransport
		}}
		receiver := newArtifactReceiverForTest(t, backend, client, cas,
			10*time.Second, 4, &artifactReceiverTestClock{at: at})
		err := <-runArtifactReceiverForTest(receiver, context.Background())
		if !errors.Is(err, ErrArtifactReceiverInvariant) ||
			receiver.Snapshot().FatalCode != ArtifactReceiverFatalCASInvariant {
			t.Fatalf("missing verified block = %v, snapshot %#v", err, receiver.Snapshot())
		}
	})
}

func TestArtifactReceiverCheckpointReadyOrderAndResponseLossReplay(t *testing.T) {
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-replay-origin")
	channelID := artifactReceiverChannelID(t, "replay")
	content := artifactReceiverContentForTest(t, "replay", []byte("replay bytes"))
	responseLost := errors.New("durable commit response was lost")
	backend := &artifactReceiverTestBackend{
		claims: []artifactReceiverClaim{artifactReceiverClaimForTest(t, "replay", origin,
			channelID, []model.Digest{content.manifest.RootDigest()})},
		checkpointErrors: []error{responseLost, nil}, readyErrors: []error{responseLost, nil},
	}
	client := &artifactReceiverTestClient{}
	client.manifest = func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
		return artifactReceiverManifestFrame(t, content.manifest), nil
	}
	client.block = func(context.Context, model.PeerID, GetBlock) (Block, error) {
		return artifactReceiverBlockFrame(t, content.blockBytes), nil
	}
	receiver := newArtifactReceiverForTest(t, backend, client, newArtifactReceiverTestCAS(t),
		10*time.Second, 4, &artifactReceiverTestClock{at: at})
	if err := receiver.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	log := append([]string(nil), backend.log...)
	backend.mu.Unlock()
	want := []string{"checkpoint", "checkpoint", "ready", "ready"}
	if fmt.Sprint(log) != fmt.Sprint(want) || receiver.Snapshot().Checkpoints != 1 ||
		receiver.Snapshot().Ready != 1 {
		t.Fatalf("response-loss replay order = %v, snapshot %#v", log, receiver.Snapshot())
	}
}

func TestArtifactReceiverConstructorDoubleRunAndPullBounds(t *testing.T) {
	cas := newArtifactReceiverTestCAS(t)
	backend := &artifactReceiverTestBackend{}
	client := &artifactReceiverTestClient{}
	reconciler := &artifactReceiverTestReconciler{}
	clock := &artifactReceiverTestClock{at: artifactReceiverTestTime()}
	if receiver, err := newArtifactReceiver(artifactReceiverConfig{backend: backend,
		client: client, cas: cas, reconciler: reconciler, clock: clock,
		period: artifactReceiverDefaultPeriod + time.Nanosecond, owner: "invalid",
		workers: 4, nodePulls: 16, peerPulls: 4, closureBuilds: 1,
	}); receiver != nil || !errors.Is(err, ErrArtifactReceiver) {
		t.Fatalf("invalid period constructor = (%#v, %v)", receiver, err)
	}
	if receiver, err := newArtifactReceiver(artifactReceiverConfig{backend: backend,
		client: client, cas: cas, reconciler: reconciler, clock: clock,
		period: time.Second, owner: "invalid", workers: 4,
		nodePulls: HermeticLimits().NodeArtifactPulls + 1, peerPulls: 4, closureBuilds: 1,
	}); receiver != nil || !errors.Is(err, ErrArtifactReceiver) {
		t.Fatalf("invalid node bound constructor = (%#v, %v)", receiver, err)
	}

	receiver := newArtifactReceiverForTest(t, backend, client, cas, 10*time.Second, 4, clock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := receiver.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Run(context.Background()); !errors.Is(err, ErrArtifactReceiverRunning) {
		t.Fatalf("double Run error = %v", err)
	}

	limiter, err := newArtifactReceiverPullLimiter(HermeticLimits().NodeArtifactPulls,
		HermeticLimits().PeerArtifactPulls)
	if err != nil {
		t.Fatal(err)
	}
	shared := testkit.NewIdentity(t, "artifact-receiver-limiter-shared").PeerID()
	peers := make([]model.PeerID, 24)
	for index := range peers {
		if index < 8 {
			peers[index] = shared
		} else {
			peers[index] = testkit.NewIdentity(t,
				fmt.Sprintf("artifact-receiver-limiter-%d", index)).PeerID()
		}
	}
	release := make(chan struct{})
	started := make(chan model.PeerID, len(peers))
	var wait sync.WaitGroup
	for _, peerID := range peers {
		wait.Add(1)
		go func(peerID model.PeerID) {
			defer wait.Done()
			releasePull, acquireErr := limiter.acquire(context.Background(), peerID)
			if acquireErr != nil {
				return
			}
			started <- peerID
			<-release
			releasePull()
		}(peerID)
	}
	active := make([]model.PeerID, 0, HermeticLimits().NodeArtifactPulls)
	for len(active) < HermeticLimits().NodeArtifactPulls {
		select {
		case peerID := <-started:
			active = append(active, peerID)
		case <-time.After(3 * time.Second):
			t.Fatalf("node limiter admitted only %d pulls", len(active))
		}
	}
	sharedActive := 0
	for _, peerID := range active {
		if peerID == shared {
			sharedActive++
		}
	}
	if len(active) != HermeticLimits().NodeArtifactPulls || sharedActive < 1 ||
		sharedActive > HermeticLimits().PeerArtifactPulls {
		t.Fatalf("limiter active=%d shared=%d", len(active), sharedActive)
	}
	select {
	case peerID := <-started:
		t.Fatalf("limiter exceeded node bound with %s", peerID)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	wait.Wait()

	peerLimiter, err := newArtifactReceiverPullLimiter(HermeticLimits().NodeArtifactPulls,
		HermeticLimits().PeerArtifactPulls)
	if err != nil {
		t.Fatal(err)
	}
	peerRelease := make(chan struct{})
	peerStarted := make(chan struct{}, 8)
	var peerWait sync.WaitGroup
	for index := 0; index < 8; index++ {
		peerWait.Add(1)
		go func() {
			defer peerWait.Done()
			releasePull, acquireErr := peerLimiter.acquire(context.Background(), shared)
			if acquireErr != nil {
				return
			}
			peerStarted <- struct{}{}
			<-peerRelease
			releasePull()
		}()
	}
	for index := 0; index < HermeticLimits().PeerArtifactPulls; index++ {
		waitArtifactReceiverSignal(t, peerStarted, "same-peer pull slot")
	}
	select {
	case <-peerStarted:
		t.Fatal("same-peer limiter exceeded four pulls")
	case <-time.After(30 * time.Millisecond):
	}
	close(peerRelease)
	peerWait.Wait()
}

type artifactReceiverRealFixture struct {
	requester     testkit.Identity
	origin        testkit.Identity
	channel       *testkit.SignedChannel
	originMember  testkit.MemberFixture
	store         *store.Store
	joinedAt      time.Time
	originHost    host.Host
	requesterHost host.Host
}

func newArtifactReceiverRealFixture(t *testing.T, ctx context.Context,
	name string,
) artifactReceiverRealFixture {
	t.Helper()
	createdAt := artifactReceiverTestTime().Add(-time.Hour)
	requester := testkit.NewIdentity(t, name+"-requester")
	origin := testkit.NewIdentity(t, name+"-origin")
	channel := testkit.NewSignedChannelForOwnerAt(t, name+"-channel", requester, createdAt)
	st := openPeerMeshStore(t, requester, createdAt)
	createPeerMeshChannel(t, st, channel, name+"-channel")
	originMember := channel.AppendActiveIdentity(t, origin)
	mergeAt := originMember.Member().CreatedAt()
	mergePeerMeshRoster(t, st, channel, originMember.Member(), mergeAt)
	baselineAt := mergeAt.Add(time.Second)
	if _, err := st.InstallInboundChannelBaseline(ctx, store.InstallInboundChannelBaselineSpec{
		AuthenticatedPeerID: origin.PeerID(), Baseline: store.ChannelDataBaseline{
			ChannelID: channel.Channel().ID(), OriginPeerID: origin.PeerID(),
			OriginEpoch: origin.OriginEpoch()}, At: baselineAt,
	}); err != nil {
		t.Fatal(err)
	}
	reserved, err := st.ReserveOutboundChannelBaseline(ctx, store.ReserveOutboundChannelBaselineSpec{
		ChannelID: channel.Channel().ID(), TargetPeerID: origin.PeerID(),
		At: baselineAt.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ConfirmOutboundChannelBaseline(ctx, store.ConfirmOutboundChannelBaselineSpec{
		AuthenticatedPeerID: origin.PeerID(), Ack: store.ChannelDataBaselineAck(reserved.Baseline),
		At: baselineAt.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	topics, err := st.ReadChannelTopicRuntime(ctx)
	if err != nil || len(topics) != 1 {
		t.Fatalf("read %s topic = (%#v, %v)", name, topics, err)
	}
	joiningAt := baselineAt.Add(3 * time.Second)
	if !joiningAt.After(topics[0].UpdatedAt) {
		joiningAt = topics[0].UpdatedAt.Add(time.Second)
	}
	joining, err := st.CompareAndSetChannelTopicState(ctx, store.CompareAndSetChannelTopicStateSpec{
		ChannelID: channel.Channel().ID(), ExpectedStatus: topics[0].Status,
		ExpectedRosterHead: topics[0].RosterHead, ExpectedTopicState: topics[0].TopicState,
		TopicState: model.TopicJoining, At: joiningAt,
	})
	if err != nil || joining.Topic.TopicState != model.TopicJoining {
		t.Fatalf("begin %s topic join = (%#v, %v)", name, joining, err)
	}
	joinedAt := joiningAt.Add(time.Second)
	joined, err := st.CompareAndSetChannelTopicState(ctx, store.CompareAndSetChannelTopicStateSpec{
		ChannelID: channel.Channel().ID(), ExpectedStatus: joining.Topic.Status,
		ExpectedRosterHead: joining.Topic.RosterHead, ExpectedTopicState: model.TopicJoining,
		TopicState: model.TopicJoined, At: joinedAt,
	})
	if err != nil || joined.Topic.TopicState != model.TopicJoined {
		t.Fatalf("join %s topic = (%#v, %v)", name, joined, err)
	}
	originHost := newArtifactServerTestHost(t, origin)
	requesterHost := newArtifactServerTestHost(t, requester)
	t.Cleanup(func() {
		_ = requesterHost.Close()
		_ = originHost.Close()
	})
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	return artifactReceiverRealFixture{requester: requester, origin: origin, channel: channel,
		originMember: originMember, store: st, joinedAt: joinedAt,
		originHost: originHost, requesterHost: requesterHost}
}
func TestArtifactReceiverRealStoreCASClientLibp2pHappyAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fixture := newArtifactReceiverRealFixture(t, ctx, "artifact-receiver-real-store")
	requester, origin, channel := fixture.requester, fixture.origin, fixture.channel
	originMember, st, joinedAt := fixture.originMember, fixture.store, fixture.joinedAt
	content := artifactReceiverContentForTest(t, "real-store", []byte("real Store receiver bytes"))
	serverContent := artifactServerContent{channelID: channel.Channel().ID(),
		manifest: content.manifest, rootDigest: content.manifest.RootDigest(),
		blockDigest: model.Sum(content.blockBytes), blockBytes: content.blockBytes}
	source := artifactServerStaticSource(serverContent)
	server, err := NewArtifactServer(context.Background(), ArtifactServerOptions{
		Host: fixture.originHost, Source: source, CAS: serverContent.cas(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := NewArtifactClient(ArtifactClientOptions{Host: fixture.requesterHost})
	if err != nil {
		t.Fatal(err)
	}
	cas := newArtifactReceiverTestCAS(t)
	reconciler := &artifactReceiverTestReconciler{}
	firstAt := joinedAt.Add(10 * time.Second)
	firstPublication := artifactReceiverStorePublicationForTest(t, "real-store-first", channel,
		originMember, requester, 1, content.manifest.RootDigest(), firstAt)
	firstPut, err := st.PutPeerInbox(ctx, store.PutPeerInboxSpec{Publication: firstPublication,
		TransportPeerID: origin.PeerID(), ArrivalSource: model.ArrivalPull, ReceivedAt: firstAt})
	if err != nil || firstPut.Disposition != store.PeerInboxStored {
		t.Fatalf("put first real Inbox = (%#v, %v)", firstPut, err)
	}
	first, err := NewArtifactReceiver(ArtifactReceiverOptions{Store: st, Client: client, CAS: cas,
		Reconciler: reconciler, Clock: &artifactReceiverTestClock{at: firstAt},
		Period: artifactReceiverDefaultPeriod})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	verified, err := st.GetVerifiedArtifactRoot(ctx, content.manifest.RootDigest())
	if err != nil || verified.ManifestDigest != content.manifest.ManifestDigest() ||
		first.Snapshot().Ready != 1 || source.manifestCalls.Load() != 1 ||
		source.blockCalls.Load() != 1 {
		t.Fatalf("first real Store receive = root %#v err %v source %d/%d snapshot %#v",
			verified, err, source.manifestCalls.Load(), source.blockCalls.Load(), first.Snapshot())
	}
	secondAt := firstAt.Add(time.Second)
	secondPublication := artifactReceiverStorePublicationForTest(t, "real-store-second", channel,
		originMember, requester, 2, content.manifest.RootDigest(), secondAt)
	secondPut, err := st.PutPeerInbox(ctx, store.PutPeerInboxSpec{Publication: secondPublication,
		TransportPeerID: origin.PeerID(), ArrivalSource: model.ArrivalPull, ReceivedAt: secondAt})
	if err != nil || secondPut.Disposition != store.PeerInboxStored {
		t.Fatalf("put second real Inbox = (%#v, %v)", secondPut, err)
	}
	second, err := NewArtifactReceiver(ArtifactReceiverOptions{Store: st, Client: client, CAS: cas,
		Reconciler: reconciler, Clock: &artifactReceiverTestClock{at: secondAt},
		Period: artifactReceiverDefaultPeriod})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if source.manifestCalls.Load() != 1 || source.blockCalls.Load() != 1 ||
		second.Snapshot().ManifestCacheHits != 1 || second.Snapshot().BlockCacheHits != 1 ||
		second.Snapshot().ManifestPulls != 0 || second.Snapshot().BlockPulls != 0 ||
		second.Snapshot().Ready != 1 {
		t.Fatalf("real Store cached restart = source %d/%d snapshot %#v",
			source.manifestCalls.Load(), source.blockCalls.Load(), second.Snapshot())
	}
	if claimed, err := st.ClaimPeerInboxArtifact(ctx, store.ClaimPeerInboxArtifactSpec{
		LeaseOwner: "artifact-receiver-real-store-proof", At: secondAt.Add(time.Second),
	}); err != nil || claimed.Found() {
		t.Fatalf("real Store left a claimable Inbox = (%#v, %v)", claimed, err)
	}
}
func TestArtifactReceiverRealStoreStagedPartialRestartAndResponseLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fixture := newArtifactReceiverRealFixture(t, ctx, "artifact-receiver-partial")
	requester, origin, channel := fixture.requester, fixture.origin, fixture.channel
	originMember, st, joinedAt := fixture.originMember, fixture.store, fixture.joinedAt
	originHost, requesterHost := fixture.originHost, fixture.requesterHost
	firstBytes := bytes.Repeat([]byte{'p'}, artifactdomain.BlockSize)
	secondBytes := []byte("partial restart second missing block")
	manifest := artifactReceiverTwoBlockManifest(t, "partial-restart", firstBytes, secondBytes)
	rootDigest := manifest.RootDigest()
	blockBytes := map[model.Digest][]byte{
		model.Sum(firstBytes):  append([]byte(nil), firstBytes...),
		model.Sum(secondBytes): append([]byte(nil), secondBytes...),
	}
	source := &artifactServerTestSource{}
	source.manifest = artifactServerManifestValue(artifactServerManifestView{
		rootDigest: rootDigest, manifestDigest: manifest.ManifestDigest(),
		manifestBytes: manifest.CanonicalJSON().Bytes(), totalBytes: manifest.TotalBytes(),
	})
	source.block = func(_ context.Context,
		spec store.ReadArtifactSourceBlockSpec,
	) (ArtifactSourceBlock, error) {
		content, found := blockBytes[spec.BlockDigest]
		if !found || spec.RootDigest != rootDigest {
			return nil, errors.New("unexpected partial-restart block authorization")
		}
		return artifactServerBlockView{rootDigest: rootDigest,
			blockDigest: spec.BlockDigest, sizeBytes: uint64(len(content))}, nil
	}
	serverObjects := map[model.Digest][]byte{
		manifest.ManifestDigest(): manifest.CanonicalJSON().Bytes(),
	}
	for digest, content := range blockBytes {
		serverObjects[digest] = append([]byte(nil), content...)
	}
	server, err := NewArtifactServer(context.Background(), ArtifactServerOptions{
		Host: originHost, Source: source, CAS: &artifactServerTestCAS{objects: serverObjects},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	realClient, err := NewArtifactClient(ArtifactClientOptions{Host: requesterHost})
	if err != nil {
		t.Fatal(err)
	}
	receiverCAS := newArtifactReceiverTestCAS(t)
	reconciler := &artifactReceiverTestReconciler{}
	firstAt := joinedAt.Add(10 * time.Second)
	publication := artifactReceiverStorePublicationForTest(t, "partial-restart", channel,
		originMember, requester, 1, rootDigest, firstAt)
	put, err := st.PutPeerInbox(ctx, store.PutPeerInboxSpec{Publication: publication,
		TransportPeerID: origin.PeerID(), ArrivalSource: model.ArrivalPull, ReceivedAt: firstAt})
	if err != nil || put.Disposition != store.PeerInboxStored {
		t.Fatalf("put partial-restart Inbox = (%#v, %v)", put, err)
	}
	firstContext, cancelFirst := context.WithCancel(ctx)
	firstStore := &artifactReceiverResponseLossStore{ArtifactReceiverStore: st,
		loseFirstStage: true}
	firstClient := &artifactReceiverRecordingClient{delegate: realClient, cas: receiverCAS,
		cancelBeforeBlock: 2, cancel: cancelFirst}
	first, err := NewArtifactReceiver(ArtifactReceiverOptions{Store: firstStore,
		Client: firstClient, CAS: receiverCAS, Reconciler: reconciler,
		Clock: &artifactReceiverTestClock{at: firstAt}, Period: artifactReceiverDefaultPeriod})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.runCycle(firstContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("partial first worker cancellation = %v", err)
	}
	firstBlockRequests, stageFence := assertArtifactReceiverPartialFirstPass(t,
		firstStore, firstClient, source, first, receiverCAS)
	assertArtifactReceiverPartialStage(t, st, stageFence, rootDigest, firstAt)

	secondAt := firstAt.Add(121 * time.Second)
	secondStore := &artifactReceiverResponseLossStore{ArtifactReceiverStore: st,
		loseFirstReady: true}
	secondClient := &artifactReceiverRecordingClient{delegate: realClient, cas: receiverCAS}
	second, err := NewArtifactReceiver(ArtifactReceiverOptions{Store: secondStore,
		Client: secondClient, CAS: receiverCAS, Reconciler: reconciler,
		Clock: &artifactReceiverTestClock{at: secondAt}, Period: artifactReceiverDefaultPeriod})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	assertArtifactReceiverPartialRestart(t, secondStore, secondClient, source, second,
		firstBlockRequests[1])
	verified, err := st.GetVerifiedArtifactRoot(ctx, rootDigest)
	if err != nil || verified.ManifestDigest != manifest.ManifestDigest() {
		t.Fatalf("partial restart final verified root = (%#v, %v)", verified, err)
	}
	for digest := range blockBytes {
		if _, err := receiverCAS.Read(digest, artifactdomain.BlockSize); err != nil {
			t.Fatalf("final CAS block %s = %v", digest, err)
		}
	}
	if claimed, err := st.ClaimPeerInboxArtifact(ctx, store.ClaimPeerInboxArtifactSpec{
		LeaseOwner: "artifact-receiver-partial-proof", At: secondAt.Add(time.Second),
	}); err != nil || claimed.Found() {
		t.Fatalf("partial restart left a claimable Inbox = (%#v, %v)", claimed, err)
	}
}

func assertArtifactReceiverPartialFirstPass(t *testing.T,
	responseLoss *artifactReceiverResponseLossStore, client *artifactReceiverRecordingClient,
	source *artifactServerTestSource, receiver *ArtifactReceiver, cas artifactReceiverCAS,
) ([]model.Digest, store.PeerInboxArtifactFence) {
	t.Helper()
	stageResults, stageSpecs, _ := responseLoss.snapshot()
	if len(stageResults) != 2 || !stageResults[0].Changed() || stageResults[0].Replayed() ||
		stageResults[1].Changed() || !stageResults[1].Replayed() {
		t.Fatalf("stage response-loss proof = %#v", stageResults)
	}
	if len(stageSpecs) != 2 || stageSpecs[0].Fence != stageSpecs[1].Fence ||
		stageSpecs[0].At != stageSpecs[1].At {
		t.Fatalf("stage replay fence changed = %#v", stageSpecs)
	}
	manifestRequests, blockRequests, observedDurable := client.snapshot()
	if len(manifestRequests) != 1 || len(blockRequests) != 2 || !observedDurable ||
		source.manifestCalls.Load() != 1 || source.blockCalls.Load() != 1 ||
		receiver.Snapshot().Checkpoints != 1 || receiver.Snapshot().Ready != 0 {
		t.Fatalf("partial first worker = manifests %v blocks %v durable %t source %d/%d snapshot %#v",
			manifestRequests, blockRequests, observedDurable, source.manifestCalls.Load(),
			source.blockCalls.Load(), receiver.Snapshot())
	}
	if _, err := cas.Read(blockRequests[0], artifactdomain.BlockSize); err != nil {
		t.Fatalf("first pulled block was not durable: %v", err)
	}
	if _, err := cas.Read(blockRequests[1], artifactdomain.BlockSize); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled block unexpectedly materialized: %v", err)
	}
	return blockRequests, stageSpecs[0].Fence
}

func assertArtifactReceiverPartialStage(t *testing.T, st *store.Store,
	fence store.PeerInboxArtifactFence, rootDigest model.Digest, at time.Time,
) {
	t.Helper()
	staged, found, err := st.ReadPeerInboxArtifactRoot(context.Background(),
		store.ReadPeerInboxArtifactRootSpec{Fence: fence, RootDigest: rootDigest, At: at})
	if err != nil || !found || staged.State() != store.PeerInboxArtifactRootStaged {
		t.Fatalf("durable partial stage = (%#v, found %t, %v)", staged, found, err)
	}
	if _, err := st.GetVerifiedArtifactRoot(context.Background(), rootDigest); !errors.Is(err, store.ErrArtifactUnverified) {
		t.Fatalf("partial stage escaped as verified authority: %v", err)
	}
}

func assertArtifactReceiverPartialRestart(t *testing.T,
	responseLoss *artifactReceiverResponseLossStore, client *artifactReceiverRecordingClient,
	source *artifactServerTestSource, receiver *ArtifactReceiver, missing model.Digest,
) {
	t.Helper()
	manifestRequests, blockRequests, _ := client.snapshot()
	if len(manifestRequests) != 0 || len(blockRequests) != 1 || blockRequests[0] != missing ||
		source.manifestCalls.Load() != 1 || source.blockCalls.Load() != 2 ||
		receiver.Snapshot().ManifestCacheHits != 1 || receiver.Snapshot().ManifestPulls != 0 ||
		receiver.Snapshot().BlockCacheHits != 1 || receiver.Snapshot().BlockPulls != 1 ||
		receiver.Snapshot().Ready != 1 {
		t.Fatalf("partial restart reuse = manifests %v blocks %v source %d/%d snapshot %#v",
			manifestRequests, blockRequests, source.manifestCalls.Load(), source.blockCalls.Load(),
			receiver.Snapshot())
	}
	_, _, readyResults := responseLoss.snapshot()
	if len(readyResults) != 2 || !readyResults[0].Changed() || readyResults[0].Replayed() ||
		readyResults[1].Changed() || !readyResults[1].Replayed() ||
		readyResults[1].Status() != model.InboxReady {
		t.Fatalf("ready response-loss proof = %#v", readyResults)
	}
}

type artifactReceiverResponseLossStore struct {
	ArtifactReceiverStore

	mu             sync.Mutex
	loseFirstStage bool
	loseFirstReady bool
	stageResults   []store.PeerInboxArtifactStage
	stageSpecs     []store.StagePeerInboxArtifactClosureSpec
	readyResults   []store.PeerInboxArtifactSettlement
}

func (st *artifactReceiverResponseLossStore) StagePeerInboxArtifactClosure(ctx context.Context,
	spec store.StagePeerInboxArtifactClosureSpec,
) (store.PeerInboxArtifactStage, error) {
	result, err := st.ArtifactReceiverStore.StagePeerInboxArtifactClosure(ctx, spec)
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
	[]store.StagePeerInboxArtifactClosureSpec, []store.PeerInboxArtifactSettlement,
) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]store.PeerInboxArtifactStage(nil), st.stageResults...),
		append([]store.StagePeerInboxArtifactClosureSpec(nil), st.stageSpecs...),
		append([]store.PeerInboxArtifactSettlement(nil), st.readyResults...)
}

type artifactReceiverRecordingClient struct {
	delegate ArtifactReceiverClient
	cas      *artifactdomain.CAS
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
		if client.cas == nil || first.IsZero() {
			return Block{}, errors.New("partial-restart cancellation has no prior CAS block")
		}
		if _, err := client.cas.Read(first, artifactdomain.BlockSize); err != nil {
			return Block{}, fmt.Errorf("partial-restart prior block is not durable: %w", err)
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

type artifactReceiverTestBackend struct {
	mu sync.Mutex

	claims           []artifactReceiverClaim
	roots            map[model.Digest]artifactReceiverCachedRoot
	leaseDuration    time.Duration
	claimCalls       int
	claimHook        func(context.Context, int) error
	readRootFn       func(context.Context, artifactReceiverFence, model.Digest, time.Time) (artifactReceiverCachedRoot, bool, error)
	renewError       error
	renewed          []artifactReceiverFence
	probeError       error
	probeCalls       int
	checkpointErrors []error
	checkpointCalls  int
	checkpointHook   func()
	lastCheckpoint   store.VerifiedArtifactClosure
	readyErrors      []error
	readyCalls       int
	readyFences      []artifactReceiverFence
	sourceError      error
	sources          []model.PeerID
	retries          []artifactReceiverTestRetry
	quarantines      []store.PeerInboxArtifactPermanentDiagnostic
	log              []string
}

type artifactReceiverTestRetry struct {
	diagnostic store.PeerInboxArtifactRetryDiagnostic
	after      time.Duration
	fence      artifactReceiverFence
}

func (backend *artifactReceiverTestBackend) claim(ctx context.Context, owner string,
	at time.Time,
) (artifactReceiverClaim, bool, error) {
	backend.mu.Lock()
	backend.claimCalls++
	call := backend.claimCalls
	hook := backend.claimHook
	backend.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, call); err != nil {
			return artifactReceiverClaim{}, false, err
		}
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.claims) == 0 {
		return artifactReceiverClaim{}, false, nil
	}
	claim := backend.claims[0]
	backend.claims = backend.claims[1:]
	duration := backend.leaseDuration
	if duration == 0 {
		duration = 120 * time.Second
	}
	claim.fence.leaseOwner = owner
	claim.fence.leaseUntil = at.Add(duration)
	if claim.fence.attempt == 0 {
		claim.fence.attempt = 1
	}
	return claim, true, nil
}

func (backend *artifactReceiverTestBackend) renew(_ context.Context,
	fence artifactReceiverFence, at time.Time,
) (artifactReceiverFence, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.renewError != nil {
		return artifactReceiverFence{}, backend.renewError
	}
	fence.leaseUntil = at.Add(120 * time.Second)
	backend.renewed = append(backend.renewed, fence)
	return fence, nil
}

func (backend *artifactReceiverTestBackend) probe(_ context.Context,
	_ artifactReceiverFence, _ time.Time,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.probeCalls++
	return backend.probeError
}

func (backend *artifactReceiverTestBackend) readRoot(ctx context.Context,
	fence artifactReceiverFence, root model.Digest, at time.Time,
) (artifactReceiverCachedRoot, bool, error) {
	backend.mu.Lock()
	read := backend.readRootFn
	value, found := backend.roots[root]
	backend.mu.Unlock()
	if read != nil {
		return read(ctx, fence, root, at)
	}
	return value, found, nil
}

func (backend *artifactReceiverTestBackend) recordSource(_ context.Context,
	_ artifactReceiverFence, source model.PeerID, _ time.Time,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.sourceError != nil {
		return backend.sourceError
	}
	backend.sources = append(backend.sources, source)
	return nil
}

func (backend *artifactReceiverTestBackend) stage(_ context.Context,
	_ artifactReceiverFence, closure store.VerifiedArtifactClosure, _ time.Time,
) error {
	backend.mu.Lock()
	backend.log = append(backend.log, "checkpoint")
	backend.lastCheckpoint = closure
	call := backend.checkpointCalls
	backend.checkpointCalls++
	hook := backend.checkpointHook
	var result error
	if call < len(backend.checkpointErrors) {
		result = backend.checkpointErrors[call]
	}
	backend.mu.Unlock()
	if hook != nil {
		hook()
	}
	return result
}

func (backend *artifactReceiverTestBackend) ready(_ context.Context,
	fence artifactReceiverFence, _ time.Time,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.log = append(backend.log, "ready")
	backend.readyFences = append(backend.readyFences, fence)
	call := backend.readyCalls
	backend.readyCalls++
	if call < len(backend.readyErrors) {
		return backend.readyErrors[call]
	}
	return nil
}

func (backend *artifactReceiverTestBackend) retry(_ context.Context,
	fence artifactReceiverFence, diagnostic store.PeerInboxArtifactRetryDiagnostic,
	after time.Duration, _ time.Time,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.log = append(backend.log, "retry")
	backend.retries = append(backend.retries, artifactReceiverTestRetry{
		diagnostic: diagnostic, after: after, fence: fence})
	return nil
}

func (backend *artifactReceiverTestBackend) quarantine(_ context.Context,
	_ artifactReceiverFence, diagnostic store.PeerInboxArtifactPermanentDiagnostic,
	_ time.Time,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.log = append(backend.log, "quarantine")
	backend.quarantines = append(backend.quarantines, diagnostic)
	return nil
}

type artifactReceiverTestClient struct {
	manifest func(context.Context, model.PeerID, GetManifest) (Manifest, error)
	block    func(context.Context, model.PeerID, GetBlock) (Block, error)
}

func (client *artifactReceiverTestClient) GetManifest(ctx context.Context, source model.PeerID,
	request GetManifest,
) (Manifest, error) {
	if client == nil || client.manifest == nil {
		return Manifest{}, ErrArtifactClientTransport
	}
	return client.manifest(ctx, source, request)
}

func (client *artifactReceiverTestClient) GetBlock(ctx context.Context, source model.PeerID,
	request GetBlock,
) (Block, error) {
	if client == nil || client.block == nil {
		return Block{}, ErrArtifactClientTransport
	}
	return client.block(ctx, source, request)
}

type artifactReceiverTestReconciler struct {
	reconcile func(context.Context, model.ChannelID, model.PeerID) error
}

func (reconciler *artifactReceiverTestReconciler) ReconcileArtifactReceiver(ctx context.Context,
	channelID model.ChannelID, peerID model.PeerID,
) error {
	if reconciler == nil || reconciler.reconcile == nil {
		return nil
	}
	return reconciler.reconcile(ctx, channelID, peerID)
}

type artifactReceiverTestClock struct {
	mu sync.Mutex
	at time.Time
}

func (clock *artifactReceiverTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.at
}

func (clock *artifactReceiverTestClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.at = clock.at.Add(duration)
	clock.mu.Unlock()
}

type artifactReceiverAdvancingCAS struct {
	*artifactdomain.CAS
	clock         *artifactReceiverTestClock
	verifyAdvance time.Duration
}

type artifactReceiverFailingCAS struct {
	*artifactdomain.CAS
	putError error
}

type artifactReceiverNilUseCAS struct{ *artifactdomain.CAS }

func (*artifactReceiverNilUseCAS) AcquireUse() (*artifactdomain.CASLease, error) {
	return nil, nil
}

func (cas *artifactReceiverFailingCAS) Put(digest model.Digest,
	content []byte,
) (artifactdomain.PutResult, error) {
	if cas.putError != nil {
		return artifactdomain.PutResult{}, cas.putError
	}
	return cas.CAS.Put(digest, content)
}

func (cas *artifactReceiverAdvancingCAS) VerifyClosure(ctx context.Context,
	closure artifactdomain.Closure,
) error {
	if cas.verifyAdvance != 0 {
		cas.clock.Advance(cas.verifyAdvance)
	}
	return cas.CAS.VerifyClosure(ctx, closure)
}

type artifactReceiverTestContent struct {
	manifest   artifactdomain.Manifest
	blockBytes []byte
}

func artifactReceiverContentForTest(t testing.TB, name string,
	blockBytes []byte,
) artifactReceiverTestContent {
	t.Helper()
	digest := model.Sum(blockBytes)
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: name + ".txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: name + ".txt", Mode: 0o600, SizeBytes: uint64(len(blockBytes)),
			Blocks: []artifactdomain.ManifestBlock{{Digest: digest,
				LengthBytes: uint64(len(blockBytes))}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifactReceiverTestContent{manifest: manifest,
		blockBytes: append([]byte(nil), blockBytes...)}
}

func artifactReceiverTwoBlockManifest(t testing.TB, name string, first, second []byte,
) artifactdomain.Manifest {
	t.Helper()
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: name + ".txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: name + ".txt", Mode: 0o600,
			SizeBytes: uint64(len(first) + len(second)), Blocks: []artifactdomain.ManifestBlock{
				{Digest: model.Sum(first), LengthBytes: uint64(len(first))},
				{Digest: model.Sum(second), OffsetBytes: uint64(len(first)),
					LengthBytes: uint64(len(second))},
			}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func artifactReceiverLargeManifest(t testing.TB, name string, mebibytes int,
) artifactdomain.Manifest {
	t.Helper()
	blocks := make([]artifactdomain.ManifestBlock, mebibytes)
	for index := range blocks {
		blocks[index] = artifactdomain.ManifestBlock{
			Digest:      model.Sum([]byte(fmt.Sprintf("%s-%d", name, index))),
			OffsetBytes: uint64(index * artifactdomain.BlockSize),
			LengthBytes: artifactdomain.BlockSize,
		}
	}
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: name + ".bin",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: name + ".bin", Mode: 0o600,
			SizeBytes: uint64(mebibytes * artifactdomain.BlockSize), Blocks: blocks}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func artifactReceiverConflictingLengthManifests(t testing.TB) []artifactdomain.Manifest {
	t.Helper()
	digest := model.Sum([]byte("conflicting shared digest"))
	result := make([]artifactdomain.Manifest, 2)
	for index, length := range []uint64{2, 3} {
		name := fmt.Sprintf("conflict-%d", index)
		manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
			RootKind: artifactdomain.EntryFile, RootPath: name,
			Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
				LogicalPath: name, Mode: 0o600, SizeBytes: length,
				Blocks: []artifactdomain.ManifestBlock{{Digest: digest,
					LengthBytes: length}}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		result[index] = manifest
	}
	return result
}

func artifactReceiverClaimForTest(t testing.TB, name string, origin testkit.Identity,
	channelID model.ChannelID, roots []model.Digest,
) artifactReceiverClaim {
	t.Helper()
	roots = append([]model.Digest(nil), roots...)
	sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })
	publication := artifactReceiverPublicationForTest(t, name, origin, channelID, roots)
	inboxID, err := model.ParseInboxID("inbox-artifact-receiver-" + artifactReceiverSafeName(name))
	if err != nil {
		t.Fatal(err)
	}
	return artifactReceiverClaim{fence: artifactReceiverFence{inboxID: inboxID,
		attempt: 1, token: uint64(len(name) + 1)}, publication: publication,
		channelID: channelID, originPeerID: origin.PeerID(), originEpoch: origin.OriginEpoch(),
		requiredRoots: roots}
}

func artifactReceiverPublicationForTest(t testing.TB, name string, origin testkit.Identity,
	channelID model.ChannelID, roots []model.Digest,
) model.SignedPublication {
	t.Helper()
	audiencePeer, err := model.ParsePeerID("peer-artifact-receiver-audience-" +
		artifactReceiverSafeName(name))
	if err != nil {
		t.Fatal(err)
	}
	audience, err := model.NewAudience([]model.PeerID{audiencePeer})
	if err != nil {
		t.Fatal(err)
	}
	workID, err := model.ParseWorkID("work-artifact-receiver-" + artifactReceiverSafeName(name))
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewWorkRef(origin.PeerID(), workID)
	if err != nil {
		t.Fatal(err)
	}
	originHead, _ := model.NewRecordHead(1, model.Sum([]byte("artifact-receiver-origin-"+name)))
	rosterHead, _ := model.NewRecordHead(2, model.Sum([]byte("artifact-receiver-roster-"+name)))
	scope, err := model.NewEventScope(channelID, origin.PeerID(), origin.OriginEpoch(), 1, 1,
		originHead, rosterHead, work)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := make([]model.ArtifactRef, len(roots))
	for index, root := range roots {
		artifacts[index], err = model.NewArtifactRef(root, model.ArtifactProduced)
		if err != nil {
			t.Fatal(err)
		}
	}
	eventID, err := model.ParseEventID("event-artifact-receiver-" + artifactReceiverSafeName(name))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := model.JSONFrom(map[string]any{"receiver": name})
	at := artifactReceiverTestTime()
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-artifact-receiver",
		Type: model.EventReviewOffered, Audience: audience, Summary: "Artifact receiver test",
		Payload: payload, Artifacts: artifacts, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("mnemon/artifact-receiver/test/" + name))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	message, err := model.PublicationSigningMessage(channelID, body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	publication, err := model.AttachSignature(body, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func artifactReceiverStorePublicationForTest(t testing.TB, name string,
	channel *testkit.SignedChannel, origin testkit.MemberFixture, requester testkit.Identity,
	sequence uint64, root model.Digest, at time.Time,
) model.SignedPublication {
	t.Helper()
	audience, err := model.NewAudience([]model.PeerID{requester.PeerID()})
	if err != nil {
		t.Fatal(err)
	}
	workID, err := model.ParseWorkID(fmt.Sprintf("work-artifact-receiver-%s-%d",
		artifactReceiverSafeName(name), sequence))
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewWorkRef(origin.Identity().PeerID(), workID)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := model.NewEventScope(channel.Channel().ID(), origin.Identity().PeerID(),
		origin.Identity().OriginEpoch(), sequence, sequence, origin.Member().Head(),
		channel.Roster().Head(), work)
	if err != nil {
		t.Fatal(err)
	}
	artifactRef, err := model.NewArtifactRef(root, model.ArtifactProduced)
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := model.ParseEventID(fmt.Sprintf("event-artifact-receiver-%s-%d",
		artifactReceiverSafeName(name), sequence))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.JSONFrom(map[string]any{"receiver": name, "sequence": sequence})
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-artifact-receiver-store",
		Type: model.EventReviewOffered, Audience: audience, Summary: "real Store Artifact receive",
		Payload: payload, Artifacts: []model.ArtifactRef{artifactRef},
		CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.PublicationSigningMessage(channel.Channel().ID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	publication, err := model.AttachSignature(body,
		ed25519.Sign(peerMeshPrivateKey(t, origin.Identity()), message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func artifactReceiverManifestFrame(t testing.TB, manifest artifactdomain.Manifest) Manifest {
	t.Helper()
	frame, err := NewManifest(ManifestSpec{RootDigest: manifest.RootDigest(),
		Manifest: manifest.CanonicalJSON()})
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func artifactReceiverContentClient(t testing.TB,
	content artifactReceiverTestContent,
) *artifactReceiverTestClient {
	t.Helper()
	return &artifactReceiverTestClient{
		manifest: func(context.Context, model.PeerID, GetManifest) (Manifest, error) {
			return artifactReceiverManifestFrame(t, content.manifest), nil
		},
		block: func(context.Context, model.PeerID, GetBlock) (Block, error) {
			return artifactReceiverBlockFrame(t, content.blockBytes), nil
		},
	}
}

func artifactReceiverBlockFrame(t testing.TB, content []byte) Block {
	t.Helper()
	frame, err := NewBlock(BlockSpec{BlockDigest: model.Sum(content), BlockBytes: content})
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func artifactReceiverVerifiedRoot(manifest artifactdomain.Manifest,
	at time.Time,
) artifactReceiverCachedRoot {
	return artifactReceiverCachedRoot{rootDigest: manifest.RootDigest(),
		manifest: manifest.CanonicalJSON(), manifestDigest: manifest.ManifestDigest(),
		totalBytes: manifest.TotalBytes(), createdAt: at, verifiedAt: at, verified: true}
}

func putArtifactReceiverContent(t testing.TB, cas *artifactdomain.CAS,
	content artifactReceiverTestContent,
) {
	t.Helper()
	if _, err := cas.Put(content.manifest.ManifestDigest(),
		content.manifest.CanonicalJSON().Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Put(model.Sum(content.blockBytes), content.blockBytes); err != nil {
		t.Fatal(err)
	}
}

func newArtifactReceiverTestCAS(t testing.TB) *artifactdomain.CAS {
	t.Helper()
	cas, err := artifactdomain.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}

func artifactReceiverCASObjectPath(cas *artifactdomain.CAS, digest model.Digest) string {
	hexDigest := digest.String()[len("sha256:"):]
	return filepath.Join(cas.Root(), hexDigest[:2], hexDigest)
}

func newArtifactReceiverForTest(t testing.TB, backend artifactReceiverBackend,
	client ArtifactReceiverClient, cas artifactReceiverCAS, period time.Duration, workers int,
	clock *artifactReceiverTestClock, reconcilers ...ArtifactReceiverReconciler,
) *ArtifactReceiver {
	t.Helper()
	if clock == nil {
		clock = &artifactReceiverTestClock{at: artifactReceiverTestTime()}
	}
	reconciler := ArtifactReceiverReconciler(&artifactReceiverTestReconciler{})
	if len(reconcilers) > 0 {
		reconciler = reconcilers[0]
	}
	receiver, err := newArtifactReceiver(artifactReceiverConfig{backend: backend,
		client: client, cas: cas, reconciler: reconciler, clock: clock, period: period,
		owner: "artifact-receiver-test-owner", workers: workers,
		nodePulls: HermeticLimits().NodeArtifactPulls,
		peerPulls: HermeticLimits().PeerArtifactPulls, closureBuilds: 1})
	if err != nil {
		t.Fatal(err)
	}
	return receiver
}

func runArtifactReceiverForTest(receiver *ArtifactReceiver, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- receiver.Run(ctx) }()
	return done
}

func waitArtifactReceiverResult(t testing.TB, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Artifact receiver did not stop")
		return nil
	}
}

func waitArtifactReceiverSignal(t testing.TB, signal <-chan struct{}, detail string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", detail)
	}
}

func waitArtifactReceiverCondition(t testing.TB, condition func() bool, detail string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", detail)
}

func artifactReceiverChannelID(t testing.TB, name string) model.ChannelID {
	t.Helper()
	channelID, err := model.ParseChannelID("channel-artifact-receiver-" +
		artifactReceiverSafeName(name))
	if err != nil {
		t.Fatal(err)
	}
	return channelID
}

func artifactReceiverSafeName(name string) string {
	result := make([]byte, 0, len(name))
	for index := 0; index < len(name); index++ {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			result = append(result, character)
		} else {
			result = append(result, '-')
		}
	}
	return string(result)
}

func artifactReceiverTestTime() time.Time {
	return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
}
