package peer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactServerServesAuthenticatedManifestAndBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originIdentity := testkit.NewIdentity(t, "artifact-server-success-origin")
	requesterIdentity := testkit.NewIdentity(t, "artifact-server-success-requester")
	originHost := newArtifactServerTestHost(t, originIdentity)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requesterIdentity)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)

	fixture := newArtifactServerContent(t, "success", []byte("artifact server block bytes"))
	source := &artifactServerTestSource{}
	source.manifest = func(ctx context.Context,
		spec store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) < 25*time.Second || time.Until(deadline) > 31*time.Second {
			t.Errorf("Artifact request deadline = %v, present=%t", deadline, ok)
		}
		return fixture.manifestView(), nil
	}
	source.block = func(context.Context,
		store.ReadArtifactSourceBlockSpec,
	) (ArtifactSourceBlock, error) {
		return fixture.blockView(), nil
	}
	cas := fixture.cas()
	server, err := NewArtifactServer(context.Background(), ArtifactServerOptions{
		Host: originHost, Source: source, CAS: cas,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	getManifest, err := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	if err != nil {
		t.Fatal(err)
	}
	response := exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), getManifest)
	manifest, ok := response.Payload().(Manifest)
	if response.Type() != ArtifactFrameManifest || !ok ||
		manifest.RootDigest() != fixture.rootDigest ||
		manifest.ManifestDigest() != fixture.manifest.ManifestDigest() ||
		!bytes.Equal(manifest.ManifestBytes(), fixture.manifest.CanonicalJSON().Bytes()) {
		t.Fatalf("Manifest response = %#v", response)
	}
	manifestSpec := source.lastManifestSpec()
	if manifestSpec.AuthenticatedPeerID != requesterIdentity.PeerID() ||
		manifestSpec.ChannelID != fixture.channelID || manifestSpec.RootDigest != fixture.rootDigest {
		t.Fatalf("authenticated Manifest Store input = %#v", manifestSpec)
	}

	getBlock, err := NewGetBlock(GetBlockSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest, BlockDigest: fixture.blockDigest})
	if err != nil {
		t.Fatal(err)
	}
	response = exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), getBlock)
	block, ok := response.Payload().(Block)
	if response.Type() != ArtifactFrameBlock || !ok || block.BlockDigest() != fixture.blockDigest ||
		!bytes.Equal(block.BlockBytes(), fixture.blockBytes) {
		t.Fatalf("Block response = %#v", response)
	}
	blockSpec := source.lastBlockSpec()
	if blockSpec.AuthenticatedPeerID != requesterIdentity.PeerID() ||
		blockSpec.ChannelID != fixture.channelID || blockSpec.RootDigest != fixture.rootDigest ||
		blockSpec.BlockDigest != fixture.blockDigest {
		t.Fatalf("authenticated Block Store input = %#v", blockSpec)
	}
	reads := cas.readsSnapshot()
	if len(reads) != 2 || reads[0].digest != fixture.manifest.ManifestDigest() ||
		reads[0].maximum != artifactManifestMaximum() || reads[1].digest != fixture.blockDigest ||
		reads[1].maximum != artifactBlockMaximum() {
		t.Fatalf("CAS reads = %#v", reads)
	}
	if countArtifactProtocol(originHost.Mux().Protocols(), ArtifactsProtocol) != 1 {
		t.Fatal("Artifact server did not own exactly one protocol handler")
	}
}

func TestArtifactServerStoreAdapterCallsExactStoreAPIWithoutCaching(t *testing.T) {
	t.Parallel()

	manifestErr := errors.New("manifest Store result")
	blockErr := errors.New("block Store result")
	exact := &artifactServerExactStore{manifestErr: manifestErr, blockErr: blockErr}
	adapter, err := NewArtifactServerStoreSource(exact)
	if err != nil {
		t.Fatal(err)
	}
	peerID, _ := model.ParsePeerID("peer-artifact-adapter")
	channelID, _ := model.ParseChannelID("channel-artifact-adapter")
	rootDigest := model.Sum([]byte("adapter-root"))
	blockDigest := model.Sum([]byte("adapter-block"))
	manifestSpec := store.ReadArtifactSourceManifestSpec{AuthenticatedPeerID: peerID,
		ChannelID: channelID, RootDigest: rootDigest}
	if _, err := adapter.ReadArtifactSourceManifest(context.Background(), manifestSpec); !errors.Is(err, manifestErr) {
		t.Fatalf("Store adapter Manifest error = %v", err)
	}
	blockSpec := store.ReadArtifactSourceBlockSpec{AuthenticatedPeerID: peerID,
		ChannelID: channelID, RootDigest: rootDigest, BlockDigest: blockDigest}
	if _, err := adapter.ReadArtifactSourceBlock(context.Background(), blockSpec); !errors.Is(err, blockErr) {
		t.Fatalf("Store adapter Block error = %v", err)
	}
	if exact.manifestCalls.Load() != 1 || exact.blockCalls.Load() != 1 ||
		exact.manifestSpec != manifestSpec || exact.blockSpec != blockSpec {
		t.Fatalf("exact Store adapter calls = %#v / %#v", exact.manifestSpec, exact.blockSpec)
	}
	var typedNil *artifactServerExactStore
	if adapter, err := NewArtifactServerStoreSource(typedNil); adapter != nil ||
		!errors.Is(err, ErrArtifactServer) {
		t.Fatalf("typed-nil Store adapter = (%#v, %v)", adapter, err)
	}
}

func TestArtifactServerRequiresLiveDependenciesAndReleasesProtocolOwnership(t *testing.T) {
	t.Parallel()

	origin := testkit.NewIdentity(t, "artifact-server-dependencies-origin")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	fixture := newArtifactServerContent(t, "dependencies", []byte("dependency block"))
	source := artifactServerStaticSource(fixture)
	cas := fixture.cas()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	var typedNilSource *artifactServerTestSource
	var typedNilCAS *artifactServerTestCAS
	typedNilHost := reflect.Zero(reflect.TypeOf(originHost)).Interface().(host.Host)
	tests := []struct {
		name     string
		lifetime context.Context
		options  ArtifactServerOptions
	}{
		{name: "nil lifetime", options: ArtifactServerOptions{
			Host: originHost, Source: source, CAS: cas}},
		{name: "canceled lifetime", lifetime: canceled, options: ArtifactServerOptions{
			Host: originHost, Source: source, CAS: cas}},
		{name: "nil Host", lifetime: context.Background(), options: ArtifactServerOptions{
			Source: source, CAS: cas}},
		{name: "typed nil Host", lifetime: context.Background(), options: ArtifactServerOptions{
			Host: typedNilHost, Source: source, CAS: cas}},
		{name: "nil source", lifetime: context.Background(), options: ArtifactServerOptions{
			Host: originHost, CAS: cas}},
		{name: "typed nil source", lifetime: context.Background(), options: ArtifactServerOptions{
			Host: originHost, Source: typedNilSource, CAS: cas}},
		{name: "nil CAS", lifetime: context.Background(), options: ArtifactServerOptions{
			Host: originHost, Source: source}},
		{name: "typed nil CAS", lifetime: context.Background(), options: ArtifactServerOptions{
			Host: originHost, Source: source, CAS: typedNilCAS}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			server, err := NewArtifactServer(test.lifetime, test.options)
			if server != nil || !errors.Is(err, ErrArtifactServer) {
				t.Fatalf("invalid Artifact server dependencies = (%#v, %v)", server, err)
			}
		})
	}
	if countArtifactProtocol(originHost.Mux().Protocols(), ArtifactsProtocol) != 0 {
		t.Fatal("invalid Artifact server retained protocol ownership")
	}
	first, err := NewArtifactServer(context.Background(), ArtifactServerOptions{
		Host: originHost, Source: source, CAS: cas})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewArtifactServer(context.Background(), ArtifactServerOptions{
		Host: originHost, Source: source, CAS: cas})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
}

func TestArtifactServerRejectsDuplicateWithoutReplacingLiveOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-duplicate-origin")
	requester := testkit.NewIdentity(t, "artifact-server-duplicate-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)

	fixture := newArtifactServerContent(t, "duplicate", []byte("duplicate block"))
	firstSource := artifactServerStaticSource(fixture)
	first, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: firstSource, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	replacementSource := artifactServerStaticSource(fixture)
	replacement, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: replacementSource, CAS: fixture.cas()})
	if replacement != nil || !errors.Is(err, ErrArtifactServer) {
		t.Fatalf("duplicate Artifact server = (%p, %v)", replacement, err)
	}

	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	response := exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	if response.Type() != ArtifactFrameManifest || firstSource.manifestCalls.Load() != 1 ||
		replacementSource.manifestCalls.Load() != 0 {
		t.Fatal("rejected duplicate replaced the live Artifact server")
	}
}

func TestArtifactServerMakesUnknownUnauthorizedAndCrossChannelIndistinguishable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-denial-origin")
	requester := testkit.NewIdentity(t, "artifact-server-denial-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)

	source := &artifactServerTestSource{}
	source.manifest = func(context.Context,
		store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error) {
		return nil, store.ErrArtifactSourceUnavailable
	}
	source.block = func(context.Context,
		store.ReadArtifactSourceBlockSpec,
	) (ArtifactSourceBlock, error) {
		return nil, store.ErrArtifactSourceUnavailable
	}
	cas := &artifactServerTestCAS{}
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: cas})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	channelA, _ := model.ParseChannelID("channel-artifact-denial-a")
	channelB, _ := model.ParseChannelID("channel-artifact-denial-b")
	rootA := model.Sum([]byte("artifact-denial-root-a"))
	rootB := model.Sum([]byte("artifact-denial-root-b"))
	requests := []GetManifestSpec{{ChannelID: channelA, RootDigest: rootA},
		{ChannelID: channelA, RootDigest: rootB}, {ChannelID: channelB, RootDigest: rootA}}
	var wantWire string
	for _, spec := range requests {
		request, _ := NewGetManifest(spec)
		response := exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request)
		failure, ok := response.Payload().(ArtifactProtocolError)
		if response.Type() != ArtifactFrameProtocolError || !ok ||
			failure.Code() != ArtifactErrorNotAuthorized || failure.Retryable() {
			t.Fatalf("closed Artifact denial = %#v", response)
		}
		if wantWire == "" {
			wantWire = response.CanonicalJSON().String()
		} else if response.CanonicalJSON().String() != wantWire {
			t.Fatalf("Artifact denial oracle: %s != %s", response.CanonicalJSON().String(), wantWire)
		}
	}
	blockRequest, _ := NewGetBlock(GetBlockSpec{ChannelID: channelA, RootDigest: rootA,
		BlockDigest: model.Sum([]byte("artifact-denial-block"))})
	blockResponse := exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), blockRequest)
	if blockResponse.CanonicalJSON().String() != wantWire {
		t.Fatalf("Artifact block denial oracle: %s != %s",
			blockResponse.CanonicalJSON().String(), wantWire)
	}
	if len(cas.readsSnapshot()) != 0 {
		t.Fatal("unauthorized Artifact request reached the CAS")
	}
}

func TestArtifactServerMapsOnlyDeadlineAndCapacityToBusy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-busy-origin")
	requester := testkit.NewIdentity(t, "artifact-server-busy-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	fixture := newArtifactServerContent(t, "busy", []byte("busy block"))
	source := &artifactServerTestSource{}
	source.manifest = func(context.Context,
		store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error) {
		return nil, context.DeadlineExceeded
	}
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	response := exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	failure, ok := response.Payload().(ArtifactProtocolError)
	if response.Type() != ArtifactFrameProtocolError || !ok || failure.Code() != ArtifactErrorBusy ||
		!failure.Retryable() || failure.RetryAfter() != artifactServerBusyRetry {
		t.Fatalf("deadline response = %#v", response)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}

	resourceCAS := fixture.cas()
	resourceCAS.read = func(model.Digest, int) ([]byte, error) {
		return nil, network.ErrResourceLimitExceeded
	}
	resourceServer, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: artifactServerStaticSource(fixture), CAS: resourceCAS})
	if err != nil {
		t.Fatal(err)
	}
	defer resourceServer.Close()
	response = exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	assertArtifactBusyResponse(t, response)
}

func TestArtifactServerResetsUnsafeSourceAndClosesCASCorruption(t *testing.T) {
	fixture := newArtifactServerContent(t, "corruption", []byte("verified corruption block"))
	tests := []struct {
		name      string
		source    func(*artifactServerTestSource)
		cas       func(*artifactServerTestCAS)
		corrupted bool
	}{
		{name: "source input", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestFailure(store.ErrArtifactSourceInput)
		}},
		{name: "source invariant", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestFailure(store.ErrArtifactSourceInvariant)
		}},
		{name: "joined unavailable invariant", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestFailure(errors.Join(
				store.ErrArtifactSourceUnavailable, store.ErrArtifactSourceInvariant))
		}},
		{name: "unknown source", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestFailure(errors.New("sqlite internal path detail"))
		}},
		{name: "typed nil source view", source: func(source *artifactServerTestSource) {
			source.manifest = func(context.Context,
				store.ReadArtifactSourceManifestSpec,
			) (ArtifactSourceManifest, error) {
				var view *artifactServerManifestView
				return view, nil
			}
		}},
		{name: "mismatched source root", source: func(source *artifactServerTestSource) {
			view := fixture.manifestView()
			view.rootDigest = model.Sum([]byte("wrong root"))
			source.manifest = artifactServerManifestValue(view)
		}},
		{name: "mismatched source digest", source: func(source *artifactServerTestSource) {
			view := fixture.manifestView()
			view.manifestDigest = model.Sum([]byte("wrong manifest"))
			source.manifest = artifactServerManifestValue(view)
		}},
		{name: "mismatched source total", source: func(source *artifactServerTestSource) {
			view := fixture.manifestView()
			view.totalBytes++
			source.manifest = artifactServerManifestValue(view)
		}},
		{name: "malformed source manifest", source: func(source *artifactServerTestSource) {
			view := fixture.manifestView()
			view.manifestBytes = []byte(`{"broken":`)
			view.manifestDigest = model.Sum(view.manifestBytes)
			source.manifest = artifactServerManifestValue(view)
		}},
		{name: "CAS missing", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestValue(fixture.manifestView())
		}, cas: func(cas *artifactServerTestCAS) {
			cas.read = func(model.Digest, int) ([]byte, error) { return nil, errors.New("object path missing") }
		}},
		{name: "CAS changed bytes", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestValue(fixture.manifestView())
		}, cas: func(cas *artifactServerTestCAS) {
			cas.read = func(model.Digest, int) ([]byte, error) { return []byte("changed"), nil }
		}, corrupted: true},
		{name: "CAS digest corruption", source: func(source *artifactServerTestSource) {
			source.manifest = artifactServerManifestValue(fixture.manifestView())
		}, cas: func(cas *artifactServerTestCAS) {
			cas.read = func(model.Digest, int) ([]byte, error) {
				return nil, artifactdomain.ErrCASCorruption
			}
		}, corrupted: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			origin := testkit.NewIdentity(t, "artifact-server-corrupt-origin-"+test.name)
			requester := testkit.NewIdentity(t, "artifact-server-corrupt-requester-"+test.name)
			originHost := newArtifactServerTestHost(t, origin)
			defer originHost.Close()
			requesterHost := newArtifactServerTestHost(t, requester)
			defer requesterHost.Close()
			connectArtifactServerHosts(t, ctx, requesterHost, originHost)
			source := &artifactServerTestSource{}
			test.source(source)
			cas := fixture.cas()
			if test.cas != nil {
				test.cas(cas)
			}
			server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
				Source: source, CAS: cas})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
				RootDigest: fixture.rootDigest})
			if test.corrupted {
				assertArtifactCorruptResponse(t,
					exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request))
			} else {
				assertArtifactServerReset(t, ctx, requesterHost, originHost.ID(), request)
			}
		})
	}

	blockTests := []struct {
		name      string
		source    func(*artifactServerTestSource)
		cas       func(*artifactServerTestCAS)
		corrupted bool
	}{
		{name: "source input", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockFailure(store.ErrArtifactSourceInput)
		}},
		{name: "source invariant", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockFailure(store.ErrArtifactSourceInvariant)
		}},
		{name: "joined unavailable invariant", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockFailure(errors.Join(
				store.ErrArtifactSourceUnavailable, store.ErrArtifactSourceInvariant))
		}},
		{name: "unknown source", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockFailure(errors.New("CAS filesystem detail"))
		}},
		{name: "typed nil source view", source: func(source *artifactServerTestSource) {
			source.block = func(context.Context,
				store.ReadArtifactSourceBlockSpec,
			) (ArtifactSourceBlock, error) {
				var view *artifactServerBlockView
				return view, nil
			}
		}},
		{name: "mismatched source root", source: func(source *artifactServerTestSource) {
			view := fixture.blockView()
			view.rootDigest = model.Sum([]byte("wrong block root"))
			source.block = artifactServerBlockValue(view)
		}},
		{name: "mismatched source digest", source: func(source *artifactServerTestSource) {
			view := fixture.blockView()
			view.blockDigest = model.Sum([]byte("wrong block digest"))
			source.block = artifactServerBlockValue(view)
		}},
		{name: "zero source size", source: func(source *artifactServerTestSource) {
			view := fixture.blockView()
			view.sizeBytes = 0
			source.block = artifactServerBlockValue(view)
		}},
		{name: "oversized source size", source: func(source *artifactServerTestSource) {
			view := fixture.blockView()
			view.sizeBytes = uint64(artifactBlockMaximum() + 1)
			source.block = artifactServerBlockValue(view)
		}},
		{name: "CAS missing", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockValue(fixture.blockView())
		}, cas: func(cas *artifactServerTestCAS) {
			cas.read = func(model.Digest, int) ([]byte, error) {
				return nil, errors.New("CAS object path missing")
			}
		}},
		{name: "CAS changed bytes", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockValue(fixture.blockView())
		}, cas: func(cas *artifactServerTestCAS) {
			cas.read = func(model.Digest, int) ([]byte, error) {
				return []byte("wrong block bytes"), nil
			}
		}, corrupted: true},
		{name: "CAS digest corruption", source: func(source *artifactServerTestSource) {
			source.block = artifactServerBlockValue(fixture.blockView())
		}, cas: func(cas *artifactServerTestCAS) {
			cas.read = func(model.Digest, int) ([]byte, error) {
				return nil, artifactdomain.ErrCASCorruption
			}
		}, corrupted: true},
	}
	for _, test := range blockTests {
		test := test
		t.Run("block "+test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			origin := testkit.NewIdentity(t, "artifact-server-block-corrupt-origin-"+test.name)
			requester := testkit.NewIdentity(t, "artifact-server-block-corrupt-requester-"+test.name)
			originHost := newArtifactServerTestHost(t, origin)
			defer originHost.Close()
			requesterHost := newArtifactServerTestHost(t, requester)
			defer requesterHost.Close()
			connectArtifactServerHosts(t, ctx, requesterHost, originHost)
			source := &artifactServerTestSource{}
			test.source(source)
			cas := fixture.cas()
			if test.cas != nil {
				test.cas(cas)
			}
			server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
				Source: source, CAS: cas})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			request, _ := NewGetBlock(GetBlockSpec{ChannelID: fixture.channelID,
				RootDigest: fixture.rootDigest, BlockDigest: fixture.blockDigest})
			if test.corrupted {
				assertArtifactCorruptResponse(t,
					exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request))
			} else {
				assertArtifactServerReset(t, ctx, requesterHost, originHost.ID(), request)
			}
		})
	}
}

func TestArtifactServerResetsMalformedOversizedAndResponseFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-malformed-origin")
	requester := testkit.NewIdentity(t, "artifact-server-malformed-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	fixture := newArtifactServerContent(t, "malformed", []byte("malformed block"))
	source := artifactServerStaticSource(fixture)
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	stream := openArtifactServerTestStream(t, ctx, requesterHost, originHost.ID())
	var prefix [artifactFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(artifactSmallFrameBytes+1))
	if _, err := stream.Write(prefix[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadArtifactFrame(stream); err == nil {
		t.Fatal("oversized first Artifact frame received a response")
	}
	_ = stream.Close()

	malformed := openArtifactServerTestStream(t, ctx, requesterHost, originHost.ID())
	raw := []byte(`{"payload":{},"type":"future","version":1}`)
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	_, _ = malformed.Write(prefix[:])
	_, _ = malformed.Write(raw)
	if _, err := ReadArtifactFrame(malformed); err == nil {
		t.Fatal("malformed Artifact frame received a response")
	}
	_ = malformed.Close()

	ack, _ := NewArtifactAck()
	manifest, _ := NewManifest(ManifestSpec{RootDigest: fixture.rootDigest,
		Manifest: fixture.manifest.CanonicalJSON()})
	block, _ := NewBlock(BlockSpec{BlockDigest: fixture.blockDigest, BlockBytes: fixture.blockBytes})
	denied, _ := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorNotAuthorized})
	for _, payload := range []ArtifactFramePayload{ack, manifest, block, denied} {
		assertArtifactServerReset(t, ctx, requesterHost, originHost.ID(), payload)
	}
	if source.manifestCalls.Load() != 0 || source.blockCalls.Load() != 0 {
		t.Fatal("invalid first Artifact frames reached the source Store")
	}
}

func TestArtifactServerClosesStreamAfterOneRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-single-origin")
	requester := testkit.NewIdentity(t, "artifact-server-single-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	fixture := newArtifactServerContent(t, "single", []byte("single block"))
	source := artifactServerStaticSource(fixture)
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	frame, _ := NewArtifactFrame(request)
	stream := openArtifactServerTestStream(t, ctx, requesterHost, originHost.ID())
	defer stream.Close()
	if err := WriteArtifactFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	_ = WriteArtifactFrame(stream, frame)
	if response, err := ReadArtifactFrame(stream); err != nil || response.Type() != ArtifactFrameManifest {
		t.Fatalf("first response = (%#v, %v)", response, err)
	}
	if _, err := ReadArtifactFrame(stream); err == nil {
		t.Fatal("second request on one stream received a second response")
	}
	if source.manifestCalls.Load() != 1 {
		t.Fatalf("one stream source calls = %d", source.manifestCalls.Load())
	}
}

func TestArtifactServerRejectsNonEd25519RequesterBeforeSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-identity-origin")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterKey, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}
	requesterHost, err := libp2p.New(libp2p.Identity(requesterKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	fixture := newArtifactServerContent(t, "identity", []byte("identity block"))
	source := artifactServerStaticSource(fixture)
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	frame, _ := NewArtifactFrame(request)
	stream, streamErr := requesterHost.NewStream(ctx, originHost.ID(), ArtifactsProtocol)
	if streamErr == nil {
		defer stream.Close()
		_ = stream.SetDeadline(time.Now().Add(3 * time.Second))
		if err := WriteArtifactFrame(stream, frame); err == nil {
			if _, err := ReadArtifactFrame(stream); err == nil {
				t.Fatal("non-Ed25519 requester received an Artifact response")
			}
		}
	}
	if source.manifestCalls.Load() != 0 || source.blockCalls.Load() != 0 {
		t.Fatal("non-Ed25519 requester reached the source Store")
	}
}

func TestArtifactServerEnforcesPerPeerPullLimit(t *testing.T) {
	ctx := context.Background()
	origin := testkit.NewIdentity(t, "artifact-server-peer-limit-origin")
	requester := testkit.NewIdentity(t, "artifact-server-peer-limit-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	fixture := newArtifactServerContent(t, "peer-limit", []byte("peer limit block"))
	started := make(chan struct{}, HermeticLimits().PeerArtifactPulls)
	source := &artifactServerTestSource{}
	source.manifest = func(ctx context.Context,
		_ store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	frame, _ := NewArtifactFrame(request)
	streams := make([]network.Stream, 0, HermeticLimits().PeerArtifactPulls)
	for index := 0; index < HermeticLimits().PeerArtifactPulls; index++ {
		stream := openArtifactServerTestStream(t, ctx, requesterHost, originHost.ID())
		streams = append(streams, stream)
		if err := WriteArtifactFrame(stream, frame); err != nil {
			t.Fatal(err)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("per-Peer request %d did not start", index+1)
		}
	}
	overloaded := openArtifactServerTestStream(t, ctx, requesterHost, originHost.ID())
	response, err := ReadArtifactFrame(overloaded)
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactBusyResponse(t, response)
	_ = overloaded.Close()

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Artifact Close did not cancel and drain per-Peer work")
	}
	for _, stream := range streams {
		_ = stream.Close()
	}
	if countArtifactProtocol(originHost.Mux().Protocols(), ArtifactsProtocol) != 0 {
		t.Fatal("Artifact Close retained its protocol handler")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactServerEnforcesNodePullLimitAcrossPeers(t *testing.T) {
	ctx := context.Background()
	origin := testkit.NewIdentity(t, "artifact-server-node-limit-origin")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	fixture := newArtifactServerContent(t, "node-limit", []byte("node limit block"))
	started := make(chan struct{}, HermeticLimits().NodeArtifactPulls)
	source := &artifactServerTestSource{}
	source.manifest = func(ctx context.Context,
		_ store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: fixture.cas()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	frame, _ := NewArtifactFrame(request)
	peerCount := HermeticLimits().NodeArtifactPulls / HermeticLimits().PeerArtifactPulls
	requesters := make([]host.Host, 0, peerCount+1)
	streams := make([]network.Stream, 0, HermeticLimits().NodeArtifactPulls)
	for peerIndex := 0; peerIndex < peerCount; peerIndex++ {
		identity := testkit.NewIdentity(t, fmt.Sprintf("artifact-server-node-peer-%d", peerIndex))
		requester := newArtifactServerTestHost(t, identity)
		defer requester.Close()
		requesters = append(requesters, requester)
		connectArtifactServerHosts(t, ctx, requester, originHost)
		for streamIndex := 0; streamIndex < HermeticLimits().PeerArtifactPulls; streamIndex++ {
			stream := openArtifactServerTestStream(t, ctx, requester, originHost.ID())
			streams = append(streams, stream)
			if err := WriteArtifactFrame(stream, frame); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatalf("Node request %d did not start", len(streams))
			}
		}
	}
	overflowIdentity := testkit.NewIdentity(t, "artifact-server-node-overflow")
	overflowHost := newArtifactServerTestHost(t, overflowIdentity)
	defer overflowHost.Close()
	requesters = append(requesters, overflowHost)
	connectArtifactServerHosts(t, ctx, overflowHost, originHost)
	overloaded := openArtifactServerTestStream(t, ctx, overflowHost, originHost.ID())
	response, err := ReadArtifactFrame(overloaded)
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactBusyResponse(t, response)
	_ = overloaded.Close()

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Artifact Close did not cancel and drain Node work")
	}
	for _, stream := range streams {
		_ = stream.Close()
	}
	for _, requester := range requesters {
		_ = requester.Close()
	}
}

func TestArtifactServerResponseLossRepeatsReadOnlySnapshotAndCASVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "artifact-server-loss-origin")
	requester := testkit.NewIdentity(t, "artifact-server-loss-requester")
	originHost := newArtifactServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newArtifactServerTestHost(t, requester)
	defer requesterHost.Close()
	connectArtifactServerHosts(t, ctx, requesterHost, originHost)
	fixture := newArtifactServerContent(t, "loss", []byte("response loss block"))
	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	source := &artifactServerTestSource{}
	source.manifest = func(_ context.Context,
		_ store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error) {
		if source.manifestCalls.Load() == 1 {
			firstStarted <- struct{}{}
			<-releaseFirst
		}
		return fixture.manifestView(), nil
	}
	cas := fixture.cas()
	server, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: originHost,
		Source: source, CAS: cas})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	frame, _ := NewArtifactFrame(request)
	first := openArtifactServerTestStream(t, ctx, requesterHost, originHost.ID())
	if err := WriteArtifactFrame(first, frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("response-loss Store snapshot did not start")
	}
	_ = first.Reset()
	close(releaseFirst)

	response := exchangeArtifactServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	if response.Type() != ArtifactFrameManifest || source.manifestCalls.Load() != 2 {
		t.Fatalf("response-loss retry = type %s, calls %d",
			response.Type(), source.manifestCalls.Load())
	}
	deadline := time.After(2 * time.Second)
	for len(cas.readsSnapshot()) < 2 {
		select {
		case <-deadline:
			t.Fatalf("response-loss CAS reads = %d", len(cas.readsSnapshot()))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestArtifactServerOutputScopeReservationReleasesOnSuccessAndFailure(t *testing.T) {
	t.Parallel()

	denied, _ := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorNotAuthorized})
	frame, _ := NewArtifactFrame(denied)
	scope := &artifactServerTestScope{}
	var encoded bytes.Buffer
	if err := writeArtifactFrameWithScope(&encoded, scope, frame); err != nil {
		t.Fatal(err)
	}
	want := len(frame.CanonicalJSON().Bytes())
	if scope.reserved != want || scope.released != want ||
		scope.priority != network.ReservationPriorityAlways {
		t.Fatalf("output scope = %#v, want %d", scope, want)
	}
	if parsed, err := ReadArtifactFrame(&encoded); err != nil ||
		parsed.Type() != ArtifactFrameProtocolError {
		t.Fatalf("reserved output = (%#v, %v)", parsed, err)
	}

	reserveError := fmt.Errorf("test output: %w", network.ErrResourceLimitExceeded)
	rejected := &artifactServerTestScope{reserveError: reserveError}
	encoded.Reset()
	if err := writeArtifactFrameWithScope(&encoded, rejected, frame); !errors.Is(err, ErrArtifactServer) || !errors.Is(err, errArtifactServerBudget) ||
		!errors.Is(err, network.ErrResourceLimitExceeded) || encoded.Len() != 0 || rejected.released != 0 {
		t.Fatalf("output reservation rejection = (%d, %#v, %v)", encoded.Len(), rejected, err)
	}

	failing := &artifactServerFailWriter{err: io.ErrClosedPipe}
	failureScope := &artifactServerTestScope{}
	if err := writeArtifactFrameWithScope(failing, failureScope, frame); !errors.Is(err, ErrArtifactFrame) || !errors.Is(err, io.ErrClosedPipe) ||
		failureScope.released != failureScope.reserved {
		t.Fatalf("output write failure = (scope %#v, %v)", failureScope, err)
	}
}

type artifactServerManifestView struct {
	rootDigest     model.Digest
	manifestDigest model.Digest
	manifestBytes  []byte
	totalBytes     uint64
}

func (view artifactServerManifestView) RootDigest() model.Digest     { return view.rootDigest }
func (view artifactServerManifestView) ManifestDigest() model.Digest { return view.manifestDigest }
func (view artifactServerManifestView) ManifestBytes() []byte {
	return append([]byte(nil), view.manifestBytes...)
}
func (view artifactServerManifestView) TotalBytes() uint64 { return view.totalBytes }

type artifactServerBlockView struct {
	rootDigest  model.Digest
	blockDigest model.Digest
	sizeBytes   uint64
}

func (view artifactServerBlockView) RootDigest() model.Digest  { return view.rootDigest }
func (view artifactServerBlockView) BlockDigest() model.Digest { return view.blockDigest }
func (view artifactServerBlockView) SizeBytes() uint64         { return view.sizeBytes }

type artifactServerTestSource struct {
	manifest func(context.Context,
		store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error)
	block func(context.Context,
		store.ReadArtifactSourceBlockSpec,
	) (ArtifactSourceBlock, error)

	mu            sync.Mutex
	manifestSpec  store.ReadArtifactSourceManifestSpec
	blockSpec     store.ReadArtifactSourceBlockSpec
	manifestCalls atomic.Int32
	blockCalls    atomic.Int32
}

func (source *artifactServerTestSource) ReadArtifactSourceManifest(ctx context.Context,
	spec store.ReadArtifactSourceManifestSpec,
) (ArtifactSourceManifest, error) {
	source.mu.Lock()
	source.manifestSpec = spec
	source.mu.Unlock()
	source.manifestCalls.Add(1)
	if source.manifest == nil {
		return nil, errors.New("unexpected Manifest source call")
	}
	return source.manifest(ctx, spec)
}

func (source *artifactServerTestSource) ReadArtifactSourceBlock(ctx context.Context,
	spec store.ReadArtifactSourceBlockSpec,
) (ArtifactSourceBlock, error) {
	source.mu.Lock()
	source.blockSpec = spec
	source.mu.Unlock()
	source.blockCalls.Add(1)
	if source.block == nil {
		return nil, errors.New("unexpected Block source call")
	}
	return source.block(ctx, spec)
}

func (source *artifactServerTestSource) lastManifestSpec() store.ReadArtifactSourceManifestSpec {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.manifestSpec
}

func (source *artifactServerTestSource) lastBlockSpec() store.ReadArtifactSourceBlockSpec {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.blockSpec
}

func artifactServerManifestFailure(cause error) func(context.Context,
	store.ReadArtifactSourceManifestSpec,
) (ArtifactSourceManifest, error) {
	return func(context.Context, store.ReadArtifactSourceManifestSpec) (ArtifactSourceManifest, error) {
		return nil, cause
	}
}

func artifactServerManifestValue(view artifactServerManifestView) func(context.Context,
	store.ReadArtifactSourceManifestSpec,
) (ArtifactSourceManifest, error) {
	return func(context.Context, store.ReadArtifactSourceManifestSpec) (ArtifactSourceManifest, error) {
		return view, nil
	}
}

func artifactServerBlockFailure(cause error) func(context.Context,
	store.ReadArtifactSourceBlockSpec,
) (ArtifactSourceBlock, error) {
	return func(context.Context, store.ReadArtifactSourceBlockSpec) (ArtifactSourceBlock, error) {
		return nil, cause
	}
}

func artifactServerBlockValue(view artifactServerBlockView) func(context.Context,
	store.ReadArtifactSourceBlockSpec,
) (ArtifactSourceBlock, error) {
	return func(context.Context, store.ReadArtifactSourceBlockSpec) (ArtifactSourceBlock, error) {
		return view, nil
	}
}

type artifactServerCASRead struct {
	digest  model.Digest
	maximum int
}

type artifactServerTestCAS struct {
	read    func(model.Digest, int) ([]byte, error)
	objects map[model.Digest][]byte

	mu    sync.Mutex
	reads []artifactServerCASRead
}

func (cas *artifactServerTestCAS) Read(digest model.Digest, maximum int) ([]byte, error) {
	cas.mu.Lock()
	cas.reads = append(cas.reads, artifactServerCASRead{digest: digest, maximum: maximum})
	read := cas.read
	value, present := cas.objects[digest]
	cas.mu.Unlock()
	if read != nil {
		return read(digest, maximum)
	}
	if !present {
		return nil, errors.New("CAS object unavailable")
	}
	return append([]byte(nil), value...), nil
}

func (cas *artifactServerTestCAS) readsSnapshot() []artifactServerCASRead {
	cas.mu.Lock()
	defer cas.mu.Unlock()
	return append([]artifactServerCASRead(nil), cas.reads...)
}

type artifactServerContent struct {
	channelID   model.ChannelID
	manifest    artifactdomain.Manifest
	rootDigest  model.Digest
	blockDigest model.Digest
	blockBytes  []byte
}

func newArtifactServerContent(t testing.TB, name string, blockBytes []byte) artifactServerContent {
	t.Helper()
	channelID, err := model.ParseChannelID("channel-artifact-server-" + name)
	if err != nil {
		t.Fatal(err)
	}
	blockDigest := model.Sum(blockBytes)
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: name + ".txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: name + ".txt", Mode: 0o600, SizeBytes: uint64(len(blockBytes)),
			Blocks: []artifactdomain.ManifestBlock{{Digest: blockDigest,
				LengthBytes: uint64(len(blockBytes))}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifactServerContent{channelID: channelID, manifest: manifest,
		rootDigest: manifest.RootDigest(), blockDigest: blockDigest,
		blockBytes: append([]byte(nil), blockBytes...)}
}

func (fixture artifactServerContent) manifestView() artifactServerManifestView {
	return artifactServerManifestView{rootDigest: fixture.rootDigest,
		manifestDigest: fixture.manifest.ManifestDigest(),
		manifestBytes:  fixture.manifest.CanonicalJSON().Bytes(),
		totalBytes:     fixture.manifest.TotalBytes()}
}

func (fixture artifactServerContent) blockView() artifactServerBlockView {
	return artifactServerBlockView{rootDigest: fixture.rootDigest,
		blockDigest: fixture.blockDigest, sizeBytes: uint64(len(fixture.blockBytes))}
}

func (fixture artifactServerContent) cas() *artifactServerTestCAS {
	return &artifactServerTestCAS{objects: map[model.Digest][]byte{
		fixture.manifest.ManifestDigest(): fixture.manifest.CanonicalJSON().Bytes(),
		fixture.blockDigest:               fixture.blockBytes,
	}}
}

func artifactServerStaticSource(fixture artifactServerContent) *artifactServerTestSource {
	source := &artifactServerTestSource{}
	source.manifest = artifactServerManifestValue(fixture.manifestView())
	source.block = func(context.Context,
		store.ReadArtifactSourceBlockSpec,
	) (ArtifactSourceBlock, error) {
		return fixture.blockView(), nil
	}
	return source
}

type artifactServerExactStore struct {
	manifestErr error
	blockErr    error

	manifestCalls atomic.Int32
	blockCalls    atomic.Int32
	manifestSpec  store.ReadArtifactSourceManifestSpec
	blockSpec     store.ReadArtifactSourceBlockSpec
}

func (source *artifactServerExactStore) ReadArtifactSourceManifest(_ context.Context,
	spec store.ReadArtifactSourceManifestSpec,
) (store.ArtifactSourceManifest, error) {
	source.manifestSpec = spec
	source.manifestCalls.Add(1)
	return store.ArtifactSourceManifest{}, source.manifestErr
}

func (source *artifactServerExactStore) ReadArtifactSourceBlock(_ context.Context,
	spec store.ReadArtifactSourceBlockSpec,
) (store.ArtifactSourceBlock, error) {
	source.blockSpec = spec
	source.blockCalls.Add(1)
	return store.ArtifactSourceBlock{}, source.blockErr
}

func newArtifactServerTestHost(t testing.TB, identity testkit.Identity) host.Host {
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

func connectArtifactServerHosts(t testing.TB, ctx context.Context, remote, origin host.Host) {
	t.Helper()
	if err := remote.Connect(ctx, libp2ppeer.AddrInfo{ID: origin.ID(), Addrs: origin.Addrs()}); err != nil {
		t.Fatal(err)
	}
}

func openArtifactServerTestStream(t testing.TB, ctx context.Context, remote host.Host,
	originID libp2ppeer.ID,
) network.Stream {
	t.Helper()
	stream, err := remote.NewStream(ctx, originID, ArtifactsProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return stream
}

func exchangeArtifactServerPayload(t testing.TB, ctx context.Context, remote host.Host,
	originID libp2ppeer.ID, payload ArtifactFramePayload,
) ArtifactFrame {
	t.Helper()
	stream := openArtifactServerTestStream(t, ctx, remote, originID)
	defer stream.Close()
	frame, err := NewArtifactFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteArtifactFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadArtifactFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertArtifactServerReset(t testing.TB, ctx context.Context, remote host.Host,
	originID libp2ppeer.ID, payload ArtifactFramePayload,
) {
	t.Helper()
	stream := openArtifactServerTestStream(t, ctx, remote, originID)
	defer stream.Close()
	frame, err := NewArtifactFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteArtifactFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadArtifactFrame(stream); err == nil {
		t.Fatal("unsafe Artifact failure received a response instead of reset")
	}
}

func assertArtifactBusyResponse(t testing.TB, response ArtifactFrame) {
	t.Helper()
	failure, ok := response.Payload().(ArtifactProtocolError)
	if response.Type() != ArtifactFrameProtocolError || !ok || failure.Code() != ArtifactErrorBusy ||
		!failure.Retryable() || failure.RetryAfter() != artifactServerBusyRetry {
		t.Fatalf("Artifact overload response = %#v", response)
	}
}

func assertArtifactCorruptResponse(t testing.TB, response ArtifactFrame) {
	t.Helper()
	failure, ok := response.Payload().(ArtifactProtocolError)
	if response.Type() != ArtifactFrameProtocolError || !ok ||
		failure.Code() != ArtifactErrorCorrupt || failure.Retryable() || failure.RetryAfter() != 0 {
		t.Fatalf("Artifact corruption response = %#v", response)
	}
}

func countArtifactProtocol(protocols []protocol.ID, want protocol.ID) int {
	count := 0
	for _, protocolID := range protocols {
		if protocolID == want {
			count++
		}
	}
	return count
}

type artifactServerTestScope struct {
	reserved     int
	released     int
	priority     uint8
	reserveError error
}

func (scope *artifactServerTestScope) ReserveMemory(size int, priority uint8) error {
	scope.priority = priority
	if scope.reserveError != nil {
		return scope.reserveError
	}
	scope.reserved += size
	return nil
}

func (scope *artifactServerTestScope) ReleaseMemory(size int) { scope.released += size }
func (scope *artifactServerTestScope) Stat() network.ScopeStat {
	return network.ScopeStat{Memory: int64(scope.reserved - scope.released)}
}
func (scope *artifactServerTestScope) BeginSpan() (network.ResourceScopeSpan, error) {
	return scope, nil
}
func (scope *artifactServerTestScope) Done() {}

type artifactServerFailWriter struct{ err error }

func (writer *artifactServerFailWriter) Write([]byte) (int, error) { return 0, writer.err }

var _ ArtifactServerSource = (*artifactServerTestSource)(nil)
var _ ArtifactStoreSource = (*artifactServerExactStore)(nil)
var _ ArtifactCAS = (*artifactServerTestCAS)(nil)
var _ network.ResourceScopeSpan = (*artifactServerTestScope)(nil)
