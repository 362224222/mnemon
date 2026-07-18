package peer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactClientGetsVerifiedManifestAndBlockOnSingleRequestStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originIdentity := testkit.NewIdentity(t, "artifact-client-success-origin")
	requesterIdentity := testkit.NewIdentity(t, "artifact-client-success-requester")
	origin := newArtifactClientTestHost(t, originIdentity)
	defer origin.Close()
	requester := newArtifactClientTestHost(t, requesterIdentity)
	defer requester.Close()
	connectArtifactClientHosts(t, ctx, requester, origin)
	fixture := newArtifactClientFixture(t, "success", []byte("verified artifact client block"))
	manifestResponse := fixture.manifestPayload(t)
	blockResponse := fixture.blockPayload(t)
	requests := make(chan ArtifactFrame, 2)
	singleRequest := make(chan bool, 2)
	origin.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
		defer stream.Close()
		request, err := ReadArtifactFrame(stream)
		if err != nil {
			_ = stream.Reset()
			return
		}
		requests <- request
		_, secondErr := ReadArtifactFrame(stream)
		singleRequest <- secondErr != nil
		var payload ArtifactFramePayload
		switch request.Type() {
		case ArtifactFrameGetManifest:
			payload = manifestResponse
		case ArtifactFrameGetBlock:
			payload = blockResponse
		default:
			_ = stream.Reset()
			return
		}
		frame, _ := NewArtifactFrame(payload)
		_ = WriteArtifactFrame(stream, frame)
	})
	client, err := NewArtifactClient(ArtifactClientOptions{Host: requester})
	if err != nil {
		t.Fatal(err)
	}
	manifestRequest, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	manifest, err := client.GetManifest(ctx, originIdentity.PeerID(), manifestRequest)
	if err != nil || manifest.RootDigest() != fixture.rootDigest ||
		manifest.ManifestDigest() != fixture.manifest.ManifestDigest() ||
		!bytes.Equal(manifest.ManifestBytes(), fixture.manifest.CanonicalJSON().Bytes()) {
		t.Fatalf("GetManifest() = (%#v, %v)", manifest, err)
	}
	blockRequest, _ := NewGetBlock(GetBlockSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest, BlockDigest: fixture.blockDigest})
	block, err := client.GetBlock(ctx, originIdentity.PeerID(), blockRequest)
	if err != nil || block.BlockDigest() != fixture.blockDigest ||
		!bytes.Equal(block.BlockBytes(), fixture.blockBytes) {
		t.Fatalf("GetBlock() = (%#v, %v)", block, err)
	}

	first := <-requests
	firstManifest, ok := first.Payload().(GetManifest)
	if first.Type() != ArtifactFrameGetManifest || !ok ||
		firstManifest.ChannelID() != fixture.channelID ||
		firstManifest.RootDigest() != fixture.rootDigest {
		t.Fatalf("first request = %#v", first)
	}
	second := <-requests
	secondBlock, ok := second.Payload().(GetBlock)
	if second.Type() != ArtifactFrameGetBlock || !ok ||
		secondBlock.ChannelID() != fixture.channelID ||
		secondBlock.RootDigest() != fixture.rootDigest ||
		secondBlock.BlockDigest() != fixture.blockDigest {
		t.Fatalf("second request = %#v", second)
	}
	if !<-singleRequest || !<-singleRequest {
		t.Fatal("client did not half-close each one-request stream")
	}
	manifestCopy := manifest.ManifestBytes()
	manifestCopy[0] ^= 0xff
	blockCopy := block.BlockBytes()
	blockCopy[0] ^= 0xff
	if bytes.Equal(manifestCopy, manifest.ManifestBytes()) ||
		bytes.Equal(blockCopy, block.BlockBytes()) {
		t.Fatal("Artifact client response bytes were mutable aliases")
	}
}

func TestArtifactClientReturnsOnlyClosedRemoteFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originIdentity := testkit.NewIdentity(t, "artifact-client-remote-origin")
	requesterIdentity := testkit.NewIdentity(t, "artifact-client-remote-requester")
	origin := newArtifactClientTestHost(t, originIdentity)
	defer origin.Close()
	requester := newArtifactClientTestHost(t, requesterIdentity)
	defer requester.Close()
	connectArtifactClientHosts(t, ctx, requester, origin)
	fixture := newArtifactClientFixture(t, "remote", []byte("remote failure block"))
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	client, _ := NewArtifactClient(ArtifactClientOptions{Host: requester})
	tests := []struct {
		name       string
		payload    ArtifactProtocolError
		code       ArtifactProtocolErrorCode
		retryable  bool
		retryAfter time.Duration
	}{
		{name: "not authorized", payload: mustArtifactClientProtocolError(t,
			ArtifactProtocolErrorSpec{Code: ArtifactErrorNotAuthorized}),
			code: ArtifactErrorNotAuthorized},
		{name: "busy", payload: mustArtifactClientProtocolError(t,
			ArtifactProtocolErrorSpec{Code: ArtifactErrorBusy, Retryable: true,
				RetryAfter: 1250 * time.Millisecond}), code: ArtifactErrorBusy,
			retryable: true, retryAfter: 1250 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			installArtifactClientPayloadHandler(origin, test.payload)
			_, err := client.GetManifest(ctx, originIdentity.PeerID(), request)
			var failure *ArtifactRemoteFailure
			if !errors.Is(err, ErrArtifactClient) ||
				errors.Is(err, ErrArtifactClientTransport) ||
				errors.Is(err, ErrArtifactClientResponse) || !errors.As(err, &failure) ||
				failure.Code() != test.code || failure.Retryable() != test.retryable ||
				failure.RetryAfter() != test.retryAfter ||
				strings.Contains(failure.Error(), "implementation") {
				t.Fatalf("closed remote failure = %#v (%v)", failure, err)
			}
		})
	}
	var nilFailure *ArtifactRemoteFailure
	if nilFailure.Code() != "" || nilFailure.Retryable() || nilFailure.RetryAfter() != 0 ||
		nilFailure.Error() != ErrArtifactClient.Error() {
		t.Fatalf("nil remote failure API is not closed")
	}
}

func TestArtifactClientFencesIdentityInputAndTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	identity := testkit.NewIdentity(t, "artifact-client-input")
	local := newArtifactClientTestHost(t, identity)
	defer local.Close()
	client, err := NewArtifactClient(ArtifactClientOptions{Host: local})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newArtifactClientFixture(t, "input", []byte("input block"))
	request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	if _, err := client.GetManifest(ctx, identity.PeerID(), request); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("self source error = %v", err)
	}
	if _, err := client.GetManifest(nil, identity.PeerID(), request); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := client.GetManifest(ctx, model.PeerID{}, request); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("zero source error = %v", err)
	}
	if _, err := client.GetManifest(ctx, identity.PeerID(), GetManifest{}); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("zero request error = %v", err)
	}
	var nilClient *ArtifactClient
	if _, err := nilClient.GetManifest(ctx, identity.PeerID(), request); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("nil client error = %v", err)
	}
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	unreachable := testkit.NewIdentity(t, "artifact-client-unreachable")
	if _, err := client.GetManifest(cancelled, unreachable.PeerID(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled exchange error = %v", err)
	}
	if _, err := client.GetManifest(ctx, unreachable.PeerID(), request); !errors.Is(err, ErrArtifactClientTransport) {
		t.Fatalf("unreachable exact source error = %v", err)
	}
	if _, err := NewArtifactClient(ArtifactClientOptions{}); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("nil Host constructor error = %v", err)
	}

	rsaLocal := newArtifactClientRSAHost(t)
	defer rsaLocal.Close()
	if _, err := NewArtifactClient(ArtifactClientOptions{Host: rsaLocal}); !errors.Is(err, ErrArtifactClient) {
		t.Fatalf("RSA local identity error = %v", err)
	}
	rsaRemote := newArtifactClientRSAHost(t)
	defer rsaRemote.Close()
	connectArtifactClientHosts(t, ctx, local, rsaRemote)
	rsaRemote.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, _ = io.Copy(io.Discard, stream)
	})
	rsaPeer, parseErr := model.ParsePeerID(rsaRemote.ID().String())
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if _, err := client.GetManifest(ctx, rsaPeer, request); !errors.Is(err, ErrArtifactClientResponse) {
		t.Fatalf("RSA remote identity error = %v", err)
	}

	wrongIdentity := testkit.NewIdentity(t, "artifact-client-misdirected-remote")
	wrongRemote := newArtifactClientTestHost(t, wrongIdentity)
	defer wrongRemote.Close()
	connectArtifactClientHosts(t, ctx, local, wrongRemote)
	wrongRemote.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, _ = io.Copy(io.Discard, stream)
	})
	misdirected, err := NewArtifactClient(ArtifactClientOptions{Host: artifactClientMisdirectHost{
		Host: local, target: wrongRemote.ID()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := misdirected.GetManifest(ctx, unreachable.PeerID(), request); !errors.Is(err, ErrArtifactClientResponse) {
		t.Fatalf("wrong secure remote PeerID error = %v", err)
	}
}

func TestArtifactClientRejectsMaliciousResponses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originIdentity := testkit.NewIdentity(t, "artifact-client-malicious-origin")
	requesterIdentity := testkit.NewIdentity(t, "artifact-client-malicious-requester")
	origin := newArtifactClientTestHost(t, originIdentity)
	defer origin.Close()
	requester := newArtifactClientTestHost(t, requesterIdentity)
	defer requester.Close()
	connectArtifactClientHosts(t, ctx, requester, origin)
	client, _ := NewArtifactClient(ArtifactClientOptions{Host: requester})
	fixture := newArtifactClientFixture(t, "malicious", []byte("expected malicious block"))
	other := newArtifactClientFixture(t, "other", []byte("different block"))
	manifestRequest, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest})
	blockRequest, _ := NewGetBlock(GetBlockSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest, BlockDigest: fixture.blockDigest})
	ack, _ := NewArtifactAck()
	ackFrame, _ := NewArtifactFrame(ack)
	requestFrame, _ := NewArtifactFrame(manifestRequest)
	wrongManifestFrame, _ := NewArtifactFrame(other.manifestPayload(t))
	wrongBlockFrame, _ := NewArtifactFrame(other.blockPayload(t))
	validManifestFrame, _ := NewArtifactFrame(fixture.manifestPayload(t))
	corruptBlock := []byte(fmt.Sprintf(
		`{"payload":{"block_bytes":"%s","block_digest":"%s"},"type":"block","version":1}`,
		base64.StdEncoding.EncodeToString([]byte("corrupt")), fixture.blockDigest))
	corruptManifest := []byte(fmt.Sprintf(
		`{"payload":{"manifest_bytes":"%s","manifest_digest":"%s","root_digest":"%s"},"type":"manifest","version":1}`,
		base64.StdEncoding.EncodeToString(fixture.manifest.CanonicalJSON().Bytes()),
		fixture.blockDigest, fixture.rootDigest))
	tests := []struct {
		name  string
		block bool
		write func(network.Stream)
	}{
		{name: "wrong response type", write: writeArtifactClientFrameResponse(ackFrame)},
		{name: "request returned as response", write: writeArtifactClientFrameResponse(requestFrame)},
		{name: "wrong manifest root tuple", write: writeArtifactClientFrameResponse(wrongManifestFrame)},
		{name: "wrong block digest tuple", block: true,
			write: writeArtifactClientFrameResponse(wrongBlockFrame)},
		{name: "unknown frame", write: writeArtifactClientRawResponse(
			[]byte(`{"payload":{},"type":"future","version":1}`))},
		{name: "noncanonical envelope", write: writeArtifactClientRawResponse(
			append([]byte(" "), validManifestFrame.CanonicalJSON().Bytes()...))},
		{name: "corrupt block digest", block: true, write: writeArtifactClientRawResponse(corruptBlock)},
		{name: "corrupt manifest digest", write: writeArtifactClientRawResponse(corruptManifest)},
		{name: "oversized direct frame", write: func(stream network.Stream) {
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], uint32(maxArtifactFrameBytes()+1))
			_, _ = stream.Write(prefix[:])
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			origin.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
				defer stream.Close()
				if _, err := ReadArtifactFrame(stream); err != nil {
					_ = stream.Reset()
					return
				}
				test.write(stream)
			})
			var err error
			if test.block {
				_, err = client.GetBlock(ctx, originIdentity.PeerID(), blockRequest)
			} else {
				_, err = client.GetManifest(ctx, originIdentity.PeerID(), manifestRequest)
			}
			if !errors.Is(err, ErrArtifactClientResponse) ||
				errors.Is(err, ErrArtifactClientTransport) {
				t.Fatalf("malicious response error = %v", err)
			}
		})
	}
}

func TestArtifactClientHonorsCancellationTimeoutAndDoesNotRetryLoss(t *testing.T) {
	t.Run("response loss", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		originIdentity, origin, requester := newArtifactClientHostPair(t, ctx, "loss")
		var streams atomic.Int32
		origin.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
			streams.Add(1)
			_, _ = ReadArtifactFrame(stream)
			_ = stream.Reset()
		})
		client, _ := NewArtifactClient(ArtifactClientOptions{Host: requester})
		fixture := newArtifactClientFixture(t, "loss", []byte("lost response"))
		request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
			RootDigest: fixture.rootDigest})
		if _, err := client.GetManifest(ctx, originIdentity.PeerID(), request); !errors.Is(err, ErrArtifactClientTransport) {
			t.Fatalf("response loss error = %v", err)
		}
		if streams.Load() != 1 {
			t.Fatalf("client retried response loss on %d streams", streams.Load())
		}
	})
	for _, test := range []struct {
		name       string
		cancelCall bool
	}{
		{name: "caller deadline"},
		{name: "caller cancellation", cancelCall: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, baseCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer baseCancel()
			originIdentity, origin, requester := newArtifactClientHostPair(t, base, test.name)
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			origin.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
				defer stream.Close()
				_, _ = ReadArtifactFrame(stream)
				started <- struct{}{}
				<-release
			})
			client, _ := NewArtifactClient(ArtifactClientOptions{Host: requester})
			fixture := newArtifactClientFixture(t, test.name, []byte("blocked response"))
			request, _ := NewGetManifest(GetManifestSpec{ChannelID: fixture.channelID,
				RootDigest: fixture.rootDigest})
			callContext, callCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			if test.cancelCall {
				callCancel()
				callContext, callCancel = context.WithCancel(context.Background())
				go func() {
					<-started
					callCancel()
				}()
			}
			_, err := client.GetManifest(callContext, originIdentity.PeerID(), request)
			callCancel()
			close(release)
			if test.cancelCall && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation error = %v", err)
			}
			if !test.cancelCall && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline error = %v", err)
			}
		})
	}
}

func TestArtifactClientEnforcesRequestResponseMemoryBudgets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originIdentity := testkit.NewIdentity(t, "artifact-client-budget-origin")
	origin := newArtifactClientTestHost(t, originIdentity)
	defer origin.Close()
	fixture := newArtifactClientFixture(t, "budget", bytes.Repeat([]byte("b"), 4096))
	blockRequest, _ := NewGetBlock(GetBlockSpec{ChannelID: fixture.channelID,
		RootDigest: fixture.rootDigest, BlockDigest: fixture.blockDigest})

	t.Run("request reservation", func(t *testing.T) {
		requesterIdentity := testkit.NewIdentity(t, "artifact-client-request-budget")
		requester := newArtifactClientLimitedHost(t, requesterIdentity, 64)
		defer requester.Close()
		connectArtifactClientHosts(t, ctx, requester, origin)
		origin.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
			defer stream.Close()
			_, _ = io.Copy(io.Discard, stream)
		})
		client, _ := NewArtifactClient(ArtifactClientOptions{Host: requester})
		if _, err := client.GetBlock(ctx, originIdentity.PeerID(), blockRequest); !errors.Is(err, ErrArtifactClientTransport) {
			t.Fatalf("request reservation error = %v", err)
		}
	})
	t.Run("response reservation", func(t *testing.T) {
		requesterIdentity := testkit.NewIdentity(t, "artifact-client-response-budget")
		requester := newArtifactClientLimitedHost(t, requesterIdentity, 2048)
		defer requester.Close()
		connectArtifactClientHosts(t, ctx, requester, origin)
		installArtifactClientPayloadHandler(origin, fixture.blockPayload(t))
		client, _ := NewArtifactClient(ArtifactClientOptions{Host: requester})
		if _, err := client.GetBlock(ctx, originIdentity.PeerID(), blockRequest); !errors.Is(err, ErrArtifactClientTransport) {
			t.Fatalf("response reservation error = %v", err)
		}
	})
}

type artifactClientFixture struct {
	channelID   model.ChannelID
	manifest    artifactdomain.Manifest
	rootDigest  model.Digest
	blockDigest model.Digest
	blockBytes  []byte
}

type artifactClientMisdirectHost struct {
	host.Host
	target libp2ppeer.ID
}

func (nodeHost artifactClientMisdirectHost) NewStream(ctx context.Context, _ libp2ppeer.ID,
	protocolIDs ...protocol.ID,
) (network.Stream, error) {
	return nodeHost.Host.NewStream(ctx, nodeHost.target, protocolIDs...)
}

func newArtifactClientFixture(t testing.TB, name string, blockBytes []byte) artifactClientFixture {
	t.Helper()
	channelID, err := model.ParseChannelID("channel-artifact-client-" + strings.ReplaceAll(name, " ", "-"))
	if err != nil {
		t.Fatal(err)
	}
	blockDigest := model.Sum(blockBytes)
	path := strings.ReplaceAll(name, " ", "-") + ".txt"
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: path,
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: path, Mode: 0o600, SizeBytes: uint64(len(blockBytes)),
			Blocks: []artifactdomain.ManifestBlock{{Digest: blockDigest,
				LengthBytes: uint64(len(blockBytes))}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifactClientFixture{channelID: channelID, manifest: manifest,
		rootDigest: manifest.RootDigest(), blockDigest: blockDigest,
		blockBytes: append([]byte(nil), blockBytes...)}
}

func (fixture artifactClientFixture) manifestPayload(t testing.TB) Manifest {
	t.Helper()
	payload, err := NewManifest(ManifestSpec{RootDigest: fixture.rootDigest,
		Manifest: fixture.manifest.CanonicalJSON()})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func (fixture artifactClientFixture) blockPayload(t testing.TB) Block {
	t.Helper()
	payload, err := NewBlock(BlockSpec{BlockDigest: fixture.blockDigest,
		BlockBytes: fixture.blockBytes})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func newArtifactClientTestHost(t testing.TB, identity testkit.Identity) host.Host {
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

func newArtifactClientLimitedHost(t testing.TB, identity testkit.Identity,
	streamMemory int64,
) host.Host {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	partial := resourceLimitConfig().ToPartialLimitConfig()
	partial.Stream.Memory = rcmgr.LimitVal64(streamMemory)
	manager, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(
		partial.Build(rcmgr.DefaultLimits.AutoScale())))
	if err != nil {
		t.Fatal(err)
	}
	nodeHost, err := libp2p.New(libp2p.Identity(privateKey), libp2p.ResourceManager(manager),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		_ = manager.Close()
		t.Fatal(err)
	}
	return nodeHost
}

func newArtifactClientRSAHost(t testing.TB) host.Host {
	t.Helper()
	privateKey, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.RSA, 2048)
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

func newArtifactClientHostPair(t testing.TB, ctx context.Context,
	name string,
) (testkit.Identity, host.Host, host.Host) {
	t.Helper()
	originIdentity := testkit.NewIdentity(t, "artifact-client-pair-origin-"+name)
	requesterIdentity := testkit.NewIdentity(t, "artifact-client-pair-requester-"+name)
	origin := newArtifactClientTestHost(t, originIdentity)
	requester := newArtifactClientTestHost(t, requesterIdentity)
	t.Cleanup(func() {
		_ = requester.Close()
		_ = origin.Close()
	})
	connectArtifactClientHosts(t, ctx, requester, origin)
	return originIdentity, origin, requester
}

func connectArtifactClientHosts(t testing.TB, ctx context.Context, remote, origin host.Host) {
	t.Helper()
	if err := remote.Connect(ctx, libp2ppeer.AddrInfo{ID: origin.ID(), Addrs: origin.Addrs()}); err != nil {
		t.Fatal(err)
	}
}

func installArtifactClientPayloadHandler(origin host.Host, payload ArtifactFramePayload) {
	origin.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
		defer stream.Close()
		if _, err := ReadArtifactFrame(stream); err != nil {
			_ = stream.Reset()
			return
		}
		frame, err := NewArtifactFrame(payload)
		if err != nil {
			_ = stream.Reset()
			return
		}
		_ = WriteArtifactFrame(stream, frame)
	})
}

func mustArtifactClientProtocolError(t testing.TB,
	spec ArtifactProtocolErrorSpec,
) ArtifactProtocolError {
	t.Helper()
	payload, err := NewArtifactProtocolError(spec)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func writeArtifactClientFrameResponse(frame ArtifactFrame) func(network.Stream) {
	return func(stream network.Stream) { _ = WriteArtifactFrame(stream, frame) }
}

func writeArtifactClientRawResponse(raw []byte) func(network.Stream) {
	copyOfRaw := append([]byte(nil), raw...)
	return func(stream network.Stream) {
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(copyOfRaw)))
		_, _ = stream.Write(prefix[:])
		_, _ = stream.Write(copyOfRaw)
	}
}
