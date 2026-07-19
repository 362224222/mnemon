package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const artifactServerBusyRetry = time.Second

var (
	ErrArtifactServer = errors.New("Mnemon Artifact server")

	errArtifactServerBudget = errors.New("Artifact server resource budget exhausted")

	artifactServerConstructorMu sync.Mutex
)

// ArtifactSourceManifest is the read-only Store capability visible to the
// protocol server. Implementations cannot expose paths, descriptors, or a
// mutable authority handle.
type ArtifactSourceManifest interface {
	RootDigest() model.Digest
	ManifestDigest() model.Digest
	ManifestBytes() []byte
	TotalBytes() uint64
}

// ArtifactSourceBlock is the read-only reachability proof returned from one
// exact Channel/root/block Store snapshot.
type ArtifactSourceBlock interface {
	RootDigest() model.Digest
	BlockDigest() model.Digest
	SizeBytes() uint64
}

// ArtifactServerSource is the complete authority surface admitted by the
// Artifact protocol. A production Store is adapted without caching or
// widening its transaction result.
type ArtifactServerSource interface {
	ReadArtifactSourceManifest(context.Context,
		store.ReadArtifactSourceManifestSpec,
	) (ArtifactSourceManifest, error)
	ReadArtifactSourceBlock(context.Context,
		store.ReadArtifactSourceBlockSpec,
	) (ArtifactSourceBlock, error)
}

// ArtifactStoreSource is the exact durable Store API wrapped by
// NewArtifactServerStoreSource. Keeping the concrete result types here means
// the adapter cannot manufacture or reinterpret authority.
type ArtifactStoreSource interface {
	ReadArtifactSourceManifest(context.Context,
		store.ReadArtifactSourceManifestSpec,
	) (store.ArtifactSourceManifest, error)
	ReadArtifactSourceBlock(context.Context,
		store.ReadArtifactSourceBlockSpec,
	) (store.ArtifactSourceBlock, error)
}

type artifactServerStoreSource struct {
	store ArtifactStoreSource
}

// NewArtifactServerStoreSource provides the production adapter from the exact
// Store API to the server's immutable accessor-only view.
func NewArtifactServerStoreSource(source ArtifactStoreSource) (ArtifactServerSource, error) {
	if isNilArtifactStoreSource(source) {
		return nil, fmt.Errorf("%w: live Artifact Store is required", ErrArtifactServer)
	}
	return artifactServerStoreSource{store: source}, nil
}

func (source artifactServerStoreSource) ReadArtifactSourceManifest(ctx context.Context,
	spec store.ReadArtifactSourceManifestSpec,
) (ArtifactSourceManifest, error) {
	return source.store.ReadArtifactSourceManifest(ctx, spec)
}

func (source artifactServerStoreSource) ReadArtifactSourceBlock(ctx context.Context,
	spec store.ReadArtifactSourceBlockSpec,
) (ArtifactSourceBlock, error) {
	return source.store.ReadArtifactSourceBlock(ctx, spec)
}

// ArtifactCAS is the sole byte-store capability used after the Store snapshot
// has ended. It provides no path or generic write surface.
type ArtifactCAS interface {
	Read(model.Digest, int) ([]byte, error)
}

type ArtifactServerOptions struct {
	Host   host.Host
	Source ArtifactServerSource
	CAS    ArtifactCAS
}

// ArtifactServer is the sole /mnemon/artifacts/1 owner for one Host. Each
// secure stream carries one request and one response; RemotePeer is the only
// requester identity supplied to the Store.
type ArtifactServer struct {
	host   host.Host
	source ArtifactServerSource
	cas    ArtifactCAS
	ctx    context.Context
	cancel context.CancelFunc

	nodeLimit int
	peerLimit int

	mu         sync.Mutex
	active     sync.WaitGroup
	closed     bool
	nodeActive int
	peerActive map[model.PeerID]int
	closeOnce  sync.Once
}

func NewArtifactServer(lifetime context.Context,
	options ArtifactServerOptions,
) (*ArtifactServer, error) {
	if lifetime == nil || lifetime.Err() != nil || nilArtifactServerValue(options.Host) ||
		isNilArtifactServerSource(options.Source) || isNilArtifactCAS(options.CAS) {
		return nil, fmt.Errorf("%w: live Host, source and CAS are required", ErrArtifactServer)
	}
	limits := HermeticLimits()
	if limits.NodeArtifactPulls <= 0 || limits.PeerArtifactPulls <= 0 ||
		limits.PeerArtifactPulls > limits.NodeArtifactPulls || limits.ArtifactRequestTimeout != 30*time.Second {
		return nil, fmt.Errorf("%w: invalid Artifact stream limits", ErrArtifactServer)
	}

	artifactServerConstructorMu.Lock()
	defer artifactServerConstructorMu.Unlock()
	for _, protocolID := range options.Host.Mux().Protocols() {
		if protocolID == ArtifactsProtocol {
			return nil, fmt.Errorf("%w: Host already owns the Artifact protocol", ErrArtifactServer)
		}
	}

	ownedContext, cancel := context.WithCancel(lifetime)
	server := &ArtifactServer{host: options.Host, source: options.Source, cas: options.CAS,
		ctx: ownedContext, cancel: cancel, nodeLimit: limits.NodeArtifactPulls,
		peerLimit: limits.PeerArtifactPulls, peerActive: make(map[model.PeerID]int)}
	options.Host.SetStreamHandler(ArtifactsProtocol, server.handle)
	return server, nil
}

func (server *ArtifactServer) handle(stream network.Stream) {
	if !server.begin() {
		if stream != nil {
			_ = stream.Reset()
		}
		return
	}
	defer server.active.Done()

	requester, err := authenticateArtifactStream(stream)
	if err != nil {
		if stream != nil {
			_ = stream.Reset()
		}
		return
	}
	if !server.admit(requester) {
		if err := server.writeBusy(stream); err != nil {
			_ = stream.Reset()
			return
		}
		_ = stream.Close()
		return
	}
	defer server.release(requester)

	if err := server.serve(stream, requester); err != nil {
		_ = stream.Reset()
		return
	}
	_ = stream.Close()
}

func (server *ArtifactServer) begin() bool {
	if server == nil {
		return false
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.closed || server.ctx == nil || server.ctx.Err() != nil {
		return false
	}
	server.active.Add(1)
	return true
}

func (server *ArtifactServer) admit(requester model.PeerID) bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.closed || server.ctx.Err() != nil || requester.IsZero() ||
		server.nodeActive >= server.nodeLimit || server.peerActive[requester] >= server.peerLimit {
		return false
	}
	server.nodeActive++
	server.peerActive[requester]++
	return true
}

func (server *ArtifactServer) release(requester model.PeerID) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.nodeActive > 0 {
		server.nodeActive--
	}
	if server.peerActive[requester] <= 1 {
		delete(server.peerActive, requester)
	} else {
		server.peerActive[requester]--
	}
}

func authenticateArtifactStream(stream network.Stream) (model.PeerID, error) {
	if stream == nil || stream.Protocol() != ArtifactsProtocol || stream.Conn() == nil ||
		stream.Conn().RemotePeer() == "" || stream.Conn().LocalPeer() == "" {
		return model.PeerID{}, fmt.Errorf("%w: invalid Artifact stream", ErrArtifactServer)
	}
	requester, _, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	local, _, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	if remoteErr != nil || localErr != nil || requester == local {
		return model.PeerID{}, fmt.Errorf("%w: authenticate secure Artifact peers", ErrArtifactServer)
	}
	return requester, nil
}

func (server *ArtifactServer) serve(stream network.Stream, requester model.PeerID) error {
	requestContext, cancel, err := server.bindStream(stream)
	if err != nil {
		return err
	}
	defer cancel()

	first, release, err := readArtifactStreamFrame(stream, artifactSmallFrameBytes)
	if err != nil {
		if errors.Is(err, network.ErrResourceLimitExceeded) {
			return server.writeBusy(stream)
		}
		return fmt.Errorf("%w: read first frame: %w", ErrArtifactServer, err)
	}
	defer release()

	var response ArtifactFramePayload
	switch first.Type() {
	case ArtifactFrameGetManifest:
		request, ok := first.Payload().(GetManifest)
		if !ok {
			return fmt.Errorf("%w: invalid GetManifest payload", ErrArtifactServer)
		}
		response, err = server.manifest(requestContext, requester, request)
	case ArtifactFrameGetBlock:
		request, ok := first.Payload().(GetBlock)
		if !ok {
			return fmt.Errorf("%w: invalid GetBlock payload", ErrArtifactServer)
		}
		response, err = server.block(requestContext, requester, request)
	default:
		return fmt.Errorf("%w: first frame is not an Artifact request", ErrArtifactServer)
	}
	if err != nil {
		failure, safe := artifactSourceProtocolFailure(err)
		if !safe {
			return fmt.Errorf("%w: source operation failed", ErrArtifactServer)
		}
		response = failure
	}
	frame, err := NewArtifactFrame(response)
	if err != nil {
		return fmt.Errorf("%w: construct response", ErrArtifactServer)
	}
	if err := writeArtifactStreamFrame(stream, frame); err != nil {
		if errors.Is(err, errArtifactServerBudget) {
			return server.writeBusy(stream)
		}
		return fmt.Errorf("%w: write response: %w", ErrArtifactServer, err)
	}
	return nil
}

func (server *ArtifactServer) bindStream(stream network.Stream) (context.Context,
	context.CancelFunc, error,
) {
	deadline := time.Now().Add(HermeticLimits().ArtifactRequestTimeout)
	if parentDeadline, ok := server.ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return nil, nil, fmt.Errorf("%w: set stream deadline", ErrArtifactServer)
	}
	requestContext, cancel := context.WithDeadline(server.ctx, deadline)
	stopCancellation := context.AfterFunc(requestContext,
		func() { _ = stream.SetDeadline(time.Now()) })
	return requestContext, func() {
		stopCancellation()
		cancel()
	}, nil
}

func (server *ArtifactServer) manifest(ctx context.Context, requester model.PeerID,
	request GetManifest,
) (ArtifactFramePayload, error) {
	authority, err := server.source.ReadArtifactSourceManifest(ctx,
		store.ReadArtifactSourceManifestSpec{AuthenticatedPeerID: requester,
			ChannelID: request.ChannelID(), RootDigest: request.RootDigest()})
	if err != nil {
		return nil, err
	}
	if isNilArtifactSourceManifest(authority) {
		return nil, store.ErrArtifactSourceInvariant
	}
	rootDigest := authority.RootDigest()
	manifestDigest := authority.ManifestDigest()
	manifestBytes := authority.ManifestBytes()
	totalBytes := authority.TotalBytes()
	domainManifest, manifestErr := artifactdomain.ParseManifest(manifestBytes)
	if manifestErr != nil || rootDigest != request.RootDigest() ||
		manifestDigest != domainManifest.ManifestDigest() ||
		rootDigest != domainManifest.RootDigest() || totalBytes != domainManifest.TotalBytes() ||
		len(manifestBytes) == 0 || len(manifestBytes) > artifactManifestMaximum() {
		return nil, store.ErrArtifactSourceInvariant
	}

	// The Store snapshot is complete before this byte read starts. The CAS is
	// rehashed and compared with the exact immutable snapshot before any frame
	// is constructed.
	casBytes, err := server.cas.Read(manifestDigest, artifactManifestMaximum())
	if err != nil {
		return nil, fmt.Errorf("%w: manifest CAS read: %w", ErrArtifactServer, err)
	}
	if !bytes.Equal(casBytes, manifestBytes) || model.Sum(casBytes) != manifestDigest {
		return nil, fmt.Errorf("%w: manifest CAS verification: %w",
			ErrArtifactServer, artifactdomain.ErrCASCorruption)
	}
	manifestJSON, err := model.NewJSON(casBytes)
	if err != nil || !bytes.Equal(manifestJSON.Bytes(), casBytes) {
		return nil, fmt.Errorf("%w: manifest CAS canonical bytes", ErrArtifactServer)
	}
	payload, err := NewManifest(ManifestSpec{RootDigest: request.RootDigest(), Manifest: manifestJSON})
	if err != nil || payload.ManifestDigest() != manifestDigest {
		return nil, fmt.Errorf("%w: construct verified Manifest", ErrArtifactServer)
	}
	return payload, nil
}

func (server *ArtifactServer) block(ctx context.Context, requester model.PeerID,
	request GetBlock,
) (ArtifactFramePayload, error) {
	authority, err := server.source.ReadArtifactSourceBlock(ctx,
		store.ReadArtifactSourceBlockSpec{AuthenticatedPeerID: requester,
			ChannelID: request.ChannelID(), RootDigest: request.RootDigest(),
			BlockDigest: request.BlockDigest()})
	if err != nil {
		return nil, err
	}
	if isNilArtifactSourceBlock(authority) {
		return nil, store.ErrArtifactSourceInvariant
	}
	rootDigest := authority.RootDigest()
	blockDigest := authority.BlockDigest()
	sizeBytes := authority.SizeBytes()
	if rootDigest != request.RootDigest() || blockDigest != request.BlockDigest() || sizeBytes == 0 ||
		sizeBytes > uint64(artifactBlockMaximum()) {
		return nil, store.ErrArtifactSourceInvariant
	}

	blockBytes, err := server.cas.Read(blockDigest, artifactBlockMaximum())
	if err != nil {
		return nil, fmt.Errorf("%w: block CAS read: %w", ErrArtifactServer, err)
	}
	if uint64(len(blockBytes)) != sizeBytes || model.Sum(blockBytes) != blockDigest {
		return nil, fmt.Errorf("%w: block CAS verification: %w",
			ErrArtifactServer, artifactdomain.ErrCASCorruption)
	}
	payload, err := NewBlock(BlockSpec{BlockDigest: blockDigest, BlockBytes: blockBytes})
	if err != nil {
		return nil, fmt.Errorf("%w: construct verified Block", ErrArtifactServer)
	}
	return payload, nil
}

func artifactSourceProtocolFailure(cause error) (ArtifactProtocolError, bool) {
	var specification ArtifactProtocolErrorSpec
	switch {
	case errors.Is(cause, store.ErrArtifactSourceInput),
		errors.Is(cause, store.ErrArtifactSourceInvariant):
		return ArtifactProtocolError{}, false
	case errors.Is(cause, store.ErrArtifactSourceUnavailable):
		specification = ArtifactProtocolErrorSpec{Code: ArtifactErrorNotAuthorized}
	case errors.Is(cause, artifactdomain.ErrCASCorruption):
		specification = ArtifactProtocolErrorSpec{Code: ArtifactErrorCorrupt}
	case errors.Is(cause, context.DeadlineExceeded),
		errors.Is(cause, network.ErrResourceLimitExceeded),
		errors.Is(cause, errArtifactServerBudget):
		specification = ArtifactProtocolErrorSpec{Code: ArtifactErrorBusy,
			Retryable: true, RetryAfter: artifactServerBusyRetry}
	default:
		return ArtifactProtocolError{}, false
	}
	payload, err := NewArtifactProtocolError(specification)
	return payload, err == nil
}

func (server *ArtifactServer) writeBusy(stream network.Stream) error {
	if server == nil || server.ctx == nil {
		return fmt.Errorf("%w: unavailable overload context", ErrArtifactServer)
	}
	if _, err := authenticateArtifactStream(stream); err != nil {
		return err
	}
	deadline := time.Now().Add(artifactServerBusyRetry)
	if parentDeadline, ok := server.ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return fmt.Errorf("%w: set overload deadline", ErrArtifactServer)
	}
	stopCancellation := context.AfterFunc(server.ctx,
		func() { _ = stream.SetDeadline(time.Now()) })
	defer stopCancellation()
	payload, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorBusy,
		Retryable: true, RetryAfter: artifactServerBusyRetry})
	if err != nil {
		return err
	}
	frame, err := NewArtifactFrame(payload)
	if err != nil {
		return err
	}
	return writeArtifactStreamFrame(stream, frame)
}

func writeArtifactStreamFrame(stream network.Stream, frame ArtifactFrame) error {
	if stream == nil {
		return fmt.Errorf("%w: live stream is required", ErrArtifactServer)
	}
	return writeArtifactFrameWithScope(stream, stream.Scope(), frame)
}

func writeArtifactFrameWithScope(writer io.Writer, scope network.ResourceScope,
	frame ArtifactFrame,
) error {
	if writer == nil || scope == nil || frame.IsZero() {
		return fmt.Errorf("%w: live output scope and response frame are required", ErrArtifactServer)
	}
	reserved := len(frame.canonical.raw)
	if reserved <= 0 || reserved > maxArtifactFrameBytes() {
		return fmt.Errorf("%w: response frame exceeds its memory bound", ErrArtifactServer)
	}
	if err := scope.ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
		return fmt.Errorf("%w: %w: reserve response frame memory: %w",
			ErrArtifactServer, errArtifactServerBudget, err)
	}
	defer scope.ReleaseMemory(reserved)
	return WriteArtifactFrame(writer, frame)
}

func (server *ArtifactServer) Close() error {
	if server == nil {
		return nil
	}
	server.closeOnce.Do(func() {
		artifactServerConstructorMu.Lock()
		defer artifactServerConstructorMu.Unlock()
		server.mu.Lock()
		server.closed = true
		if server.cancel != nil {
			server.cancel()
		}
		if server.host != nil {
			server.host.RemoveStreamHandler(ArtifactsProtocol)
		}
		server.mu.Unlock()
		server.active.Wait()
	})
	return nil
}

func isNilArtifactServerSource(source ArtifactServerSource) bool {
	return nilArtifactServerValue(source)
}

func isNilArtifactStoreSource(source ArtifactStoreSource) bool {
	return nilArtifactServerValue(source)
}

func isNilArtifactCAS(cas ArtifactCAS) bool { return nilArtifactServerValue(cas) }

func isNilArtifactSourceManifest(manifest ArtifactSourceManifest) bool {
	return nilArtifactServerValue(manifest)
}

func isNilArtifactSourceBlock(block ArtifactSourceBlock) bool {
	return nilArtifactServerValue(block)
}

func nilArtifactServerValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
