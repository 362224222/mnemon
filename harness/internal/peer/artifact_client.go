package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrArtifactClient                = errors.New("Mnemon Artifact client")
	ErrArtifactClientTransport       = fmt.Errorf("%w: transport unavailable", ErrArtifactClient)
	ErrArtifactClientResponse        = fmt.Errorf("%w: invalid response", ErrArtifactClient)
	ErrArtifactClientManifestInvalid = fmt.Errorf("%w: manifest invalid", ErrArtifactClientResponse)
	ErrArtifactClientDigestMismatch  = fmt.Errorf("%w: digest mismatch", ErrArtifactClientResponse)
)

// ArtifactRemoteFailure is the complete, diagnostic-free failure vocabulary
// admitted from an authenticated Artifact source. It deliberately exposes no
// remote-provided text or implementation error.
type ArtifactRemoteFailure struct {
	code       ArtifactProtocolErrorCode
	retryable  bool
	retryAfter time.Duration
}

func (failure *ArtifactRemoteFailure) Error() string {
	if failure == nil || !failure.code.Valid() {
		return ErrArtifactClient.Error()
	}
	return fmt.Sprintf("%s: %s", ErrArtifactClient, failure.code)
}

func (failure *ArtifactRemoteFailure) Unwrap() error { return ErrArtifactClient }
func (failure *ArtifactRemoteFailure) Code() ArtifactProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}
func (failure *ArtifactRemoteFailure) Retryable() bool {
	return failure != nil && failure.retryable
}
func (failure *ArtifactRemoteFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.retryAfter
}

type ArtifactClientOptions struct{ Host host.Host }

// ArtifactClient performs one authenticated content-addressed read per
// stream. Peer discovery, retries, CAS writes, Inbox transitions and pinning
// belong to the mesh and receiver layers.
type ArtifactClient struct{ host host.Host }

func NewArtifactClient(options ArtifactClientOptions) (*ArtifactClient, error) {
	if nilArtifactClientHost(options.Host) {
		return nil, fmt.Errorf("%w: live Host is required", ErrArtifactClient)
	}
	localPeer, _, err := secureChannelPeer(options.Host.ID())
	if err != nil || localPeer.IsZero() {
		return nil, fmt.Errorf("%w: Host must use a canonical Ed25519 identity", ErrArtifactClient)
	}
	if HermeticLimits().ArtifactRequestTimeout != 30*time.Second ||
		maxArtifactFrameBytes() != 8<<20 {
		return nil, fmt.Errorf("%w: invalid Artifact exchange limits", ErrArtifactClient)
	}
	return &ArtifactClient{host: options.Host}, nil
}

// GetManifest obtains and verifies one exact root manifest from source.
func (client *ArtifactClient) GetManifest(ctx context.Context, source model.PeerID,
	request GetManifest,
) (Manifest, error) {
	if request.IsZero() {
		return Manifest{}, fmt.Errorf("%w: complete GetManifest is required", ErrArtifactClient)
	}
	response, err := client.exchange(ctx, source, request)
	if err != nil {
		return Manifest{}, err
	}
	manifest, ok := response.Payload().(Manifest)
	if response.Type() != ArtifactFrameManifest || !ok {
		return Manifest{}, ErrArtifactClientResponse
	}
	if err := validateArtifactClientManifest(request, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// GetBlock obtains and verifies one exact content block from source.
func (client *ArtifactClient) GetBlock(ctx context.Context, source model.PeerID,
	request GetBlock,
) (Block, error) {
	if request.IsZero() {
		return Block{}, fmt.Errorf("%w: complete GetBlock is required", ErrArtifactClient)
	}
	response, err := client.exchange(ctx, source, request)
	if err != nil {
		return Block{}, err
	}
	block, ok := response.Payload().(Block)
	if response.Type() != ArtifactFrameBlock || !ok {
		return Block{}, ErrArtifactClientResponse
	}
	if err := validateArtifactClientBlock(request, block); err != nil {
		return Block{}, err
	}
	return block, nil
}

func (client *ArtifactClient) exchange(ctx context.Context, source model.PeerID,
	request ArtifactFramePayload,
) (ArtifactFrame, error) {
	if client == nil || nilArtifactClientHost(client.host) || ctx == nil || source.IsZero() ||
		request == nil {
		return ArtifactFrame{}, fmt.Errorf("%w: complete exchange input is required", ErrArtifactClient)
	}
	if err := ctx.Err(); err != nil {
		return ArtifactFrame{}, err
	}
	sourceID, err := canonicalLibp2pID(source)
	if err != nil || sourceID == client.host.ID() {
		return ArtifactFrame{}, fmt.Errorf("%w: exact remote source is required", ErrArtifactClient)
	}
	frame, err := NewArtifactFrame(request)
	if err != nil || !frame.IsRequest() {
		return ArtifactFrame{}, fmt.Errorf("%w: invalid local request", ErrArtifactClient)
	}

	requestContext, cancel := context.WithTimeout(ctx, HermeticLimits().ArtifactRequestTimeout)
	defer cancel()
	stream, err := client.host.NewStream(requestContext, sourceID, ArtifactsProtocol)
	if err != nil {
		return ArtifactFrame{}, artifactClientTransportFailure(ctx)
	}
	completed := false
	defer func() {
		if completed {
			_ = stream.Close()
		} else {
			_ = stream.Reset()
		}
	}()

	if err := authenticateArtifactClientStream(client.host, stream, source); err != nil {
		return ArtifactFrame{}, ErrArtifactClientResponse
	}
	stopCancellation, err := bindArtifactClientStream(requestContext, stream)
	if err != nil {
		return ArtifactFrame{}, artifactClientTransportFailure(ctx)
	}
	defer stopCancellation()
	if err := writeArtifactClientFrame(stream, frame); err != nil {
		return ArtifactFrame{}, artifactClientTransportFailure(ctx)
	}
	if err := stream.CloseWrite(); err != nil {
		return ArtifactFrame{}, artifactClientTransportFailure(ctx)
	}
	response, release, err := readArtifactStreamFrame(stream, maxArtifactFrameBytes())
	if err != nil {
		switch request.(type) {
		case GetManifest:
			switch {
			case errors.Is(err, errArtifactFrameManifestInvalid):
				return ArtifactFrame{}, ErrArtifactClientManifestInvalid
			case errors.Is(err, errArtifactFrameManifestDigestMismatch):
				return ArtifactFrame{}, ErrArtifactClientDigestMismatch
			}
		case GetBlock:
			if errors.Is(err, errArtifactFrameBlockDigestMismatch) {
				return ArtifactFrame{}, ErrArtifactClientDigestMismatch
			}
		}
		return ArtifactFrame{}, artifactClientReadFailure(ctx, err)
	}
	defer release()
	if response.Type() == ArtifactFrameProtocolError {
		failure, ok := response.Payload().(ArtifactProtocolError)
		if !ok || !validArtifactRemoteFailure(failure) {
			return ArtifactFrame{}, ErrArtifactClientResponse
		}
		completed = true
		return ArtifactFrame{}, &ArtifactRemoteFailure{code: failure.Code(),
			retryable: failure.Retryable(), retryAfter: failure.RetryAfter()}
	}
	switch typed := request.(type) {
	case GetManifest:
		manifest, ok := response.Payload().(Manifest)
		if response.Type() != ArtifactFrameManifest || !ok {
			return ArtifactFrame{}, ErrArtifactClientResponse
		}
		if err := validateArtifactClientManifest(typed, manifest); err != nil {
			return ArtifactFrame{}, err
		}
	case GetBlock:
		block, ok := response.Payload().(Block)
		if response.Type() != ArtifactFrameBlock || !ok {
			return ArtifactFrame{}, ErrArtifactClientResponse
		}
		if err := validateArtifactClientBlock(typed, block); err != nil {
			return ArtifactFrame{}, err
		}
	default:
		return ArtifactFrame{}, fmt.Errorf("%w: request is not a client operation", ErrArtifactClient)
	}
	completed = true
	return response, nil
}

func authenticateArtifactClientStream(local host.Host, stream network.Stream,
	source model.PeerID,
) error {
	if nilArtifactClientHost(local) || stream == nil || stream.Scope() == nil ||
		stream.Conn() == nil || stream.Protocol() != ArtifactsProtocol ||
		stream.Conn().RemotePeer() == "" || stream.Conn().LocalPeer() != local.ID() {
		return ErrArtifactClientResponse
	}
	remotePeer, _, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	localPeer, _, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	if remoteErr != nil || localErr != nil || remotePeer != source || remotePeer == localPeer {
		return ErrArtifactClientResponse
	}
	return nil
}

func bindArtifactClientStream(ctx context.Context, stream network.Stream) (func(), error) {
	if ctx == nil || stream == nil {
		return nil, ErrArtifactClientTransport
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > HermeticLimits().ArtifactRequestTimeout {
		return nil, ErrArtifactClientTransport
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = stream.SetDeadline(time.Now()) })
	return func() { stopCancellation() }, nil
}

func writeArtifactClientFrame(stream network.Stream, frame ArtifactFrame) error {
	if stream == nil || stream.Scope() == nil || frame.IsZero() || !frame.IsRequest() {
		return ErrArtifactClientTransport
	}
	reserved := len(frame.CanonicalJSON().Bytes())
	if reserved <= 0 || reserved > maxArtifactFrameBytes() {
		return ErrArtifactClientTransport
	}
	if err := stream.Scope().ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
		return ErrArtifactClientTransport
	}
	defer stream.Scope().ReleaseMemory(reserved)
	return WriteArtifactFrame(stream, frame)
}

func validateArtifactClientManifest(request GetManifest, response Manifest) error {
	if request.IsZero() || response.IsZero() {
		return ErrArtifactClientResponse
	}
	manifestBytes := response.ManifestBytes()
	if len(manifestBytes) == 0 || len(manifestBytes) > artifactManifestMaximum() {
		return ErrArtifactClientManifestInvalid
	}
	manifest, err := artifactdomain.ParseManifest(manifestBytes)
	if err != nil || manifest.TotalBytes() > artifactdomain.MaxTotalBytes {
		return ErrArtifactClientManifestInvalid
	}
	if response.RootDigest() != request.RootDigest() ||
		manifest.RootDigest() != request.RootDigest() ||
		manifest.RootDigest() != response.RootDigest() ||
		manifest.ManifestDigest() != response.ManifestDigest() ||
		model.Sum(manifestBytes) != response.ManifestDigest() {
		return ErrArtifactClientDigestMismatch
	}
	return nil
}

func validateArtifactClientBlock(request GetBlock, response Block) error {
	if request.IsZero() || response.IsZero() {
		return ErrArtifactClientResponse
	}
	blockBytes := response.BlockBytes()
	if len(blockBytes) == 0 || len(blockBytes) > artifactBlockMaximum() ||
		response.BlockDigest() != request.BlockDigest() ||
		model.Sum(blockBytes) != response.BlockDigest() {
		return ErrArtifactClientDigestMismatch
	}
	return nil
}

func validArtifactRemoteFailure(failure ArtifactProtocolError) bool {
	if failure.IsZero() || !failure.Code().Valid() ||
		failure.Retryable() != failure.Code().retryable() {
		return false
	}
	if failure.Code() == ArtifactErrorBusy {
		return failure.RetryAfter() > 0 &&
			failure.RetryAfter() <= HermeticLimits().ArtifactRequestTimeout
	}
	return (failure.Code() == ArtifactErrorNotAuthorized || failure.Code() == ArtifactErrorCorrupt) &&
		failure.RetryAfter() == 0
}

func artifactClientTransportFailure(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
	}
	return ErrArtifactClientTransport
}

func artifactClientReadFailure(ctx context.Context, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
	}
	transport := errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) ||
		errors.Is(cause, network.ErrReset) || errors.Is(cause, network.ErrResourceLimitExceeded) ||
		errors.Is(cause, network.ErrResourceScopeClosed)
	var networkFailure net.Error
	if transport || errors.As(cause, &networkFailure) {
		return ErrArtifactClientTransport
	}
	return ErrArtifactClientResponse
}

func nilArtifactClientHost(value host.Host) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
