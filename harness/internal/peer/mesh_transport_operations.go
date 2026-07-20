package peer

import (
	"context"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (transport *MeshTransport) EnsureChannelTopic(ctx context.Context,
	channelID model.ChannelID,
) error {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return err
	}
	defer done()
	session, err := transport.runtime.session(callCtx, channelID)
	if err != nil {
		return err
	}
	if session == nil || !session.IsCurrent() {
		return fmt.Errorf("%w: Channel topic is not current", ErrMeshTransport)
	}
	return nil
}

func (transport *MeshTransport) HasCurrentChannelTopic(channelID model.ChannelID) bool {
	if transport == nil || channelID.IsZero() {
		return false
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.state == meshTransportRunning && transport.runCtx != nil &&
		transport.runCtx.Err() == nil && transport.runtimeLive() &&
		transport.runtime.HasCurrentSession(channelID)
}

func (transport *MeshTransport) JoinChannel(ctx context.Context, spec JoinChannelSpec,
	session ChannelJoinSession,
) (ChannelJoinResult, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	defer done()
	return transport.runtime.JoinChannel(callCtx, spec, session)
}

func (transport *MeshTransport) Hello(ctx context.Context, remote model.PeerID,
	request MemberHello,
) (MemberHelloAck, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return MemberHelloAck{}, err
	}
	defer done()
	return transport.memberClient.Hello(callCtx, remote, request)
}

func (transport *MeshTransport) Sync(ctx context.Context, remote model.PeerID,
	request SyncRequest,
) (ChannelMemberSyncResult, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return ChannelMemberSyncResult{}, err
	}
	defer done()
	return transport.memberClient.Sync(callCtx, remote, request)
}

func (transport *MeshTransport) Baseline(ctx context.Context, remote model.PeerID,
	request DataBaseline,
) (DataBaselineAck, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return DataBaselineAck{}, err
	}
	defer done()
	return transport.memberClient.Baseline(callCtx, remote, request)
}

func (transport *MeshTransport) Pull(ctx context.Context, origin model.PeerID,
	request PullRequest,
) (PullPage, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return PullPage{}, err
	}
	defer done()
	return transport.eventClient.Pull(callCtx, origin, request)
}

func (transport *MeshTransport) Acknowledge(ctx context.Context, origin model.PeerID,
	acknowledgement CursorAck,
) error {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return err
	}
	defer done()
	return transport.eventClient.Acknowledge(callCtx, origin, acknowledgement)
}

func (transport *MeshTransport) GetManifest(ctx context.Context, source model.PeerID,
	request GetManifest,
) (Manifest, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return Manifest{}, err
	}
	defer done()
	return transport.artifactClient.GetManifest(callCtx, source, request)
}

func (transport *MeshTransport) GetBlock(ctx context.Context, source model.PeerID,
	request GetBlock,
) (Block, error) {
	callCtx, done, err := transport.beginCall(ctx)
	if err != nil {
		return Block{}, err
	}
	defer done()
	return transport.artifactClient.GetBlock(callCtx, source, request)
}
