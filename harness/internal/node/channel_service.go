package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const defaultChannelInviteLifetime = time.Hour

type ChannelManagerOptions struct {
	Store    *store.Store
	Identity *Identity
	Runtime  *peer.MeshRuntime
	Clock    Clock
	Random   io.Reader
}

// ChannelManager is mnemond's sole Channel mutation surface. Its mutex
// serializes selector resolution with Store mutations while Store remains the
// only durable source of truth.
type ChannelManager struct {
	store    *store.Store
	identity *Identity
	runtime  *peer.MeshRuntime
	clock    Clock
	random   io.Reader
	members  interface {
		TriggerScope(model.ChannelID, model.PeerID) error
	}
	mu sync.Mutex
}

func NewChannelManager(options ChannelManagerOptions) (*ChannelManager, error) {
	if options.Store == nil || options.Identity == nil || options.Identity.PublicationSigner() == nil ||
		options.Runtime == nil {
		return nil, errors.New("mnemond Channel manager requires Store, identity and mesh runtime")
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelManager{store: options.Store, identity: options.Identity, runtime: options.Runtime,
		clock: options.Clock, random: options.Random}, nil
}

func (manager *ChannelManager) ChannelCreate(ctx context.Context, metadata RequestMetadata,
	request ChannelCreateRequest,
) (ChannelCreateResponse, *APIError) {
	operation, apiErr := manager.channelMutationOperation(ctx, metadata,
		store.ChannelMutationCreate)
	if apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	manager.mu.Lock()
	mutation, found, err := manager.store.ReadChannelMutation(ctx, operation)
	manager.mu.Unlock()
	if err != nil {
		return ChannelCreateResponse{}, channelMutationAPIError(err)
	}
	if found {
		return manager.channelCreateMutationResponse(ctx, metadata, mutation)
	}
	channel, genesis, token, err := manager.buildChannel(ctx, request.Name,
		operation, metadata.OperationKeySecret)
	if err != nil {
		return ChannelCreateResponse{}, channelAPIError(err)
	}
	manager.mu.Lock()
	latest, err := manager.store.ReadChannelControlAuthority(ctx)
	if err == nil {
		channel, err = rebindChannelAlias(channel,
			uniqueChannelAlias(channelAlias(request.Name), latest))
	}
	var result store.CreateChannelResult
	if err == nil {
		result, err = manager.store.CreateChannel(ctx, store.CreateChannelSpec{Channel: channel,
			Genesis: genesis, Token: token, Operation: &operation})
	}
	manager.mu.Unlock()
	if err != nil {
		return ChannelCreateResponse{}, channelMutationAPIError(err)
	}
	if result.Created {
		manager.refreshAndJoin(ctx, result.Channel)
	}
	return manager.channelCreateMutationResponse(ctx, metadata, result.Mutation)
}

func (manager *ChannelManager) ChannelJoin(ctx context.Context, metadata RequestMetadata,
	request ChannelJoinRequest,
) (ChannelJoinResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelJoinResponse{}, apiErr
	}
	token, err := model.ParseEnrollmentToken(request.Token)
	if err != nil {
		return ChannelJoinResponse{}, NewAPIError(CodeInvalidToken,
			"Channel invite token is invalid")
	}
	if !manager.clock.Now().Before(token.Payload().ExpiresAt()) {
		return ChannelJoinResponse{}, NewAPIError(CodeTokenExpired,
			"Channel invite has expired")
	}
	addresses := manager.runtime.AdvertisedMultiaddrs()
	client, err := peer.NewChannelEnrollmentClient(peer.ChannelEnrollmentClientOptions{Store: manager.store})
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	manager.mu.Lock()
	control, err := manager.store.ReadChannelControlAuthority(ctx)
	if err != nil {
		manager.mu.Unlock()
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	descriptor := token.Payload().Descriptor().Descriptor()
	alias := existingChannelAlias(descriptor.ID(), control)
	if alias == "" {
		alias = uniqueChannelAlias(channelAlias(descriptor.Name()), control)
	}
	manager.mu.Unlock()
	spec := peer.JoinChannelSpec{Token: token, DisplayLabel: memberAlias(manager.identity.PeerID()),
		AdvertisedMultiaddrs: addresses, LocalAlias: alias}

	installed, err := manager.runtime.EnrollChannel(ctx, client, spec,
		manager.store.ReadChannelMeshAuthority)
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	manager.markTopicJoined(ctx, installed.Channel)
	view, err := manager.readChannelView(ctx, installed.Channel.ID())
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	manager.triggerMemberReconcileScope(installed.Channel.ID(),
		installed.Channel.OwnerPeerID())
	return ChannelJoinResponse{SchemaVersion: SchemaVersion,
		Status: channelJoinResponseStatus(installed.Status), Channel: view}, nil
}

func (manager *ChannelManager) triggerMemberReconcileScope(channelID model.ChannelID,
	peerID model.PeerID,
) {
	if manager != nil && manager.members != nil {
		_ = manager.members.TriggerScope(channelID, peerID)
	}
}

func (manager *ChannelManager) ChannelInvite(ctx context.Context, metadata RequestMetadata,
	request ChannelInviteRequest,
) (ChannelInviteResponse, *APIError) {
	operation, apiErr := manager.channelMutationOperation(ctx, metadata,
		store.ChannelMutationInvite)
	if apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	manager.mu.Lock()
	mutation, found, err := manager.store.ReadChannelMutation(ctx, operation)
	if err != nil {
		manager.mu.Unlock()
		return ChannelInviteResponse{}, channelMutationAPIError(err)
	}
	if found {
		manager.mu.Unlock()
		return manager.channelInviteMutationResponse(ctx, metadata, mutation)
	}
	authority, selected, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		manager.mu.Unlock()
		return ChannelInviteResponse{}, apiErr
	}
	channel := selected.Channel()
	if channel.OwnerPeerID() != authority.LocalPeerID() {
		manager.mu.Unlock()
		return ChannelInviteResponse{}, NewAPIError(CodeWrongOwner,
			"only the Channel owner may create invites")
	}
	remaining := remainingChannelSeats(selected.Roster(), channel.MemberLimit())
	uses := request.Uses
	if uses == 0 {
		uses = remaining
	}
	if uses == 0 || uses > remaining {
		manager.mu.Unlock()
		return ChannelInviteResponse{}, NewAPIError(CodeChannelFull,
			"Channel has insufficient remaining seats")
	}
	lifetime := defaultChannelInviteLifetime
	if request.ExpiresSeconds != 0 {
		lifetime = time.Duration(request.ExpiresSeconds) * time.Second
	}
	owner, ok := selected.Roster().CurrentMember(authority.LocalPeerID())
	if !ok {
		manager.mu.Unlock()
		return ChannelInviteResponse{}, channelAPIError(store.ErrChannelInviteOwner)
	}
	expectedOpenGrant := store.ChannelInviteOpenGrantFence{}
	if grant, found := selected.OpenGrant(); found {
		expectedOpenGrant = store.ChannelInviteOpenGrantFence{Present: true, GrantID: grant.ID()}
	}
	expectedRosterHead := selected.Roster().Head()
	manager.mu.Unlock()
	at := manager.clock.Now()
	token, err := manager.buildToken(ctx, channel.Descriptor(), owner.Multiaddrs(), at,
		lifetime, uses, operation, metadata.OperationKeySecret)
	if err != nil {
		return ChannelInviteResponse{}, channelAPIError(err)
	}
	manager.mu.Lock()
	result, err := manager.store.RotateChannelInvite(ctx, store.RotateChannelInviteSpec{
		ChannelID: channel.ID(), Token: token, At: at,
		ExpectedRosterHead: expectedRosterHead, ExpectedOpenGrant: expectedOpenGrant,
		Operation: &operation})
	manager.mu.Unlock()
	if err != nil {
		return ChannelInviteResponse{}, channelMutationAPIError(err)
	}
	return manager.channelInviteMutationResponse(ctx, metadata, result.Mutation)
}

func (manager *ChannelManager) ChannelInviteClose(ctx context.Context, metadata RequestMetadata,
	request ChannelInviteCloseRequest,
) (ChannelInviteCloseResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelInviteCloseResponse{}, apiErr
	}
	at := manager.clock.Now()
	manager.mu.Lock()
	authority, selected, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		manager.mu.Unlock()
		return ChannelInviteCloseResponse{}, apiErr
	}
	channel := selected.Channel()
	if channel.OwnerPeerID() != authority.LocalPeerID() {
		manager.mu.Unlock()
		return ChannelInviteCloseResponse{}, NewAPIError(CodeWrongOwner,
			"only the Channel owner may close invites")
	}
	grant, ok := selected.OpenGrant()
	if !ok {
		manager.mu.Unlock()
		return ChannelInviteCloseResponse{}, NewAPIError(CodeTokenClosed,
			"Channel has no open invite")
	}
	_, err := manager.store.CloseChannelInvite(ctx, channel.ID(), grant.ID(), at)
	manager.mu.Unlock()
	if err != nil {
		return ChannelInviteCloseResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, channel.ID())
	if err != nil {
		return ChannelInviteCloseResponse{}, channelAPIError(err)
	}
	view.Invite = nil
	return ChannelInviteCloseResponse{SchemaVersion: SchemaVersion,
		Status: "closed", Channel: view}, nil
}

func (manager *ChannelManager) buildChannel(ctx context.Context, requestedName string,
	operation store.ChannelMutationOperation,
	operationSecret []byte,
) (model.Channel, model.Member, model.EnrollmentToken, error) {
	at := manager.clock.Now().UTC()
	channelID, err := manager.newChannelID()
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	alias := channelAlias(requestedName)
	name := requestedName
	if name == "" {
		name = "Channel " + strings.TrimPrefix(channelID.String(), "channel-")[:8]
	}
	descriptor, err := model.NewChannelDescriptor(model.ChannelDescriptorSpec{ID: channelID, Name: name,
		OwnerPeerID: manager.identity.PeerID(), OwnerPublicKey: manager.identity.PublicKey(),
		MemberLimit: model.MaxMembersPerChannel, CreatedAt: at})
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	descriptorMessage, _ := model.ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	descriptorSignature, err := manager.identity.PublicationSigner().Sign(ctx, descriptorMessage)
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	signed, err := model.AttachChannelDescriptorSignature(descriptor, descriptorSignature)
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	local, err := manager.store.ReadLocalAuthority(ctx)
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Digest(), Revision: 1, PeerID: manager.identity.PeerID(),
		OriginEpoch: local.Node.OriginEpoch(), DisplayLabel: memberAlias(manager.identity.PeerID()),
		PublicKey: manager.identity.PublicKey(), Multiaddrs: manager.runtime.AdvertisedMultiaddrs(),
		Protocols: model.RequiredMemberProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberActive, CreatedAt: at})
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	recordMessage, _ := model.MemberRecordSigningMessage(channelID, record.Digest())
	recordSignature, err := manager.identity.PublicationSigner().Sign(ctx, recordMessage)
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	genesis, err := model.AttachMemberSignature(record, recordSignature)
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: signed, LocalAlias: alias,
		RosterHead: genesis.Head(), Status: model.ChannelActive, TopicState: model.TopicNotJoined,
		UpdatedAt: at})
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	token, err := manager.buildToken(ctx, signed, genesis.Multiaddrs(), at,
		defaultChannelInviteLifetime, model.MaxMembersPerChannel-1, operation, operationSecret)
	return channel, genesis, token, err
}

func (manager *ChannelManager) newChannelID() (model.ChannelID, error) {
	value, err := manager.randomIdentifier("channel-")
	if err != nil {
		return model.ChannelID{}, err
	}
	return model.ParseChannelID(value)
}
func (manager *ChannelManager) newGrantID() (model.GrantID, error) {
	value, err := manager.randomIdentifier("grant-")
	if err != nil {
		return model.GrantID{}, err
	}
	return model.ParseGrantID(value)
}
func (manager *ChannelManager) randomIdentifier(prefix string) (string, error) {
	raw := make([]byte, 16)
	if _, err := io.ReadFull(manager.random, raw); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw), nil
}

func channelJoinResponseStatus(status store.ChannelEnrollmentStatus) string {
	switch status {
	case store.ChannelEnrollmentReplayed:
		return "replayed"
	case store.ChannelEnrollmentMemberRevoked:
		return "member_revoked"
	case store.ChannelEnrollmentChannelClosed:
		return "channel_closed"
	default:
		return "joined"
	}
}
