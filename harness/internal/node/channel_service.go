package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"io"
	"strings"
	"sync"
	"time"
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
	members  interface{ Trigger() }
	mu       sync.Mutex
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
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, err := manager.store.ReadChannelControlAuthority(ctx)
	if err != nil {
		return ChannelCreateResponse{}, channelAPIError(err)
	}
	channel, genesis, token, err := manager.buildChannel(ctx, request.Name, authority)
	if err != nil {
		return ChannelCreateResponse{}, channelAPIError(err)
	}
	if _, err := manager.store.CreateChannel(ctx, store.CreateChannelSpec{Channel: channel,
		Genesis: genesis, Token: token}); err != nil {
		return ChannelCreateResponse{}, channelAPIError(err)
	}
	manager.refreshAndJoin(ctx, channel)
	view, err := manager.readChannelView(ctx, channel.ID())
	if err != nil {
		return ChannelCreateResponse{}, channelAPIError(err)
	}
	invite := inviteView(token.Payload().ExpiresAt(), token.Payload().MaxUses(), 0, "open", manager.clock.Now())
	view.Invite = &invite
	return ChannelCreateResponse{SchemaVersion: SchemaVersion, Status: "created",
		Channel: view, Invite: invite, InviteToken: token.Reveal()}, nil
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
	manager.mu.Lock()
	defer manager.mu.Unlock()
	before, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	control, err := manager.store.ReadChannelControlAuthority(ctx)
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	descriptor := token.Payload().Descriptor().Descriptor()
	alias := existingChannelAlias(descriptor.ID(), control)
	if alias == "" {
		alias = uniqueChannelAlias(channelAlias(descriptor.Name()), control)
	}
	client, err := peer.NewChannelEnrollmentClient(peer.ChannelEnrollmentClientOptions{Store: manager.store})
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	spec := peer.JoinChannelSpec{Token: token, DisplayLabel: memberAlias(manager.identity.PeerID()),
		AdvertisedMultiaddrs: manager.runtime.AdvertisedMultiaddrs(), LocalAlias: alias}
	installed, err := manager.runtime.EnrollChannel(ctx, before, client, spec,
		manager.store.ReadChannelMeshAuthority)
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	manager.markTopicJoined(ctx, installed.Channel)
	manager.triggerMemberReconcile()
	view, err := manager.readChannelView(ctx, installed.Channel.ID())
	if err != nil {
		return ChannelJoinResponse{}, channelAPIError(err)
	}
	return ChannelJoinResponse{SchemaVersion: SchemaVersion,
		Status: channelJoinResponseStatus(installed.Status), Channel: view}, nil
}

func (manager *ChannelManager) triggerMemberReconcile() {
	if manager != nil && manager.members != nil {
		manager.members.Trigger()
	}
}

func (manager *ChannelManager) ChannelInvite(ctx context.Context, metadata RequestMetadata,
	request ChannelInviteRequest,
) (ChannelInviteResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, selected, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	channel := selected.Channel()
	if channel.OwnerPeerID() != authority.LocalPeerID() {
		return ChannelInviteResponse{}, NewAPIError(CodeWrongOwner,
			"only the Channel owner may create invites")
	}
	remaining := remainingChannelSeats(selected.Roster(), channel.MemberLimit())
	uses := request.Uses
	if uses == 0 {
		uses = remaining
	}
	if uses == 0 || uses > remaining {
		return ChannelInviteResponse{}, NewAPIError(CodeChannelFull,
			"Channel has insufficient remaining seats")
	}
	lifetime := defaultChannelInviteLifetime
	if request.ExpiresSeconds != 0 {
		lifetime = time.Duration(request.ExpiresSeconds) * time.Second
	}
	owner, ok := selected.Roster().CurrentMember(authority.LocalPeerID())
	if !ok {
		return ChannelInviteResponse{}, channelAPIError(store.ErrChannelInviteOwner)
	}
	token, err := manager.buildToken(ctx, channel.Descriptor(), owner.Multiaddrs(), manager.clock.Now(),
		lifetime, uses)
	if err != nil {
		return ChannelInviteResponse{}, channelAPIError(err)
	}
	if _, err := manager.store.RotateChannelInvite(ctx, store.RotateChannelInviteSpec{
		ChannelID: channel.ID(), Token: token, At: manager.clock.Now()}); err != nil {
		return ChannelInviteResponse{}, channelAPIError(err)
	}
	view, err := manager.readChannelView(ctx, channel.ID())
	if err != nil {
		return ChannelInviteResponse{}, channelAPIError(err)
	}
	invite := inviteView(token.Payload().ExpiresAt(), token.Payload().MaxUses(), 0, "open", manager.clock.Now())
	view.Invite = &invite
	return ChannelInviteResponse{SchemaVersion: SchemaVersion, Status: "created",
		Channel: view, Invite: invite, InviteToken: token.Reveal()}, nil
}

func (manager *ChannelManager) ChannelInviteClose(ctx context.Context, metadata RequestMetadata,
	request ChannelInviteCloseRequest,
) (ChannelInviteCloseResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelInviteCloseResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	authority, selected, apiErr := manager.selectChannel(ctx, request.Channel)
	if apiErr != nil {
		return ChannelInviteCloseResponse{}, apiErr
	}
	channel := selected.Channel()
	if channel.OwnerPeerID() != authority.LocalPeerID() {
		return ChannelInviteCloseResponse{}, NewAPIError(CodeWrongOwner,
			"only the Channel owner may close invites")
	}
	grant, ok := selected.OpenGrant()
	if !ok {
		return ChannelInviteCloseResponse{}, NewAPIError(CodeTokenClosed,
			"Channel has no open invite")
	}
	if _, err := manager.store.CloseChannelInvite(ctx, channel.ID(), grant.ID(), manager.clock.Now()); err != nil {
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
	authority store.ChannelControlAuthority,
) (model.Channel, model.Member, model.EnrollmentToken, error) {
	at := manager.clock.Now().UTC()
	channelID, err := manager.newChannelID()
	if err != nil {
		return model.Channel{}, model.Member{}, model.EnrollmentToken{}, err
	}
	alias := uniqueChannelAlias(channelAlias(requestedName), authority)
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
		defaultChannelInviteLifetime, model.MaxMembersPerChannel-1)
	return channel, genesis, token, err
}

func (manager *ChannelManager) buildToken(ctx context.Context, descriptor model.SignedChannelDescriptor,
	addresses []string, at time.Time, lifetime time.Duration, uses uint8,
) (model.EnrollmentToken, error) {
	grantID, err := manager.newGrantID()
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	secret := make([]byte, model.EnrollmentSecretBytes)
	if _, err := io.ReadFull(manager.random, secret); err != nil {
		return model.EnrollmentToken{}, err
	}
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: addresses, GrantID: grantID, BearerSecret: secret, ExpiresAt: at.Add(lifetime),
		MaxUses: uses, ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion})
	clear(secret)
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	message, _ := model.EnrollmentTokenSigningMessage(descriptor.Descriptor().ID(), payload.Digest())
	signature, err := manager.identity.PublicationSigner().Sign(ctx, message)
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	return model.AttachEnrollmentTokenSignature(payload, signature)
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

func channelAlias(name string) string {
	var result strings.Builder
	separator := false
	for _, character := range strings.ToLower(name) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			result.WriteRune(character)
			separator = false
		} else if result.Len() != 0 && !separator {
			result.WriteByte('-')
			separator = true
		}
	}
	alias := strings.Trim(result.String(), "-")
	if alias == "" {
		return "channel"
	}
	if len(alias) > 48 {
		alias = strings.Trim(alias[:48], "-")
	}
	return alias
}

func uniqueChannelAlias(base string, authority store.ChannelControlAuthority) string {
	used := make(map[string]struct{}, len(authority.Channels()))
	for _, channel := range authority.Channels() {
		used[channel.Channel().LocalAlias()] = struct{}{}
	}
	if _, exists := used[base]; !exists {
		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s-%d", base, suffix)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func existingChannelAlias(channelID model.ChannelID, authority store.ChannelControlAuthority) string {
	for _, channel := range authority.Channels() {
		if channel.Channel().ID() == channelID {
			return channel.Channel().LocalAlias()
		}
	}
	return ""
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
