package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelControlAuthority = errors.New("durable Channel control authority is unavailable")

// ChannelControlAuthority is the controller's coherent read projection. It
// includes the current open grant's public lifecycle metadata, never its
// verifier or bearer token.
type ChannelControlAuthority struct {
	localPeerID model.PeerID
	channels    []ChannelControlChannel
}

type ChannelControlChannel struct {
	mesh             ChannelMeshChannel
	selectorBindings []model.PeerBinding
	openGrant        *ChannelControlGrant
}

type ChannelControlGrant struct {
	id        model.GrantID
	expiresAt time.Time
	maxUses   uint8
	usedUses  uint8
	status    string
}

func (authority ChannelControlAuthority) LocalPeerID() model.PeerID { return authority.localPeerID }
func (authority ChannelControlAuthority) Channels() []ChannelControlChannel {
	return append([]ChannelControlChannel(nil), authority.channels...)
}
func (channel ChannelControlChannel) Channel() model.Channel       { return channel.mesh.Channel() }
func (channel ChannelControlChannel) Roster() model.VerifiedRoster { return channel.mesh.Roster() }
func (channel ChannelControlChannel) Bindings() []model.PeerBinding {
	return channel.mesh.Bindings()
}
func (channel ChannelControlChannel) SelectorBindings() []model.PeerBinding {
	return append([]model.PeerBinding(nil), channel.selectorBindings...)
}
func (channel ChannelControlChannel) OpenGrant() (ChannelControlGrant, bool) {
	if channel.openGrant == nil {
		return ChannelControlGrant{}, false
	}
	return *channel.openGrant, true
}
func (grant ChannelControlGrant) ID() model.GrantID    { return grant.id }
func (grant ChannelControlGrant) ExpiresAt() time.Time { return grant.expiresAt }
func (grant ChannelControlGrant) MaxUses() uint8       { return grant.maxUses }
func (grant ChannelControlGrant) UsedUses() uint8      { return grant.usedUses }
func (grant ChannelControlGrant) Status() string       { return grant.status }

func (s *Store) ReadChannelControlAuthority(ctx context.Context) (ChannelControlAuthority, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ChannelControlAuthority{}, fmt.Errorf("%w: Store is unavailable", ErrChannelControlAuthority)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelControlAuthority{}, fmt.Errorf("%w: begin: %v", ErrChannelControlAuthority, err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelControlAuthority{}, fmt.Errorf("%w: read Node: %v", ErrChannelControlAuthority, err)
	}
	ids, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return ChannelControlAuthority{}, fmt.Errorf("%w: %v", ErrChannelControlAuthority, err)
	}
	channels := make([]ChannelControlChannel, 0, len(ids))
	for _, id := range ids {
		channel, err := readChannelControlChannel(ctx, tx, node.PeerID(), id)
		if err != nil {
			return ChannelControlAuthority{}, err
		}
		channels = append(channels, channel)
	}
	if err := tx.Commit(); err != nil {
		return ChannelControlAuthority{}, fmt.Errorf("%w: commit read: %v", ErrChannelControlAuthority, err)
	}
	return ChannelControlAuthority{localPeerID: node.PeerID(), channels: channels}, nil
}

func readChannelControlChannel(ctx context.Context, tx *sql.Tx, local model.PeerID,
	id model.ChannelID,
) (ChannelControlChannel, error) {
	verified, err := readVerifiedChannelAuthority(ctx, tx, local, id)
	if err != nil {
		return ChannelControlChannel{}, fmt.Errorf("%w: Channel %q: %v",
			ErrChannelControlAuthority, id.String(), err)
	}
	bindings := make([]model.PeerBinding, 0, len(verified.bindings))
	for _, binding := range verified.bindings {
		if binding.State() != model.BindingRevoked {
			bindings = append(bindings, binding)
		}
	}
	result := ChannelControlChannel{mesh: ChannelMeshChannel{channel: verified.channel,
		roster: verified.roster, bindings: bindings},
		selectorBindings: append([]model.PeerBinding(nil), verified.bindings...)}
	grant, err := readOpenEnrollmentGrant(ctx, tx, id)
	if err == nil {
		result.openGrant = &ChannelControlGrant{id: grant.id, expiresAt: grant.expiresAt,
			maxUses: grant.maxUses, usedUses: grant.usedUses, status: grant.status}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ChannelControlChannel{}, fmt.Errorf("%w: Channel %q grant: %v",
			ErrChannelControlAuthority, id.String(), err)
	}
	return result, nil
}
