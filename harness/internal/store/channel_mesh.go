package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelMeshAuthority = errors.New("durable Channel mesh authority is unavailable")

// ChannelMeshAuthority is one immutable, whole-Node view of durable Channel
// authority. It deliberately contains no transport or runtime state: those
// projections are derived by their owning packages from this Store snapshot.
type ChannelMeshAuthority struct {
	localPeerID model.PeerID
	channels    []ChannelMeshChannel
}

// ChannelMeshChannel couples one reconstructed Channel with its complete
// verified roster and only its currently live PeerBindings. Terminal binding
// history remains represented by the signed roster instead of consuming the
// bounded runtime binding projection.
type ChannelMeshChannel struct {
	channel  model.Channel
	roster   model.VerifiedRoster
	bindings []model.PeerBinding
}

func (authority ChannelMeshAuthority) LocalPeerID() model.PeerID {
	return authority.localPeerID
}

func (authority ChannelMeshAuthority) Channels() []ChannelMeshChannel {
	return append([]ChannelMeshChannel(nil), authority.channels...)
}

func (channel ChannelMeshChannel) Channel() model.Channel {
	return channel.channel
}

func (channel ChannelMeshChannel) Roster() model.VerifiedRoster {
	return channel.roster
}

func (channel ChannelMeshChannel) Bindings() []model.PeerBinding {
	return append([]model.PeerBinding(nil), channel.bindings...)
}

// ReadChannelMeshAuthority reconstructs every durable Channel from signed
// evidence in one read-only SQLite transaction. Consumers therefore receive
// one coherent Store revision and never a mixture of independently read
// Channel heads.
func (s *Store) ReadChannelMeshAuthority(ctx context.Context) (ChannelMeshAuthority, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ChannelMeshAuthority{}, fmt.Errorf("%w: Store is unavailable", ErrChannelMeshAuthority)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelMeshAuthority{}, fmt.Errorf("%w: begin: %v", ErrChannelMeshAuthority, err)
	}
	defer tx.Rollback()

	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelMeshAuthority{}, fmt.Errorf("%w: read Node: %v", ErrChannelMeshAuthority, err)
	}
	channelIDs, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return ChannelMeshAuthority{}, err
	}

	channels := make([]ChannelMeshChannel, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		verified, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
		if err != nil {
			return ChannelMeshAuthority{}, fmt.Errorf("%w: Channel %q: %w",
				ErrChannelMeshAuthority, channelID.String(), err)
		}
		projected, err := projectChannelMeshChannel(channelID, verified)
		if err != nil {
			return ChannelMeshAuthority{}, err
		}
		channels = append(channels, projected)
	}
	if err := tx.Commit(); err != nil {
		return ChannelMeshAuthority{}, fmt.Errorf("%w: commit read: %v", ErrChannelMeshAuthority, err)
	}
	return ChannelMeshAuthority{localPeerID: node.PeerID(), channels: channels}, nil
}

func projectChannelMeshChannel(channelID model.ChannelID,
	verified verifiedChannelAuthority,
) (ChannelMeshChannel, error) {
	liveBindings := make([]model.PeerBinding, 0, len(verified.bindings))
	for _, binding := range verified.bindings {
		switch binding.State() {
		case model.BindingPending, model.BindingActive:
			liveBindings = append(liveBindings, binding)
		case model.BindingRevoked:
			// The complete verified roster retains the terminal evidence.
		default:
			return ChannelMeshChannel{}, fmt.Errorf("%w: Channel %q has unknown binding state",
				ErrChannelMeshAuthority, channelID.String())
		}
	}
	return ChannelMeshChannel{channel: verified.channel,
		roster: verified.roster, bindings: liveBindings}, nil
}

func readChannelMeshIDs(ctx context.Context, tx *sql.Tx) ([]model.ChannelID, error) {
	rows, err := tx.QueryContext(ctx, "SELECT channel_id FROM channels ORDER BY channel_id")
	if err != nil {
		return nil, fmt.Errorf("%w: list Channels: %v", ErrChannelMeshAuthority, err)
	}
	defer rows.Close()
	var result []model.ChannelID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("%w: scan Channel ID: %v", ErrChannelMeshAuthority, err)
		}
		channelID, err := model.ParseChannelID(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: parse Channel ID %q: %v", ErrChannelMeshAuthority, raw, err)
		}
		result = append(result, channelID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate Channels: %v", ErrChannelMeshAuthority, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("%w: close Channel list: %v", ErrChannelMeshAuthority, err)
	}
	return result, nil
}
