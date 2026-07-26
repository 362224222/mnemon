package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (s *Store) ReadChannelObservation(ctx context.Context) (ChannelObservation, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ChannelObservation{}, fmt.Errorf("%w: Store is unavailable", ErrChannelStatusAuthority)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelObservation{}, fmt.Errorf("%w: begin: %v", ErrChannelStatusAuthority, err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelObservation{}, fmt.Errorf("%w: read Node: %v", ErrChannelStatusAuthority, err)
	}
	ids, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return ChannelObservation{}, fmt.Errorf("%w: %v", ErrChannelStatusAuthority, err)
	}
	channels := make([]ChannelObservationChannel, 0, len(ids))
	for _, id := range ids {
		control, err := readChannelControlChannel(ctx, tx, node.PeerID(), id)
		if err != nil {
			return ChannelObservation{}, fmt.Errorf("%w: %v", ErrChannelStatusAuthority, err)
		}
		members := control.Roster().Members()
		if len(members) == 0 || members[len(members)-1].Head() != control.Channel().RosterHead() {
			return ChannelObservation{}, fmt.Errorf("%w: Channel %q roster head is unavailable",
				ErrChannelStatusAuthority, id.String())
		}
		progress, err := readChannelStatusProgress(ctx, tx, node, control)
		if err != nil {
			return ChannelObservation{}, err
		}
		head := members[len(members)-1]
		channels = append(channels, ChannelObservationChannel{control: control,
			channelIDDigest: model.Sum([]byte(id.String())),
			rosterHead: ChannelObservationRosterHead{recordHead: head.Head(),
				ownerPeerID: control.Channel().OwnerPeerID(), ownerSignature: head.OwnerSignature()},
			progress: progress})
	}
	if err := tx.Commit(); err != nil {
		return ChannelObservation{}, fmt.Errorf("%w: commit read: %v", ErrChannelStatusAuthority, err)
	}
	return ChannelObservation{localPeerID: node.PeerID(), channels: channels}, nil
}
