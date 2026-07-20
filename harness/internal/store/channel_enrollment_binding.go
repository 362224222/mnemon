package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func syncChannelRosterBindings(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, existing []model.PeerBinding, at time.Time,
) error {
	aliases, active, err := deriveEffectiveAliases(localPeer, roster)
	if err != nil {
		return err
	}
	occupied, err := resolveRosterBindingAliases(roster, existing, active, aliases)
	if err != nil {
		return err
	}
	staged, err := stageRosterBindingAliases(ctx, tx, channel, roster, existing, aliases, occupied)
	if err != nil {
		return err
	}
	existingByPeer := indexRosterBindings(existing)
	currentMembers := currentRosterMembers(roster)
	for _, peerID := range sortedMemberPeerIDs(currentMembers, localPeer) {
		if err := syncRosterMemberBinding(ctx, tx, localPeer, channel, roster, at, peerID,
			currentMembers[peerID], existingByPeer, aliases, staged); err != nil {
			return err
		}
	}
	return nil
}

func resolveRosterBindingAliases(roster model.VerifiedRoster, existing []model.PeerBinding,
	active map[model.PeerID]model.Member, aliases map[model.PeerID]string,
) (map[string]model.PeerID, error) {
	occupied := make(map[string]model.PeerID, len(existing)+len(active))
	for _, binding := range existing {
		if current, ok := roster.CurrentMember(binding.PeerID()); ok && current.Status().Terminal() {
			occupied[binding.EffectiveAlias()] = binding.PeerID()
		}
	}
	for _, peerID := range sortedMemberPeerIDs(active, model.PeerID{}) {
		alias := aliases[peerID]
		if owner, collision := occupied[alias]; collision && owner != peerID {
			var err error
			alias, err = uniqueEffectiveAlias(sanitizeEffectiveAliasLabel(active[peerID].DisplayLabel()),
				peerID, occupied)
			if err != nil {
				return nil, err
			}
		}
		aliases[peerID] = alias
		occupied[alias] = peerID
	}
	return occupied, nil
}

func stageRosterBindingAliases(ctx context.Context, tx *sql.Tx, channel model.Channel,
	roster model.VerifiedRoster, existing []model.PeerBinding, aliases map[model.PeerID]string,
	occupied map[string]model.PeerID,
) (map[model.PeerID]string, error) {
	unavailable := make(map[string]struct{}, len(existing)*2)
	for _, binding := range existing {
		unavailable[binding.EffectiveAlias()] = struct{}{}
	}
	for _, alias := range aliases {
		unavailable[alias] = struct{}{}
	}
	for alias := range occupied {
		unavailable[alias] = struct{}{}
	}
	staged := make(map[model.PeerID]string)
	for _, binding := range existing {
		current, ok := roster.CurrentMember(binding.PeerID())
		if !ok {
			return nil, ErrChannelJoinConflict
		}
		desired := binding.EffectiveAlias()
		if current.Status() == model.MemberActive {
			desired = aliases[binding.PeerID()]
		}
		if desired == binding.EffectiveAlias() {
			continue
		}
		temporary := temporaryBindingAlias(channel.ID(), binding.PeerID(), unavailable)
		unavailable[temporary] = struct{}{}
		if _, err := tx.ExecContext(ctx, `UPDATE peer_bindings SET effective_alias=?
			WHERE channel_id=? AND peer_id=?`, temporary, channel.ID().String(),
			binding.PeerID().String()); err != nil {
			return nil, fmt.Errorf("stage PeerBinding alias: %w", err)
		}
		staged[binding.PeerID()] = desired
	}
	return staged, nil
}

func syncRosterMemberBinding(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, at time.Time, peerID model.PeerID,
	member model.Member, existing map[model.PeerID]model.PeerBinding,
	aliases, staged map[model.PeerID]string,
) error {
	prior, exists := existing[peerID]
	if !exists {
		if member.Status() != model.MemberActive {
			return nil
		}
		return insertPendingPeerBinding(ctx, tx, localPeer, channel, roster, peerID,
			aliases[peerID], at)
	}
	alias := prior.EffectiveAlias()
	if desired, changed := staged[peerID]; changed {
		alias = desired
	} else if member.Status() == model.MemberActive {
		alias = aliases[peerID]
	}
	state := prior.State()
	if member.Status().Terminal() {
		state = model.BindingRevoked
	}
	binding, err := model.NewPeerBinding(localPeer, model.PeerBindingSpec{Channel: channel,
		Roster: roster, PeerID: peerID, EffectiveAlias: alias, State: state,
		Reachability: prior.Reachability(), JoinedAt: prior.JoinedAt(), LastSeenAt: bindingLastSeen(prior)})
	if err != nil {
		return err
	}
	return updateJoinedPeerBinding(ctx, tx, channel.ID(), binding)
}

func updateJoinedPeerBinding(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	binding model.PeerBinding,
) error {
	multiaddrs, err := model.JSONFrom(binding.Multiaddrs())
	if err != nil {
		return err
	}
	protocols, err := model.JSONFrom(binding.Protocols())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE peer_bindings SET effective_alias=?,public_key=?,
		multiaddrs_json=?,protocols_json=?,limits_json=?,member_revision=?,member_record_hash=?,state=?
		WHERE channel_id=? AND peer_id=?`, binding.EffectiveAlias(), binding.PublicKey(),
		multiaddrs.Bytes(), protocols.Bytes(), binding.Limits().Bytes(), binding.MemberHead().Revision(),
		binding.MemberHead().Digest().Bytes(), string(binding.State()), channelID.String(),
		binding.PeerID().String())
	if err != nil {
		return fmt.Errorf("update joined PeerBinding: %w", err)
	}
	return nil
}

func insertPendingRosterBindings(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, joinedAt time.Time,
) error {
	aliases, members, err := deriveEffectiveAliases(localPeer, roster)
	if err != nil {
		return err
	}
	for _, peerID := range sortedMemberPeerIDs(members, model.PeerID{}) {
		if err := insertPendingPeerBinding(ctx, tx, localPeer, channel, roster, peerID,
			aliases[peerID], joinedAt); err != nil {
			return err
		}
	}
	return nil
}

func insertPendingPeerBinding(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, peerID model.PeerID, alias string,
	joinedAt time.Time,
) error {
	binding, err := model.NewPeerBinding(localPeer, model.PeerBindingSpec{Channel: channel,
		Roster: roster, PeerID: peerID, EffectiveAlias: alias, State: model.BindingPending,
		Reachability: model.ReachabilityUnknown, JoinedAt: joinedAt})
	if err != nil {
		return err
	}
	multiaddrs, err := model.JSONFrom(binding.Multiaddrs())
	if err != nil {
		return err
	}
	protocols, err := model.JSONFrom(binding.Protocols())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,
		effective_alias,public_key,multiaddrs_json,protocols_json,limits_json,member_revision,
		member_record_hash,state,reachability,joined_at,last_seen_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)`, channel.ID().String(), binding.PeerID().String(),
		binding.OriginEpoch().String(), binding.EffectiveAlias(), binding.PublicKey(), multiaddrs.Bytes(),
		protocols.Bytes(), binding.Limits().Bytes(), binding.MemberHead().Revision(),
		binding.MemberHead().Digest().Bytes(), string(binding.State()), string(binding.Reachability()),
		storeTime(binding.JoinedAt()))
	if err != nil {
		return fmt.Errorf("insert pending PeerBinding: %w", err)
	}
	return nil
}

func indexRosterBindings(bindings []model.PeerBinding) map[model.PeerID]model.PeerBinding {
	indexed := make(map[model.PeerID]model.PeerBinding, len(bindings))
	for _, binding := range bindings {
		indexed[binding.PeerID()] = binding
	}
	return indexed
}

func sortedMemberPeerIDs(members map[model.PeerID]model.Member,
	excluded model.PeerID,
) []model.PeerID {
	peers := make([]model.PeerID, 0, len(members))
	for peerID := range members {
		if peerID != excluded {
			peers = append(peers, peerID)
		}
	}
	sort.Slice(peers, func(left, right int) bool { return peers[left].String() < peers[right].String() })
	return peers
}

func bindingLastSeen(binding model.PeerBinding) *time.Time {
	value, ok := binding.LastSeenAt()
	if !ok {
		return nil
	}
	return &value
}
