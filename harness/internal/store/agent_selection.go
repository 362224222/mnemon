package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrAgentOfferCandidatesInput     = errors.New("invalid Agent offer candidate read input")
	ErrAgentOfferCandidatesAuthority = errors.New("Agent offer candidate authority is unavailable")
	ErrAgentOfferCandidatesInvariant = errors.New("Agent offer candidate durable invariant violated")
)

var agentOfferRequiredProtocols = [...]string{
	"/mnemon/artifacts/1",
	"/mnemon/channel/1",
	"/mnemon/events/1",
}

// AgentOfferCandidateReviewer is an internal trusted read model. It contains
// durable identity because the Agent selector must resolve aliases to exact
// PeerIDs, but it is never the identity-free prompt projection.
type AgentOfferCandidateReviewer struct {
	peerID         model.PeerID
	effectiveAlias string
	reachability   model.Reachability
	eligible       bool
}

func (r AgentOfferCandidateReviewer) PeerID() model.PeerID             { return r.peerID }
func (r AgentOfferCandidateReviewer) EffectiveAlias() string           { return r.effectiveAlias }
func (r AgentOfferCandidateReviewer) Reachability() model.Reachability { return r.reachability }
func (r AgentOfferCandidateReviewer) Eligible() bool                   { return r.eligible }

type AgentOfferCandidateChannel struct {
	channelID  model.ChannelID
	localAlias string
	rosterHead model.RecordHead
	reviewers  []AgentOfferCandidateReviewer
}

func (c AgentOfferCandidateChannel) ChannelID() model.ChannelID   { return c.channelID }
func (c AgentOfferCandidateChannel) LocalAlias() string           { return c.localAlias }
func (c AgentOfferCandidateChannel) RosterHead() model.RecordHead { return c.rosterHead }
func (c AgentOfferCandidateChannel) Reviewers() []AgentOfferCandidateReviewer {
	return append([]AgentOfferCandidateReviewer(nil), c.reviewers...)
}

type AgentOfferCandidates struct {
	channels []AgentOfferCandidateChannel
}

func (c AgentOfferCandidates) Channels() []AgentOfferCandidateChannel {
	return append([]AgentOfferCandidateChannel(nil), c.channels...)
}

// AgentInitiationParticipant is the identity-free participant projection
// exposed before an offer is admitted.
type AgentInitiationParticipant struct {
	effectiveAlias string
	reachability   model.Reachability
	eligible       bool
}

func (p AgentInitiationParticipant) EffectiveAlias() string           { return p.effectiveAlias }
func (p AgentInitiationParticipant) Reachability() model.Reachability { return p.reachability }
func (p AgentInitiationParticipant) Reachable() bool {
	return p.reachability == model.ReachabilityReachable
}
func (p AgentInitiationParticipant) Eligible() bool { return p.eligible }

// AgentInitiationChannel intentionally has no ChannelID, topic, roster head,
// or remote identity accessor.
type AgentInitiationChannel struct {
	localAlias   string
	participants []AgentInitiationParticipant
	allowTeam    bool
}

func (c AgentInitiationChannel) LocalAlias() string { return c.localAlias }
func (c AgentInitiationChannel) Participants() []AgentInitiationParticipant {
	return append([]AgentInitiationParticipant(nil), c.participants...)
}
func (c AgentInitiationChannel) AllowTeam() bool { return c.allowTeam }

type AgentInitiationContext struct {
	channels []AgentInitiationChannel
}

func (c AgentInitiationContext) Channels() []AgentInitiationChannel {
	return append([]AgentInitiationChannel(nil), c.channels...)
}

// CanonicalJSON returns the closed, identity-free initiation projection used
// by an explicit Agent current call when no Handling is claimed.
func (c AgentInitiationContext) CanonicalJSON() (model.JSON, error) {
	type participantWire struct {
		EffectiveAlias string `json:"effective_alias"`
		Eligible       bool   `json:"eligible"`
		Reachable      bool   `json:"reachable"`
	}
	type channelWire struct {
		AllowTeam    bool              `json:"allow_team"`
		LocalAlias   string            `json:"local_alias"`
		Participants []participantWire `json:"participants"`
	}
	channels := make([]channelWire, len(c.channels))
	for index, channel := range c.channels {
		participants := channel.Participants()
		rows := make([]participantWire, len(participants))
		for participantIndex, participant := range participants {
			rows[participantIndex] = participantWire{EffectiveAlias: participant.EffectiveAlias(),
				Eligible: participant.Eligible(), Reachable: participant.Reachable()}
		}
		channels[index] = channelWire{AllowTeam: channel.AllowTeam(), LocalAlias: channel.LocalAlias(),
			Participants: rows}
	}
	return model.JSONFrom(struct {
		InitiationContext struct {
			Channels []channelWire `json:"channels"`
		} `json:"initiation_context"`
		SchemaVersion int `json:"schema_version"`
	}{InitiationContext: struct {
		Channels []channelWire `json:"channels"`
	}{Channels: channels}, SchemaVersion: model.SchemaVersion})
}

// ReadAgentOfferCandidates returns one authoritative SQLite snapshot for the
// Agent layer to resolve. It performs no selector interpretation and no
// transport-specific identity decoding or ordering.
func (s *Store) ReadAgentOfferCandidates(ctx context.Context, authenticated model.Profile,
	at time.Time,
) (AgentOfferCandidates, error) {
	trustedAt, err := validateAgentOfferCandidateRead(s, ctx, authenticated, at)
	if err != nil {
		return AgentOfferCandidates{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentOfferCandidates{}, fmt.Errorf("read Agent offer candidates: begin: %w", err)
	}
	defer tx.Rollback()

	_, channels, err := readAgentOfferCandidateSnapshot(ctx, tx, authenticated, trustedAt)
	if err != nil {
		return AgentOfferCandidates{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentOfferCandidates{}, fmt.Errorf("read Agent offer candidates: commit read: %w", err)
	}
	return AgentOfferCandidates{channels: channels}, nil
}

// ReadAgentInitiationContext derives the bounded prompt-safe view from the
// same candidate snapshot rules. Offer admission must read candidates again;
// this view is never authority.
func (s *Store) ReadAgentInitiationContext(ctx context.Context, authenticated model.Profile,
	at time.Time,
) (AgentInitiationContext, error) {
	trustedAt, err := validateAgentOfferCandidateRead(s, ctx, authenticated, at)
	if err != nil {
		return AgentInitiationContext{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentInitiationContext{}, fmt.Errorf("read Agent initiation context: begin: %w", err)
	}
	defer tx.Rollback()

	_, channels, err := readAgentOfferCandidateSnapshot(ctx, tx, authenticated, trustedAt)
	if err != nil {
		return AgentInitiationContext{}, err
	}
	projection := make([]AgentInitiationChannel, len(channels))
	for index, channel := range channels {
		reviewers := channel.Reviewers()
		participants := make([]AgentInitiationParticipant, len(reviewers))
		allowTeam := false
		for participantIndex, reviewer := range reviewers {
			if reviewer.EffectiveAlias() == "auto" || reviewer.EffectiveAlias() == "team" {
				return AgentInitiationContext{}, fmt.Errorf("%w: reserved effective alias in initiation projection",
					ErrAgentOfferCandidatesInvariant)
			}
			participants[participantIndex] = AgentInitiationParticipant{
				effectiveAlias: reviewer.EffectiveAlias(), reachability: reviewer.Reachability(),
				eligible: reviewer.Eligible(),
			}
			allowTeam = allowTeam || reviewer.Eligible()
		}
		projection[index] = AgentInitiationChannel{localAlias: channel.LocalAlias(),
			participants: participants, allowTeam: allowTeam}
	}
	if err := tx.Commit(); err != nil {
		return AgentInitiationContext{}, fmt.Errorf("read Agent initiation context: commit read: %w", err)
	}
	return AgentInitiationContext{channels: projection}, nil
}

func validateAgentOfferCandidateRead(s *Store, ctx context.Context, authenticated model.Profile,
	at time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || authenticated.ID().IsZero() {
		return time.Time{}, fmt.Errorf("%w: nil store/context or zero Profile", ErrAgentOfferCandidatesInput)
	}
	trustedAt, err := canonicalStoreTime(at)
	if err != nil || trustedAt.IsZero() {
		return time.Time{}, fmt.Errorf("%w: invalid trusted time", ErrAgentOfferCandidatesInput)
	}
	return trustedAt, nil
}

func readAgentOfferCandidateSnapshot(ctx context.Context, tx *sql.Tx, authenticated model.Profile,
	at time.Time,
) (model.Node, []AgentOfferCandidateChannel, error) {
	profile, err := requireAuthenticatedManagedProfile(ctx, tx, authenticated)
	if err != nil {
		return model.Node{}, nil, fmt.Errorf("%w: %v", ErrAgentOfferCandidatesAuthority, err)
	}
	if err := requireActiveManagedProfile(ctx, tx, profile, at); err != nil {
		return model.Node{}, nil, fmt.Errorf("%w: %v", ErrAgentOfferCandidatesAuthority, err)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.Node{}, nil, fmt.Errorf("%w: read Node: %v", ErrAgentOfferCandidatesAuthority, err)
	}

	rows, err := tx.QueryContext(ctx, `SELECT channel_id,local_alias,member_limit,roster_head_revision,
		roster_head_hash,created_at,updated_at FROM channels
		WHERE status='active' AND topic_state='joined' ORDER BY local_alias`)
	if err != nil {
		return model.Node{}, nil, fmt.Errorf("%w: read active Channels: %v", ErrAgentOfferCandidatesInvariant, err)
	}
	type durableChannel struct {
		idText, alias, createdText, updatedText string
		memberLimit, rosterRevision             uint64
		rosterHash                              []byte
	}
	durable := make([]durableChannel, 0, model.MaxChannelsPerNode)
	for rows.Next() {
		var channel durableChannel
		if err := rows.Scan(&channel.idText, &channel.alias, &channel.memberLimit,
			&channel.rosterRevision, &channel.rosterHash, &channel.createdText, &channel.updatedText); err != nil {
			rows.Close()
			return model.Node{}, nil, fmt.Errorf("%w: scan active Channel: %v", ErrAgentOfferCandidatesInvariant, err)
		}
		durable = append(durable, channel)
		if len(durable) > model.MaxChannelsPerNode {
			rows.Close()
			return model.Node{}, nil, fmt.Errorf("%w: more than %d active Channels",
				ErrAgentOfferCandidatesInvariant, model.MaxChannelsPerNode)
		}
	}
	if err := rows.Close(); err != nil {
		return model.Node{}, nil, fmt.Errorf("%w: close active Channel rows: %v", ErrAgentOfferCandidatesInvariant, err)
	}
	if err := rows.Err(); err != nil {
		return model.Node{}, nil, fmt.Errorf("%w: iterate active Channels: %v", ErrAgentOfferCandidatesInvariant, err)
	}

	channels := make([]AgentOfferCandidateChannel, 0, len(durable))
	for _, raw := range durable {
		channel, err := readAgentOfferCandidateChannel(ctx, tx, node, at, raw.idText, raw.alias,
			raw.memberLimit, raw.rosterRevision, raw.rosterHash, raw.createdText, raw.updatedText)
		if err != nil {
			return model.Node{}, nil, err
		}
		channels = append(channels, channel)
	}
	return node, channels, nil
}

func readAgentOfferCandidateChannel(ctx context.Context, tx *sql.Tx, node model.Node, at time.Time,
	idText, alias string, memberLimit, rosterRevision uint64, rosterHash []byte,
	createdText, updatedText string,
) (AgentOfferCandidateChannel, error) {
	if err := validateDurableAgentAlias(alias); err != nil {
		return AgentOfferCandidateChannel{}, err
	}
	if memberLimit < 2 || memberLimit > model.MaxMembersPerChannel {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q member limit is out of range",
			ErrAgentOfferCandidatesInvariant, alias)
	}
	channelID, err := model.ParseChannelID(idText)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q ID: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q signed authority: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	digest, err := model.DigestFromBytes(rosterHash)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q roster digest: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	rosterHead, err := model.NewRecordHead(rosterRevision, digest)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q roster head: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	if authority.channel.LocalAlias() != alias || authority.channel.MemberLimit() != uint8(memberLimit) ||
		authority.channel.RosterHead() != rosterHead {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q query projection changed",
			ErrAgentOfferCandidatesInvariant, alias)
	}
	createdAt, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q created_at: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil || updatedAt.Before(createdAt) {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q updated_at is invalid",
			ErrAgentOfferCandidatesInvariant, alias)
	}
	if createdAt.After(at) || updatedAt.After(at) {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q is ahead of trusted time",
			ErrAgentOfferCandidatesAuthority, alias)
	}

	var latestRoster uint64
	var latestRosterHash []byte
	var latestRosterCreatedText string
	if err := tx.QueryRowContext(ctx, `SELECT revision,record_hash,created_at FROM channel_members
		WHERE channel_id=? ORDER BY revision DESC LIMIT 1`, channelID.String()).
		Scan(&latestRoster, &latestRosterHash, &latestRosterCreatedText); err != nil ||
		latestRoster != rosterHead.Revision() || !bytes.Equal(latestRosterHash, rosterHead.Digest().Bytes()) {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q roster head is not latest",
			ErrAgentOfferCandidatesInvariant, alias)
	}
	latestRosterCreatedAt, err := parseCanonicalStoreTime(latestRosterCreatedText)
	if err != nil || latestRosterCreatedAt.After(updatedAt) || latestRosterCreatedAt.After(at) {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q latest roster time is invalid",
			ErrAgentOfferCandidatesInvariant, alias)
	}

	var localRevision uint64
	var localRecordHash []byte
	var localEpoch, localStatus, localCreatedText string
	err = tx.QueryRowContext(ctx, `SELECT revision,record_hash,origin_epoch,status,created_at FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, channelID.String(),
		node.PeerID().String()).Scan(&localRevision, &localRecordHash, &localEpoch, &localStatus, &localCreatedText)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q latest local membership: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	if _, err := model.DigestFromBytes(localRecordHash); err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q local member digest is invalid",
			ErrAgentOfferCandidatesInvariant, alias)
	}
	localCreatedAt, err := parseCanonicalStoreTime(localCreatedText)
	if err != nil || localCreatedAt.After(at) || localRevision > rosterHead.Revision() ||
		localEpoch != node.OriginEpoch().String() || localStatus != string(model.MemberActive) {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q local membership is not latest active authority",
			ErrAgentOfferCandidatesInvariant, alias)
	}

	var activeMembers uint64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_members current
		WHERE current.channel_id=? AND current.status='active' AND current.revision=(
			SELECT MAX(latest.revision) FROM channel_members latest
			WHERE latest.channel_id=current.channel_id AND latest.member_peer_id=current.member_peer_id
		)`, channelID.String()).Scan(&activeMembers); err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q active membership count: %v",
			ErrAgentOfferCandidatesInvariant, alias, err)
	}
	if activeMembers == 0 || activeMembers > memberLimit || activeMembers > model.MaxMembersPerChannel {
		return AgentOfferCandidateChannel{}, fmt.Errorf("%w: Channel %q active membership exceeds its limit",
			ErrAgentOfferCandidatesInvariant, alias)
	}

	reviewers, err := readAgentOfferCandidateReviewers(ctx, tx, node, channelID,
		authority.channel, authority.roster, rosterHead, at)
	if err != nil {
		return AgentOfferCandidateChannel{}, fmt.Errorf("Channel %q: %w", alias, err)
	}
	return AgentOfferCandidateChannel{channelID: channelID, localAlias: alias,
		rosterHead: rosterHead, reviewers: reviewers}, nil
}

func readAgentOfferCandidateReviewers(ctx context.Context, tx *sql.Tx, node model.Node,
	channelID model.ChannelID, channel model.Channel, verifiedRoster model.VerifiedRoster,
	roster model.RecordHead, at time.Time,
) ([]AgentOfferCandidateReviewer, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.member_peer_id,m.origin_epoch,m.revision,m.record_hash,
		m.public_key,m.created_at,b.effective_alias,b.multiaddrs_json,b.protocols_json,b.limits_json,
		b.reachability,b.joined_at,b.last_seen_at,
		(SELECT updated_at FROM peer_cursors c WHERE c.channel_id=b.channel_id
			AND c.origin_peer_id=b.peer_id AND c.origin_epoch=b.origin_epoch),
		a.baseline_confirmed_at,a.updated_at
	FROM channel_members m
	JOIN peer_bindings b ON b.channel_id=m.channel_id AND b.peer_id=m.member_peer_id
		AND b.origin_epoch=m.origin_epoch AND b.public_key=m.public_key
		AND b.member_revision=m.revision AND b.member_record_hash=m.record_hash AND b.state='active'
	LEFT JOIN peer_pull_acks a ON a.channel_id=b.channel_id AND a.target_peer_id=b.peer_id
		AND a.origin_peer_id=? AND a.origin_epoch=?
	WHERE m.channel_id=? AND m.member_peer_id<>? AND m.status='active'
		AND m.revision=(SELECT MAX(latest.revision) FROM channel_members latest
			WHERE latest.channel_id=m.channel_id AND latest.member_peer_id=m.member_peer_id)`,
		node.PeerID().String(), node.OriginEpoch().String(), channelID.String(), node.PeerID().String())
	if err != nil {
		return nil, fmt.Errorf("%w: read active reviewer bindings: %v", ErrAgentOfferCandidatesInvariant, err)
	}
	defer rows.Close()

	reviewers := make([]AgentOfferCandidateReviewer, 0, model.MaxChildWorks)
	seenPeers := make(map[string]struct{}, model.MaxChildWorks)
	seenAliases := make(map[string]struct{}, model.MaxChildWorks)
	for rows.Next() {
		var peerText, epochText, memberCreatedText, alias, reachabilityText, joinedText string
		var memberRevision uint64
		var memberHash, publicKey, multiaddrsRaw, protocolsRaw, limitsRaw []byte
		var lastSeenText, cursorUpdatedText, confirmedText, ackUpdatedText sql.NullString
		if err := rows.Scan(&peerText, &epochText, &memberRevision, &memberHash, &publicKey,
			&memberCreatedText, &alias, &multiaddrsRaw, &protocolsRaw, &limitsRaw,
			&reachabilityText, &joinedText, &lastSeenText, &cursorUpdatedText,
			&confirmedText, &ackUpdatedText); err != nil {
			return nil, fmt.Errorf("%w: scan active reviewer binding: %v", ErrAgentOfferCandidatesInvariant, err)
		}
		if len(reviewers) >= model.MaxChildWorks {
			return nil, fmt.Errorf("%w: more than %d active remote bindings",
				ErrAgentOfferCandidatesInvariant, model.MaxChildWorks)
		}
		peerID, err := model.ParsePeerID(peerText)
		if err != nil || peerID == node.PeerID() {
			return nil, fmt.Errorf("%w: remote PeerID is invalid or self", ErrAgentOfferCandidatesInvariant)
		}
		if _, exists := seenPeers[peerID.String()]; exists {
			return nil, fmt.Errorf("%w: duplicate remote PeerID", ErrAgentOfferCandidatesInvariant)
		}
		seenPeers[peerID.String()] = struct{}{}
		if err := validateDurableAgentAlias(alias); err != nil {
			return nil, fmt.Errorf("%w: effective alias %q is invalid",
				ErrAgentOfferCandidatesInvariant, alias)
		}
		if _, exists := seenAliases[alias]; exists {
			return nil, fmt.Errorf("%w: duplicate effective alias %q", ErrAgentOfferCandidatesInvariant, alias)
		}
		seenAliases[alias] = struct{}{}

		memberDigest, err := model.DigestFromBytes(memberHash)
		if err != nil {
			return nil, fmt.Errorf("%w: reviewer member digest: %v", ErrAgentOfferCandidatesInvariant, err)
		}
		memberHead, err := model.NewRecordHead(memberRevision, memberDigest)
		if err != nil || memberRevision > roster.Revision() {
			return nil, fmt.Errorf("%w: reviewer member head is invalid", ErrAgentOfferCandidatesInvariant)
		}
		epoch, err := model.ParseOriginEpoch(epochText)
		if err != nil {
			return nil, fmt.Errorf("%w: reviewer origin epoch: %v", ErrAgentOfferCandidatesInvariant, err)
		}
		memberCreatedAt, err := parseCanonicalStoreTime(memberCreatedText)
		if err != nil || memberCreatedAt.After(at) {
			return nil, fmt.Errorf("%w: reviewer membership is ahead of trusted time", ErrAgentOfferCandidatesAuthority)
		}
		multiaddrs, err := parseCanonicalAgentStringArray("multiaddrs", multiaddrsRaw)
		if err != nil {
			return nil, err
		}
		protocols, err := parseCanonicalAgentStringArray("protocols", protocolsRaw)
		if err != nil {
			return nil, err
		}
		limits, err := model.NewJSON(limitsRaw)
		if err != nil || !bytes.Equal(limits.Bytes(), limitsRaw) || len(limitsRaw) == 0 || limitsRaw[0] != '{' {
			return nil, fmt.Errorf("%w: binding limits are not a canonical JSON object",
				ErrAgentOfferCandidatesInvariant)
		}
		joinedAt, err := parseCanonicalStoreTime(joinedText)
		if err != nil || joinedAt.After(at) {
			return nil, fmt.Errorf("%w: binding join time is invalid", ErrAgentOfferCandidatesAuthority)
		}
		var lastSeen *time.Time
		if lastSeenText.Valid {
			parsed, err := parseCanonicalStoreTime(lastSeenText.String)
			if err != nil || parsed.After(at) {
				return nil, fmt.Errorf("%w: binding last-seen time is invalid", ErrAgentOfferCandidatesAuthority)
			}
			lastSeen = &parsed
		}
		binding, err := model.NewPeerBinding(node.PeerID(), model.PeerBindingSpec{
			Channel: channel, Roster: verifiedRoster,
			PeerID: peerID, EffectiveAlias: alias,
			State:        model.BindingActive,
			Reachability: model.Reachability(reachabilityText), JoinedAt: joinedAt, LastSeenAt: lastSeen,
		})
		if err != nil || binding.OriginEpoch() != epoch || binding.MemberHead() != memberHead ||
			!bytes.Equal(binding.PublicKey(), publicKey) || !equalAgentStrings(binding.Multiaddrs(), multiaddrs) ||
			!equalAgentStrings(binding.Protocols(), protocols) || binding.Limits().String() != limits.String() ||
			binding.RosterHead() != roster {
			return nil, fmt.Errorf("%w: active PeerBinding is not canonical: %v",
				ErrAgentOfferCandidatesInvariant, err)
		}

		protocolReady := bindingSupportsAgentOffer(binding.Protocols())
		cursorReady, err := agentBaselineTimeReady(cursorUpdatedText, at)
		if err != nil {
			return nil, err
		}
		confirmedReady, err := agentBaselineTimeReady(confirmedText, at)
		if err != nil {
			return nil, err
		}
		ackReady, err := agentBaselineTimeReady(ackUpdatedText, at)
		if err != nil {
			return nil, err
		}
		reviewers = append(reviewers, AgentOfferCandidateReviewer{
			peerID: binding.PeerID(), effectiveAlias: binding.EffectiveAlias(),
			reachability: binding.Reachability(),
			eligible:     protocolReady && cursorReady && confirmedReady && ackReady,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate active reviewer bindings: %v", ErrAgentOfferCandidatesInvariant, err)
	}
	// Store ordering exists only to stabilize the identity-free projection.
	// Agent selection must decode and order PeerIDs independently.
	sort.Slice(reviewers, func(left, right int) bool {
		if reviewers[left].EffectiveAlias() == reviewers[right].EffectiveAlias() {
			return reviewers[left].PeerID().String() < reviewers[right].PeerID().String()
		}
		return reviewers[left].EffectiveAlias() < reviewers[right].EffectiveAlias()
	})
	return reviewers, nil
}

func validateDurableAgentAlias(value string) error {
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxIdentifierBytes ||
		strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: durable alias is empty, oversized, or non-canonical",
			ErrAgentOfferCandidatesInvariant)
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return fmt.Errorf("%w: durable alias contains whitespace or a control character",
				ErrAgentOfferCandidatesInvariant)
		}
	}
	return nil
}

func parseCanonicalAgentStringArray(field string, raw []byte) ([]string, error) {
	value, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(value.Bytes(), raw) || len(raw) == 0 || raw[0] != '[' {
		return nil, fmt.Errorf("%w: binding %s are not a canonical JSON array",
			ErrAgentOfferCandidatesInvariant, field)
	}
	var result []string
	if err := json.Unmarshal(raw, &result); err != nil || result == nil {
		return nil, fmt.Errorf("%w: binding %s are not a string array",
			ErrAgentOfferCandidatesInvariant, field)
	}
	for index, item := range result {
		if err := validateDurableAgentAlias(item); err != nil {
			return nil, fmt.Errorf("%w: invalid binding %s item", ErrAgentOfferCandidatesInvariant, field)
		}
		if index > 0 && result[index-1] >= item {
			return nil, fmt.Errorf("%w: binding %s are not sorted and unique",
				ErrAgentOfferCandidatesInvariant, field)
		}
	}
	return result, nil
}

func bindingSupportsAgentOffer(protocols []string) bool {
	for _, required := range agentOfferRequiredProtocols {
		index := sort.SearchStrings(protocols, required)
		if index == len(protocols) || protocols[index] != required {
			return false
		}
	}
	return true
}

func agentBaselineTimeReady(value sql.NullString, at time.Time) (bool, error) {
	if !value.Valid {
		return false, nil
	}
	parsed, err := parseCanonicalStoreTime(value.String)
	if err != nil {
		return false, fmt.Errorf("%w: baseline time is not canonical", ErrAgentOfferCandidatesInvariant)
	}
	if parsed.After(at) {
		return false, fmt.Errorf("%w: baseline is ahead of trusted time", ErrAgentOfferCandidatesAuthority)
	}
	return true, nil
}

func equalAgentStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
