package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelAuthorityInvariant = errors.New("durable Channel authority is invalid")

const maxVerifiedChannelConflicts = 8

type verifiedChannelAuthority struct {
	channel  model.Channel
	roster   model.VerifiedRoster
	bindings []model.PeerBinding
}

// readVerifiedChannelAuthority reconstructs one Channel only from canonical
// signed evidence and then checks every relational projection against it. It
// must run inside the same SQLite snapshot as its consumer.
func readVerifiedChannelAuthority(ctx context.Context, tx *sql.Tx,
	localPeer model.PeerID, channelID model.ChannelID,
) (verifiedChannelAuthority, error) {
	if ctx == nil || tx == nil || localPeer.IsZero() || channelID.IsZero() {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: incomplete read input", ErrChannelAuthorityInvariant)
	}
	var idText, name, localAlias, ownerText, statusText, topicText, createdText, updatedText string
	var ownerKey, descriptorRaw, descriptorDigest, descriptorSignature, rosterDigest []byte
	var memberLimit uint8
	var rosterRevision uint64
	err := tx.QueryRowContext(ctx, `SELECT channel_id,name,local_alias,owner_peer_id,owner_public_key,
		descriptor_json,descriptor_digest,descriptor_signature,member_limit,roster_head_revision,
		roster_head_hash,status,topic_state,created_at,updated_at FROM channels WHERE channel_id=?`,
		channelID.String()).Scan(&idText, &name, &localAlias, &ownerText, &ownerKey, &descriptorRaw,
		&descriptorDigest, &descriptorSignature, &memberLimit, &rosterRevision, &rosterDigest,
		&statusText, &topicText, &createdText, &updatedText)
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: read Channel %q: %v",
			ErrChannelAuthorityInvariant, channelID.String(), err)
	}
	descriptor, err := model.ParseChannelDescriptor(descriptorRaw)
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: parse descriptor: %v", ErrChannelAuthorityInvariant, err)
	}
	signedDescriptor, err := model.AttachChannelDescriptorSignature(descriptor, descriptorSignature)
	if err != nil || idText != descriptor.ID().String() || idText != channelID.String() ||
		name != descriptor.Name() || ownerText != descriptor.OwnerPeerID().String() ||
		!bytes.Equal(ownerKey, descriptor.OwnerPublicKey()) || memberLimit != descriptor.MemberLimit() ||
		createdText != storeTime(descriptor.CreatedAt()) ||
		!bytes.Equal(descriptorDigest, descriptor.Digest().Bytes()) {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: descriptor projection mismatch: %v",
			ErrChannelAuthorityInvariant, err)
	}
	rosterHash, err := model.DigestFromBytes(rosterDigest)
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: roster digest: %v", ErrChannelAuthorityInvariant, err)
	}
	rosterHead, err := model.NewRecordHead(rosterRevision, rosterHash)
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: roster head: %v", ErrChannelAuthorityInvariant, err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: updated_at: %v", ErrChannelAuthorityInvariant, err)
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: signedDescriptor, LocalAlias: localAlias,
		RosterHead: rosterHead, Status: model.ChannelStatus(statusText), TopicState: model.TopicState(topicText),
		UpdatedAt: updatedAt})
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: Channel projection: %v", ErrChannelAuthorityInvariant, err)
	}

	rows, err := tx.QueryContext(ctx, `SELECT revision,record_hash,previous_hash,member_peer_id,
		origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,status,
		signed_record_json,owner_signature,created_at FROM channel_members
		WHERE channel_id=? ORDER BY revision`, channelID.String())
	if err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: read roster: %v", ErrChannelAuthorityInvariant, err)
	}
	defer rows.Close()
	// rosterRevision is durable input, not a safe allocation hint. A sparse or
	// corrupt projection must fail during verification without inducing a large
	// allocation first.
	var members []model.Member
	for rows.Next() {
		var revision uint64
		var recordDigest, previousDigest, publicKey, multiaddrsRaw, protocolsRaw, limitsRaw []byte
		var peerText, epochText, label, memberStatus, memberCreatedText string
		var nullablePrevious []byte
		var recordRaw, signature []byte
		if err := rows.Scan(&revision, &recordDigest, &nullablePrevious, &peerText, &epochText, &label,
			&publicKey, &multiaddrsRaw, &protocolsRaw, &limitsRaw, &memberStatus, &recordRaw,
			&signature, &memberCreatedText); err != nil {
			return verifiedChannelAuthority{}, fmt.Errorf("%w: scan roster: %v", ErrChannelAuthorityInvariant, err)
		}
		previousDigest = nullablePrevious
		record, err := model.ParseMemberRecord(recordRaw)
		if err != nil {
			return verifiedChannelAuthority{}, fmt.Errorf("%w: parse MemberRecord revision %d: %v",
				ErrChannelAuthorityInvariant, revision, err)
		}
		member, err := model.AttachMemberSignature(record, signature)
		if err != nil {
			return verifiedChannelAuthority{}, fmt.Errorf("%w: attach MemberRecord revision %d: %v",
				ErrChannelAuthorityInvariant, revision, err)
		}
		multiaddrs, err := parseCanonicalAgentStringArray("member multiaddrs", multiaddrsRaw)
		if err != nil {
			return verifiedChannelAuthority{}, err
		}
		protocols, err := parseCanonicalAgentStringArray("member protocols", protocolsRaw)
		if err != nil {
			return verifiedChannelAuthority{}, err
		}
		limits, err := model.NewJSON(limitsRaw)
		if err != nil || !bytes.Equal(limits.Bytes(), limitsRaw) {
			return verifiedChannelAuthority{}, fmt.Errorf("%w: noncanonical member limits", ErrChannelAuthorityInvariant)
		}
		storedPrevious, hasStoredPrevious := record.PreviousDigest()
		if (hasStoredPrevious && !bytes.Equal(previousDigest, storedPrevious.Bytes())) ||
			(!hasStoredPrevious && len(previousDigest) != 0) || revision != record.Head().Revision() ||
			!bytes.Equal(recordDigest, record.Digest().Bytes()) || peerText != record.PeerID().String() ||
			epochText != record.OriginEpoch().String() || label != record.DisplayLabel() ||
			!bytes.Equal(publicKey, record.PublicKey()) || !equalAgentStrings(multiaddrs, record.Multiaddrs()) ||
			!equalAgentStrings(protocols, record.Protocols()) || limits.String() != record.Limits().String() ||
			memberStatus != string(record.Status()) || memberCreatedText != storeTime(record.CreatedAt()) {
			return verifiedChannelAuthority{}, fmt.Errorf("%w: MemberRecord revision %d projection mismatch",
				ErrChannelAuthorityInvariant, revision)
		}
		members = append(members, member)
		if uint64(len(members)) > rosterRevision {
			return verifiedChannelAuthority{}, fmt.Errorf("%w: roster exceeds committed head", ErrChannelAuthorityInvariant)
		}
	}
	if err := rows.Err(); err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: iterate roster: %v", ErrChannelAuthorityInvariant, err)
	}
	if err := rows.Close(); err != nil {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: close roster: %v", ErrChannelAuthorityInvariant, err)
	}
	roster, err := model.NewVerifiedRoster(signedDescriptor, members)
	if err != nil || roster.Head() != channel.RosterHead() || uint64(len(members)) != rosterRevision {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: roster does not equal committed head: %v",
			ErrChannelAuthorityInvariant, err)
	}
	if channel.UpdatedAt().Before(members[len(members)-1].CreatedAt()) {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: Channel update time precedes its roster head",
			ErrChannelAuthorityInvariant)
	}
	conflictCount, err := readVerifiedChannelConflicts(ctx, tx, channel, roster)
	if err != nil || (channel.Status() == model.ChannelConflicted && conflictCount == 0) ||
		(conflictCount > 0 && channel.Status() != model.ChannelConflicted &&
			channel.Status() != model.ChannelAbandoned) {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: Channel conflict evidence and status disagree: %v",
			ErrChannelAuthorityInvariant, err)
	}
	owner, ok := roster.CurrentMember(channel.OwnerPeerID())
	if !ok || (channel.Status() == model.ChannelClosed && owner.Status() != model.MemberLeft) ||
		((channel.Status() == model.ChannelActive || channel.Status() == model.ChannelLeaving ||
			channel.Status() == model.ChannelLeft) && owner.Status() != model.MemberActive) {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: Channel status does not match current owner membership",
			ErrChannelAuthorityInvariant)
	}
	localMember, ok := roster.CurrentMember(localPeer)
	if !ok || (channel.Status() == model.ChannelLeft && !localMember.Status().Terminal()) ||
		((channel.Status() == model.ChannelActive || channel.Status() == model.ChannelLeaving) &&
			localMember.Status() != model.MemberActive) {
		return verifiedChannelAuthority{}, fmt.Errorf("%w: Channel status does not match current local membership",
			ErrChannelAuthorityInvariant)
	}
	bindings, err := readVerifiedChannelBindings(ctx, tx, localPeer, channel, roster)
	if err != nil {
		return verifiedChannelAuthority{}, err
	}
	return verifiedChannelAuthority{channel: channel, roster: roster, bindings: bindings}, nil
}

func readVerifiedChannelConflicts(ctx context.Context, tx *sql.Tx, channel model.Channel,
	roster model.VerifiedRoster,
) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT revision,incumbent_record_hash,
		incumbent_signed_record_json,incumbent_owner_signature,challenger_record_hash,
		challenger_signed_record_json,challenger_owner_signature,transport_peer_id,detected_at
		FROM channel_conflicts WHERE channel_id=? ORDER BY revision,challenger_record_hash`,
		channel.ID().String())
	if err != nil {
		return 0, fmt.Errorf("%w: read Channel conflicts: %v", ErrChannelAuthorityInvariant, err)
	}
	defer rows.Close()
	members := roster.Members()
	count := 0
	for rows.Next() {
		count++
		if count > maxVerifiedChannelConflicts {
			return 0, fmt.Errorf("%w: Channel conflict evidence exceeds %d rows",
				ErrChannelAuthorityInvariant, maxVerifiedChannelConflicts)
		}
		var revision uint64
		var incumbentDigest, incumbentRaw, incumbentSignature []byte
		var challengerDigest, challengerRaw, challengerSignature []byte
		var transportText, detectedText string
		if err := rows.Scan(&revision, &incumbentDigest, &incumbentRaw, &incumbentSignature,
			&challengerDigest, &challengerRaw, &challengerSignature, &transportText,
			&detectedText); err != nil {
			return 0, fmt.Errorf("%w: scan Channel conflict: %v", ErrChannelAuthorityInvariant, err)
		}
		if revision == 0 || revision > uint64(len(members)) {
			return 0, fmt.Errorf("%w: conflict revision is outside the committed roster",
				ErrChannelAuthorityInvariant)
		}
		incumbent := members[revision-1]
		challengerRecord, err := model.ParseMemberRecord(challengerRaw)
		if err != nil {
			return 0, fmt.Errorf("%w: parse conflict challenger revision %d: %v",
				ErrChannelAuthorityInvariant, revision, err)
		}
		challenger, err := model.AttachMemberSignature(challengerRecord, challengerSignature)
		if err != nil || model.VerifyMember(channel.Descriptor(), challenger) != nil ||
			challenger.Head().Revision() != revision || challenger.Head().Digest() == incumbent.Head().Digest() ||
			!bytes.Equal(incumbentDigest, incumbent.Head().Digest().Bytes()) ||
			!bytes.Equal(incumbentRaw, incumbent.SignedRecord().Bytes()) ||
			!bytes.Equal(incumbentSignature, incumbent.OwnerSignature()) ||
			!bytes.Equal(challengerDigest, challenger.Head().Digest().Bytes()) {
			return 0, fmt.Errorf("%w: conflict revision %d projection or signature mismatch: %v",
				ErrChannelAuthorityInvariant, revision, err)
		}
		candidate := append([]model.Member(nil), members[:revision-1]...)
		candidate = append(candidate, challenger)
		if _, err := model.NewVerifiedRoster(channel.Descriptor(), candidate); err != nil {
			return 0, fmt.Errorf("%w: conflict revision %d challenger is not a valid branch: %v",
				ErrChannelAuthorityInvariant, revision, err)
		}
		transportPeer, err := model.ParsePeerID(transportText)
		if err != nil {
			return 0, fmt.Errorf("%w: conflict transport PeerID: %v", ErrChannelAuthorityInvariant, err)
		}
		if _, err := model.CanonicalPeerIDBytes(transportPeer); err != nil {
			return 0, fmt.Errorf("%w: conflict transport PeerID encoding: %v",
				ErrChannelAuthorityInvariant, err)
		}
		detectedAt, err := parseCanonicalStoreTime(detectedText)
		if err != nil || detectedAt.Before(channel.CreatedAt()) || detectedAt.After(channel.UpdatedAt()) ||
			detectedAt.Before(incumbent.CreatedAt()) || detectedAt.Before(challenger.CreatedAt()) {
			return 0, fmt.Errorf("%w: conflict detection time is invalid: %v",
				ErrChannelAuthorityInvariant, err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("%w: iterate Channel conflicts: %v", ErrChannelAuthorityInvariant, err)
	}
	return count, nil
}

func readVerifiedChannelBindings(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster,
) ([]model.PeerBinding, error) {
	rows, err := tx.QueryContext(ctx, `SELECT binding.peer_id,binding.origin_epoch,
		binding.effective_alias,binding.public_key,binding.multiaddrs_json,binding.protocols_json,
		binding.limits_json,binding.member_revision,binding.member_record_hash,binding.state,
		binding.reachability,binding.joined_at,binding.last_seen_at,cursor.origin_epoch
		FROM peer_bindings binding
		LEFT JOIN peer_cursors cursor ON cursor.channel_id=binding.channel_id
			AND cursor.origin_peer_id=binding.peer_id AND cursor.origin_epoch=binding.origin_epoch
		WHERE binding.channel_id=? ORDER BY binding.peer_id`,
		channel.ID().String())
	if err != nil {
		return nil, fmt.Errorf("%w: read PeerBindings: %v", ErrChannelAuthorityInvariant, err)
	}
	defer rows.Close()
	bindings := make([]model.PeerBinding, 0, model.MaxMembersPerChannel-1)
	for rows.Next() {
		var peerText, epochText, alias, stateText, reachabilityText, joinedText string
		var publicKey, multiaddrsRaw, protocolsRaw, limitsRaw, memberDigest []byte
		var memberRevision uint64
		var lastSeenText, cursorEpochText sql.NullString
		if err := rows.Scan(&peerText, &epochText, &alias, &publicKey, &multiaddrsRaw, &protocolsRaw,
			&limitsRaw, &memberRevision, &memberDigest, &stateText, &reachabilityText, &joinedText,
			&lastSeenText, &cursorEpochText); err != nil {
			return nil, fmt.Errorf("%w: scan PeerBinding: %v", ErrChannelAuthorityInvariant, err)
		}
		peerID, err := model.ParsePeerID(peerText)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding PeerID: %v", ErrChannelAuthorityInvariant, err)
		}
		epoch, err := model.ParseOriginEpoch(epochText)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding epoch: %v", ErrChannelAuthorityInvariant, err)
		}
		digest, err := model.DigestFromBytes(memberDigest)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding member digest: %v", ErrChannelAuthorityInvariant, err)
		}
		memberHead, err := model.NewRecordHead(memberRevision, digest)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding member head: %v", ErrChannelAuthorityInvariant, err)
		}
		multiaddrs, err := parseCanonicalAgentStringArray("PeerBinding multiaddrs", multiaddrsRaw)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding multiaddrs: %v", ErrChannelAuthorityInvariant, err)
		}
		protocols, err := parseCanonicalAgentStringArray("PeerBinding protocols", protocolsRaw)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding protocols: %v", ErrChannelAuthorityInvariant, err)
		}
		limits, err := model.NewJSON(limitsRaw)
		if err != nil || !bytes.Equal(limits.Bytes(), limitsRaw) {
			return nil, fmt.Errorf("%w: PeerBinding limits are noncanonical", ErrChannelAuthorityInvariant)
		}
		joinedAt, err := parseCanonicalStoreTime(joinedText)
		if err != nil {
			return nil, fmt.Errorf("%w: PeerBinding joined_at: %v", ErrChannelAuthorityInvariant, err)
		}
		var lastSeen *time.Time
		if lastSeenText.Valid {
			parsed, err := parseCanonicalStoreTime(lastSeenText.String)
			if err != nil {
				return nil, fmt.Errorf("%w: PeerBinding last_seen_at: %v", ErrChannelAuthorityInvariant, err)
			}
			lastSeen = &parsed
		}
		binding, err := model.NewPeerBinding(localPeer, model.PeerBindingSpec{Channel: channel,
			Roster: roster, PeerID: peerID, EffectiveAlias: alias, State: model.BindingState(stateText),
			Reachability: model.Reachability(reachabilityText), JoinedAt: joinedAt, LastSeenAt: lastSeen})
		if err != nil || binding.OriginEpoch() != epoch || binding.MemberHead() != memberHead ||
			!bytes.Equal(binding.PublicKey(), publicKey) || !equalAgentStrings(binding.Multiaddrs(), multiaddrs) ||
			!equalAgentStrings(binding.Protocols(), protocols) || binding.Limits().String() != limits.String() ||
			(binding.State() == model.BindingActive &&
				(!cursorEpochText.Valid || cursorEpochText.String != binding.OriginEpoch().String())) ||
			(binding.State() == model.BindingPending && cursorEpochText.Valid) {
			return nil, fmt.Errorf("%w: PeerBinding %q is not current signed authority: %v",
				ErrChannelAuthorityInvariant, peerText, err)
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate PeerBindings: %v", ErrChannelAuthorityInvariant, err)
	}
	return bindings, nil
}
