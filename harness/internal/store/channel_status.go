package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelStatusAuthority = errors.New("durable Channel status authority is unavailable")

type ChannelStatusArrival string

const (
	ChannelStatusArrivalLocal  ChannelStatusArrival = "local"
	ChannelStatusArrivalGossip ChannelStatusArrival = "gossip"
	ChannelStatusArrivalRepair ChannelStatusArrival = "repair"
)

func (arrival ChannelStatusArrival) Valid() bool {
	return arrival == ChannelStatusArrivalLocal || arrival == ChannelStatusArrivalGossip ||
		arrival == ChannelStatusArrivalRepair
}

type ChannelStatusSemanticOutcome string

const ChannelStatusOutcomeOriginated ChannelStatusSemanticOutcome = "originated"

func (outcome ChannelStatusSemanticOutcome) Valid() bool {
	if outcome == ChannelStatusOutcomeOriginated {
		return true
	}
	return model.InboxStatus(outcome).Valid()
}

// ChannelStatusAuthority is one coherent, complete and bounded public evidence
// snapshot. It contains no bearer material, private keys, lease owners, tokens,
// payloads or raw signed envelopes.
type ChannelStatusAuthority struct {
	localPeerID model.PeerID
	channels    []ChannelStatusChannel
}

type ChannelStatusChannel struct {
	control         ChannelControlChannel
	channelIDDigest model.Digest
	rosterHead      ChannelStatusRosterHead
	publications    []ChannelStatusPublication
}

type ChannelStatusRosterHead struct {
	recordHead     model.RecordHead
	ownerPeerID    model.PeerID
	ownerSignature []byte
}

type ChannelStatusPublicationRef struct {
	originPeerID    model.PeerID
	originEpoch     model.OriginEpoch
	channelSequence uint64
}

type ChannelStatusPublication struct {
	publicationRef             ChannelStatusPublicationRef
	publicationDigest          model.Digest
	eventKey                   model.EventKey
	eventDigest                model.Digest
	channelIDDigest            model.Digest
	originPeerID               model.PeerID
	immediateTransportPeerID   model.PeerID
	arrival                    ChannelStatusArrival
	audiencePeerIDs            []model.PeerID
	ignoredPeerIDs             []model.PeerID
	semanticOutcome            ChannelStatusSemanticOutcome
	artifactDirectSourcePeerID model.PeerID
	hasArtifactDirectSource    bool
	causalityEventKey          model.EventKey
	hasCausalityEventKey       bool
}

func (authority ChannelStatusAuthority) LocalPeerID() model.PeerID { return authority.localPeerID }
func (authority ChannelStatusAuthority) Channels() []ChannelStatusChannel {
	result := make([]ChannelStatusChannel, len(authority.channels))
	for index, channel := range authority.channels {
		result[index] = channel.clone()
	}
	return result
}
func (channel ChannelStatusChannel) Channel() model.Channel        { return channel.control.Channel() }
func (channel ChannelStatusChannel) Roster() model.VerifiedRoster  { return channel.control.Roster() }
func (channel ChannelStatusChannel) Bindings() []model.PeerBinding { return channel.control.Bindings() }
func (channel ChannelStatusChannel) OpenGrant() (ChannelControlGrant, bool) {
	return channel.control.OpenGrant()
}
func (channel ChannelStatusChannel) ChannelIDDigest() model.Digest       { return channel.channelIDDigest }
func (channel ChannelStatusChannel) RosterHead() ChannelStatusRosterHead { return channel.rosterHead }
func (channel ChannelStatusChannel) Publications() []ChannelStatusPublication {
	result := make([]ChannelStatusPublication, len(channel.publications))
	for index, publication := range channel.publications {
		result[index] = publication.clone()
	}
	return result
}
func (channel ChannelStatusChannel) clone() ChannelStatusChannel {
	channel.publications = channel.Publications()
	channel.rosterHead.ownerSignature = channel.rosterHead.OwnerSignature()
	return channel
}
func (head ChannelStatusRosterHead) RecordHead() model.RecordHead { return head.recordHead }
func (head ChannelStatusRosterHead) OwnerPeerID() model.PeerID    { return head.ownerPeerID }
func (head ChannelStatusRosterHead) OwnerSignature() []byte {
	return append([]byte(nil), head.ownerSignature...)
}
func (reference ChannelStatusPublicationRef) OriginPeerID() model.PeerID {
	return reference.originPeerID
}
func (reference ChannelStatusPublicationRef) OriginEpoch() model.OriginEpoch {
	return reference.originEpoch
}
func (reference ChannelStatusPublicationRef) ChannelSequence() uint64 {
	return reference.channelSequence
}
func (publication ChannelStatusPublication) PublicationRef() ChannelStatusPublicationRef {
	return publication.publicationRef
}
func (publication ChannelStatusPublication) PublicationDigest() model.Digest {
	return publication.publicationDigest
}
func (publication ChannelStatusPublication) EventKey() model.EventKey { return publication.eventKey }
func (publication ChannelStatusPublication) EventDigest() model.Digest {
	return publication.eventDigest
}
func (publication ChannelStatusPublication) ChannelIDDigest() model.Digest {
	return publication.channelIDDigest
}
func (publication ChannelStatusPublication) OriginPeerID() model.PeerID {
	return publication.originPeerID
}
func (publication ChannelStatusPublication) ImmediateTransportPeerID() model.PeerID {
	return publication.immediateTransportPeerID
}
func (publication ChannelStatusPublication) Arrival() ChannelStatusArrival {
	return publication.arrival
}
func (publication ChannelStatusPublication) AudiencePeerIDs() []model.PeerID {
	return append([]model.PeerID(nil), publication.audiencePeerIDs...)
}
func (publication ChannelStatusPublication) IgnoredPeerIDs() []model.PeerID {
	return append([]model.PeerID(nil), publication.ignoredPeerIDs...)
}
func (publication ChannelStatusPublication) SemanticOutcome() ChannelStatusSemanticOutcome {
	return publication.semanticOutcome
}
func (publication ChannelStatusPublication) ArtifactDirectSourcePeerID() (model.PeerID, bool) {
	return publication.artifactDirectSourcePeerID, publication.hasArtifactDirectSource
}
func (publication ChannelStatusPublication) CausalityEventKey() (model.EventKey, bool) {
	return publication.causalityEventKey, publication.hasCausalityEventKey
}
func (publication ChannelStatusPublication) clone() ChannelStatusPublication {
	publication.audiencePeerIDs = publication.AudiencePeerIDs()
	publication.ignoredPeerIDs = publication.IgnoredPeerIDs()
	return publication
}

func (s *Store) ReadChannelStatusAuthority(ctx context.Context) (ChannelStatusAuthority, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ChannelStatusAuthority{}, fmt.Errorf("%w: Store is unavailable", ErrChannelStatusAuthority)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelStatusAuthority{}, fmt.Errorf("%w: begin: %v", ErrChannelStatusAuthority, err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelStatusAuthority{}, fmt.Errorf("%w: read Node: %v", ErrChannelStatusAuthority, err)
	}
	ids, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return ChannelStatusAuthority{}, fmt.Errorf("%w: %v", ErrChannelStatusAuthority, err)
	}
	channels := make([]ChannelStatusChannel, 0, len(ids))
	byID := make(map[model.ChannelID]int, len(ids))
	for _, id := range ids {
		control, err := readChannelControlChannel(ctx, tx, node.PeerID(), id)
		if err != nil {
			return ChannelStatusAuthority{}, fmt.Errorf("%w: %v", ErrChannelStatusAuthority, err)
		}
		members := control.Roster().Members()
		if len(members) == 0 || members[len(members)-1].Head() != control.Channel().RosterHead() {
			return ChannelStatusAuthority{}, fmt.Errorf("%w: Channel %q roster head is unavailable",
				ErrChannelStatusAuthority, id.String())
		}
		head := members[len(members)-1]
		byID[id] = len(channels)
		channels = append(channels, ChannelStatusChannel{control: control,
			channelIDDigest: model.Sum([]byte(id.String())),
			rosterHead: ChannelStatusRosterHead{recordHead: head.Head(),
				ownerPeerID: control.Channel().OwnerPeerID(), ownerSignature: head.OwnerSignature()},
			publications: []ChannelStatusPublication{}})
	}
	if err := readChannelStatusPublications(ctx, tx, node.PeerID(), channels, byID); err != nil {
		return ChannelStatusAuthority{}, err
	}
	if err := tx.Commit(); err != nil {
		return ChannelStatusAuthority{}, fmt.Errorf("%w: commit read: %v", ErrChannelStatusAuthority, err)
	}
	return ChannelStatusAuthority{localPeerID: node.PeerID(), channels: channels}, nil
}

func readChannelStatusPublications(ctx context.Context, tx *sql.Tx, local model.PeerID,
	channels []ChannelStatusChannel, byID map[model.ChannelID]int,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT evidence_kind,channel_id,origin_peer_id,
		transport_peer_id,origin_epoch,origin_seq,channel_seq,event_id,event_digest,
		publication_digest,publication_bytes,origin_signature,arrival_source,is_audience,
		semantic_outcome,artifact_source_peer_id FROM (
		SELECT 'local' AS evidence_kind,e.channel_id,e.origin_peer_id,
			e.origin_peer_id AS transport_peer_id,e.origin_epoch,e.origin_seq,e.channel_seq,
			e.event_id,e.event_digest,e.publication_digest,
			e.canonical_publication_json AS publication_bytes,e.origin_signature,
			'local' AS arrival_source,1 AS is_audience,'originated' AS semantic_outcome,
			NULL AS artifact_source_peer_id
		FROM events e WHERE e.source='local'
		UNION ALL
		SELECT 'inbox',i.channel_id,i.origin_peer_id,i.transport_peer_id,i.origin_epoch,
			i.origin_seq,i.channel_seq,i.event_id,i.event_digest,i.publication_digest,
			i.publication_json,i.origin_signature,i.arrival_source,i.is_audience,i.status,
			r.source_peer_id
		FROM peer_inbox i LEFT JOIN peer_inbox_artifact_source_receipts r
			ON r.inbox_id=i.inbox_id
	) ORDER BY channel_id,channel_seq,origin_peer_id,origin_epoch,event_id LIMIT ?`,
		model.MaxChannelStatusPublications+1)
	if err != nil {
		return fmt.Errorf("%w: read publications: %v", ErrChannelStatusAuthority, err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		if count > model.MaxChannelStatusPublications {
			return fmt.Errorf("%w: publication evidence exceeds complete snapshot bound %d",
				ErrChannelStatusAuthority, model.MaxChannelStatusPublications)
		}
		publication, channelID, err := scanChannelStatusPublication(rows, local, channels, byID)
		if err != nil {
			return err
		}
		index, ok := byID[channelID]
		if !ok {
			return fmt.Errorf("%w: publication references unknown Channel %q",
				ErrChannelStatusAuthority, channelID.String())
		}
		channels[index].publications = append(channels[index].publications, publication)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: iterate publications: %v", ErrChannelStatusAuthority, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("%w: close publications: %v", ErrChannelStatusAuthority, err)
	}
	return nil
}

type channelStatusScanner interface{ Scan(...any) error }

func scanChannelStatusPublication(scanner channelStatusScanner, local model.PeerID,
	channels []ChannelStatusChannel, byID map[model.ChannelID]int,
) (ChannelStatusPublication, model.ChannelID, error) {
	var kind, channelText, originText, transportText, epochText, eventText string
	var arrivalText, outcomeText string
	var originSequence, channelSequence uint64
	var eventDigestRaw, publicationDigestRaw, publicationRaw, signature []byte
	var isAudience int
	var artifactSourceText sql.NullString
	if err := scanner.Scan(&kind, &channelText, &originText, &transportText, &epochText,
		&originSequence, &channelSequence, &eventText, &eventDigestRaw, &publicationDigestRaw,
		&publicationRaw, &signature, &arrivalText, &isAudience, &outcomeText,
		&artifactSourceText); err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: scan publication: %v",
			ErrChannelStatusAuthority, err)
	}
	channelID, channelErr := model.ParseChannelID(channelText)
	origin, originErr := model.ParsePeerID(originText)
	transport, transportErr := model.ParsePeerID(transportText)
	epoch, epochErr := model.ParseOriginEpoch(epochText)
	eventID, eventErr := model.ParseEventID(eventText)
	eventDigest, eventDigestErr := model.DigestFromBytes(eventDigestRaw)
	publicationDigest, publicationDigestErr := model.DigestFromBytes(publicationDigestRaw)
	if channelErr != nil || originErr != nil || transportErr != nil || epochErr != nil ||
		eventErr != nil || eventDigestErr != nil || publicationDigestErr != nil ||
		originSequence == 0 || channelSequence == 0 || isAudience < 0 || isAudience > 1 {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: malformed publication tuple",
			ErrChannelStatusAuthority)
	}
	index, ok := byID[channelID]
	if !ok {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: publication references unknown Channel",
			ErrChannelStatusAuthority)
	}
	var wire []byte
	switch kind {
	case "local":
		body, err := model.NewJSON(publicationRaw)
		if err != nil || !bytes.Equal(body.Bytes(), publicationRaw) {
			return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: noncanonical local publication body",
				ErrChannelStatusAuthority)
		}
		encoded, err := model.JSONFrom(struct {
			Publication       model.JSON   `json:"publication"`
			PublicationDigest model.Digest `json:"publication_digest"`
			OriginSignature   []byte       `json:"origin_signature"`
		}{body, publicationDigest, signature})
		if err != nil {
			return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: local publication wire: %v",
				ErrChannelStatusAuthority, err)
		}
		wire = encoded.Bytes()
	case "inbox":
		wire = publicationRaw
	default:
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: unknown publication evidence kind",
			ErrChannelStatusAuthority)
	}
	signed, err := model.ParseSignedPublication(wire)
	if err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: unsupported signed publication: %v",
			ErrChannelStatusAuthority, err)
	}
	event, scope := signed.Event(), signed.Event().Scope()
	if event.Source() != model.EventSourceLocal || signed.Digest() != publicationDigest ||
		event.Digest() != eventDigest || event.ID() != eventID || scope.ChannelID() != channelID ||
		scope.OriginPeerID() != origin || scope.OriginEpoch() != epoch ||
		scope.OriginSequence() != originSequence || scope.ChannelSequence() != channelSequence ||
		!bytes.Equal(signed.OriginSignature(), signature) {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: signed publication projection mismatch",
			ErrChannelStatusAuthority)
	}
	channel := channels[index]
	if err := verifyChannelStatusPublication(channel, signed); err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, err
	}
	arrival := ChannelStatusArrival(arrivalText)
	outcome := ChannelStatusSemanticOutcome(outcomeText)
	ignored := []model.PeerID{}
	if kind == "local" {
		if origin != local || transport != local || arrival != ChannelStatusArrivalLocal ||
			isAudience != 1 || outcome != ChannelStatusOutcomeOriginated || artifactSourceText.Valid {
			return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: invalid local publication path",
				ErrChannelStatusAuthority)
		}
	} else {
		if origin == local || transport == local || !model.ArrivalSource(arrivalText).Valid() ||
			(model.ArrivalSource(arrivalText) == model.ArrivalPull && transport != origin) ||
			!model.InboxStatus(outcomeText).Valid() ||
			(isAudience == 1) != event.Audience().Contains(local) {
			return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: invalid imported publication path",
				ErrChannelStatusAuthority)
		}
		if model.ArrivalSource(arrivalText) == model.ArrivalPull {
			arrival = ChannelStatusArrivalRepair
		} else {
			arrival = ChannelStatusArrivalGossip
		}
		if isAudience == 0 {
			ignored = append(ignored, local)
		}
	}
	if !arrival.Valid() || !outcome.Valid() {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: unclosed path outcome",
			ErrChannelStatusAuthority)
	}
	causedBy := event.CausedBy()
	if len(causedBy) > 1 {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: publication causality exceeds singular public contract",
			ErrChannelStatusAuthority)
	}
	key := signed.Key()
	publication := ChannelStatusPublication{publicationRef: ChannelStatusPublicationRef{
		originPeerID: key.OriginPeerID(), originEpoch: key.OriginEpoch(),
		channelSequence: key.ChannelSequence()},
		publicationDigest: signed.Digest(), eventKey: event.Key(), eventDigest: event.Digest(),
		channelIDDigest: channel.channelIDDigest, originPeerID: origin,
		immediateTransportPeerID: transport, arrival: arrival,
		audiencePeerIDs: event.Audience().Peers(), ignoredPeerIDs: ignored, semanticOutcome: outcome}
	if artifactSourceText.Valid {
		source, err := model.ParsePeerID(artifactSourceText.String)
		if err != nil || source != origin || len(event.Artifacts()) == 0 {
			return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf("%w: invalid Artifact direct source",
				ErrChannelStatusAuthority)
		}
		publication.artifactDirectSourcePeerID = source
		publication.hasArtifactDirectSource = true
	}
	if len(causedBy) == 1 {
		publication.causalityEventKey = causedBy[0]
		publication.hasCausalityEventKey = true
	}
	return publication, channelID, nil
}

func verifyChannelStatusPublication(channel ChannelStatusChannel,
	publication model.SignedPublication,
) error {
	event := publication.Event()
	scope := event.Scope()
	members := channel.Roster().Members()
	if scope.OriginMember().Revision() == 0 || scope.OriginMember().Revision() > uint64(len(members)) ||
		scope.PublicationRoster().Revision() < scope.OriginMember().Revision() ||
		scope.PublicationRoster().Revision() > uint64(len(members)) ||
		members[scope.OriginMember().Revision()-1].Head() != scope.OriginMember() ||
		members[scope.PublicationRoster().Revision()-1].Head() != scope.PublicationRoster() {
		return fmt.Errorf("%w: publication roster authority is outside verified history",
			ErrChannelStatusAuthority)
	}
	origin := members[scope.OriginMember().Revision()-1]
	if origin.PeerID() != scope.OriginPeerID() || origin.OriginEpoch() != scope.OriginEpoch() ||
		origin.Status() != model.MemberActive {
		return fmt.Errorf("%w: publication origin lacks active signed membership",
			ErrChannelStatusAuthority)
	}
	if err := model.VerifyPublication(origin.PublicKey(), publication); err != nil {
		return fmt.Errorf("%w: publication origin signature: %v", ErrChannelStatusAuthority, err)
	}
	return nil
}
