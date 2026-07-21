package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

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
		progress, err := readChannelStatusProgress(ctx, tx, node, control)
		if err != nil {
			return ChannelStatusAuthority{}, err
		}
		head := members[len(members)-1]
		byID[id] = len(channels)
		channels = append(channels, ChannelStatusChannel{control: control,
			channelIDDigest: model.Sum([]byte(id.String())),
			rosterHead: ChannelStatusRosterHead{recordHead: head.Head(),
				ownerPeerID: control.Channel().OwnerPeerID(), ownerSignature: head.OwnerSignature()},
			publications: []ChannelStatusPublication{}, progress: progress})
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

type channelStatusPublicationRow struct {
	kind              string
	channelID         model.ChannelID
	originPeerID      model.PeerID
	transportPeerID   model.PeerID
	originEpoch       model.OriginEpoch
	originSequence    uint64
	channelSequence   uint64
	eventID           model.EventID
	eventDigest       model.Digest
	publicationDigest model.Digest
	publicationBytes  []byte
	originSignature   []byte
	arrival           string
	isAudience        int
	semanticOutcome   string
	artifactSource    sql.NullString
}

func scanChannelStatusPublication(scanner channelStatusScanner, local model.PeerID,
	channels []ChannelStatusChannel, byID map[model.ChannelID]int,
) (ChannelStatusPublication, model.ChannelID, error) {
	row, err := scanChannelStatusPublicationRow(scanner)
	if err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, err
	}
	index, ok := byID[row.channelID]
	if !ok {
		return ChannelStatusPublication{}, model.ChannelID{}, fmt.Errorf(
			"%w: publication references unknown Channel", ErrChannelStatusAuthority)
	}
	channel := channels[index]
	signed, err := decodeChannelStatusSignedPublication(row)
	if err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, err
	}
	if err := verifyChannelStatusPublication(channel, signed); err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, err
	}
	arrival, outcome, ignored, err := projectChannelStatusPublicationPath(row, signed.Event(), local)
	if err != nil {
		return ChannelStatusPublication{}, model.ChannelID{}, err
	}
	publication, err := projectChannelStatusPublication(row, channel, signed, arrival, outcome, ignored)
	return publication, row.channelID, err
}

func scanChannelStatusPublicationRow(scanner channelStatusScanner,
) (channelStatusPublicationRow, error) {
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
		return channelStatusPublicationRow{}, fmt.Errorf("%w: scan publication: %v",
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
		return channelStatusPublicationRow{}, fmt.Errorf("%w: malformed publication tuple",
			ErrChannelStatusAuthority)
	}
	return channelStatusPublicationRow{kind: kind, channelID: channelID, originPeerID: origin,
		transportPeerID: transport, originEpoch: epoch, originSequence: originSequence,
		channelSequence: channelSequence, eventID: eventID, eventDigest: eventDigest,
		publicationDigest: publicationDigest, publicationBytes: publicationRaw,
		originSignature: signature, arrival: arrivalText, isAudience: isAudience,
		semanticOutcome: outcomeText, artifactSource: artifactSourceText}, nil
}

func decodeChannelStatusSignedPublication(row channelStatusPublicationRow,
) (model.SignedPublication, error) {
	wire, err := channelStatusPublicationWire(row)
	if err != nil {
		return model.SignedPublication{}, err
	}
	signed, err := model.ParseSignedPublication(wire)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf("%w: unsupported signed publication: %v",
			ErrChannelStatusAuthority, err)
	}
	event, scope := signed.Event(), signed.Event().Scope()
	if event.Source() != model.EventSourceLocal || signed.Digest() != row.publicationDigest ||
		event.Digest() != row.eventDigest || event.ID() != row.eventID ||
		scope.ChannelID() != row.channelID || scope.OriginPeerID() != row.originPeerID ||
		scope.OriginEpoch() != row.originEpoch || scope.OriginSequence() != row.originSequence ||
		scope.ChannelSequence() != row.channelSequence ||
		!bytes.Equal(signed.OriginSignature(), row.originSignature) {
		return model.SignedPublication{}, fmt.Errorf("%w: signed publication projection mismatch",
			ErrChannelStatusAuthority)
	}
	return signed, nil
}

func channelStatusPublicationWire(row channelStatusPublicationRow) ([]byte, error) {
	var wire []byte
	switch row.kind {
	case "local":
		body, err := model.NewJSON(row.publicationBytes)
		if err != nil || !bytes.Equal(body.Bytes(), row.publicationBytes) {
			return nil, fmt.Errorf("%w: noncanonical local publication body",
				ErrChannelStatusAuthority)
		}
		encoded, err := model.JSONFrom(struct {
			Publication       model.JSON   `json:"publication"`
			PublicationDigest model.Digest `json:"publication_digest"`
			OriginSignature   []byte       `json:"origin_signature"`
		}{body, row.publicationDigest, row.originSignature})
		if err != nil {
			return nil, fmt.Errorf("%w: local publication wire: %v",
				ErrChannelStatusAuthority, err)
		}
		wire = encoded.Bytes()
	case "inbox":
		wire = row.publicationBytes
	default:
		return nil, fmt.Errorf("%w: unknown publication evidence kind",
			ErrChannelStatusAuthority)
	}
	return wire, nil
}

func projectChannelStatusPublicationPath(row channelStatusPublicationRow, event model.Event,
	local model.PeerID,
) (ChannelStatusArrival, ChannelStatusSemanticOutcome, []model.PeerID, error) {
	arrival := ChannelStatusArrival(row.arrival)
	outcome := ChannelStatusSemanticOutcome(row.semanticOutcome)
	ignored := []model.PeerID{}
	if row.kind == "local" {
		if row.originPeerID != local || row.transportPeerID != local ||
			arrival != ChannelStatusArrivalLocal || row.isAudience != 1 ||
			outcome != ChannelStatusOutcomeOriginated || row.artifactSource.Valid {
			return "", "", nil, fmt.Errorf("%w: invalid local publication path",
				ErrChannelStatusAuthority)
		}
	} else {
		source := model.ArrivalSource(row.arrival)
		if row.kind != "inbox" || row.originPeerID == local || row.transportPeerID == local ||
			!source.Valid() || source == model.ArrivalPull && row.transportPeerID != row.originPeerID ||
			!model.InboxStatus(row.semanticOutcome).Valid() ||
			(row.isAudience == 1) != event.Audience().Contains(local) {
			return "", "", nil, fmt.Errorf("%w: invalid imported publication path",
				ErrChannelStatusAuthority)
		}
		if source == model.ArrivalPull {
			arrival = ChannelStatusArrivalRepair
		} else {
			arrival = ChannelStatusArrivalGossip
		}
		if row.isAudience == 0 {
			ignored = append(ignored, local)
		}
	}
	if !arrival.Valid() || !outcome.Valid() {
		return "", "", nil, fmt.Errorf("%w: unclosed path outcome",
			ErrChannelStatusAuthority)
	}
	return arrival, outcome, ignored, nil
}

func projectChannelStatusPublication(row channelStatusPublicationRow, channel ChannelStatusChannel,
	signed model.SignedPublication, arrival ChannelStatusArrival,
	outcome ChannelStatusSemanticOutcome, ignored []model.PeerID,
) (ChannelStatusPublication, error) {
	event := signed.Event()
	causedBy := event.CausedBy()
	if len(causedBy) > 1 {
		return ChannelStatusPublication{}, fmt.Errorf("%w: publication causality exceeds singular public contract",
			ErrChannelStatusAuthority)
	}
	key := signed.Key()
	publication := ChannelStatusPublication{publicationRef: ChannelStatusPublicationRef{
		originPeerID: key.OriginPeerID(), originEpoch: key.OriginEpoch(),
		channelSequence: key.ChannelSequence()},
		publicationDigest: signed.Digest(), eventKey: event.Key(), eventDigest: event.Digest(),
		channelIDDigest: channel.channelIDDigest, originPeerID: row.originPeerID,
		immediateTransportPeerID: row.transportPeerID, arrival: arrival,
		audiencePeerIDs: event.Audience().Peers(), ignoredPeerIDs: ignored, semanticOutcome: outcome}
	if row.artifactSource.Valid {
		source, err := model.ParsePeerID(row.artifactSource.String)
		if err != nil || source != row.originPeerID || len(event.Artifacts()) == 0 {
			return ChannelStatusPublication{}, fmt.Errorf("%w: invalid Artifact direct source",
				ErrChannelStatusAuthority)
		}
		publication.artifactDirectSourcePeerID = source
		publication.hasArtifactDirectSource = true
	}
	if len(causedBy) == 1 {
		publication.causalityEventKey = causedBy[0]
		publication.hasCausalityEventKey = true
	}
	return publication, nil
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
