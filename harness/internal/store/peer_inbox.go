package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrPeerInboxInput       = errors.New("invalid Peer Inbox arrival input")
	ErrPeerInboxAuthority   = errors.New("Peer Inbox Channel authority is unavailable")
	ErrPeerInboxQuarantined = errors.New("Peer Inbox origin epoch is quarantined")
	ErrPeerInboxConflict    = errors.New("Peer Inbox durable identity conflicts")
	ErrPeerInboxPressure    = errors.New("Peer Inbox pending byte budget is exhausted")
)

type PeerInboxDisposition string

const (
	PeerInboxStored      PeerInboxDisposition = "stored"
	PeerInboxIgnored     PeerInboxDisposition = "ignored"
	PeerInboxDuplicate   PeerInboxDisposition = "duplicate"
	PeerInboxCovered     PeerInboxDisposition = "covered"
	PeerInboxConflicted  PeerInboxDisposition = "conflicted"
	PeerInboxQuarantined PeerInboxDisposition = "quarantined"
)

type PutPeerInboxSpec struct {
	Publication     model.SignedPublication
	TransportPeerID model.PeerID
	ArrivalSource   model.ArrivalSource
	ReceivedAt      time.Time
}

type PeerCursorProjection struct {
	ChannelID                 model.ChannelID
	OriginPeerID              model.PeerID
	OriginEpoch               model.OriginEpoch
	BaselineChannelSequence   uint64
	ContiguousChannelSequence uint64
	ObservedChannelSequence   uint64
	UpdatedAt                 time.Time
}

type PutPeerInboxResult struct {
	InboxID     model.InboxID
	Disposition PeerInboxDisposition
	ConflictID  string
	Cursor      PeerCursorProjection
}

type peerInboxIncumbent struct {
	inboxID           string
	channelID         string
	originPeerID      string
	originEpoch       string
	originSequence    uint64
	channelSequence   uint64
	eventID           string
	eventDigest       []byte
	publicationDigest []byte
	signature         []byte
	wire              []byte
}

type peerInboxArrivalAuthority struct {
	node      model.Node
	channel   verifiedChannelAuthority
	originKey []byte
}

// PutPeerInbox durably records one authenticated publication arrival. The
// signed wire, Inbox row, equivocation evidence, quarantine and repair cursor
// are committed in one transaction, so an ACK can only describe durable
// coverage. Semantic Event application deliberately happens in a later Inbox
// worker transaction.
func (s *Store) PutPeerInbox(ctx context.Context, spec PutPeerInboxSpec) (PutPeerInboxResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return PutPeerInboxResult{}, ErrPeerInboxInput
	}
	var err error
	spec, err = normalizePeerInboxSpec(spec)
	if err != nil {
		return PutPeerInboxResult{}, err
	}
	scope := spec.Publication.Event().Scope()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("put Peer Inbox: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readPeerInboxArrivalAuthority(ctx, tx, spec)
	if err != nil {
		return PutPeerInboxResult{}, err
	}
	cursor, err := readPeerCursor(ctx, tx, scope.ChannelID(), scope.OriginPeerID(), scope.OriginEpoch())
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: inbound baseline: %v", ErrPeerInboxAuthority, err)
	}
	result, err := putPeerInboxTx(ctx, tx, spec, authority, cursor)
	if err != nil {
		return PutPeerInboxResult{}, err
	}
	cursor, err = advancePeerCursor(ctx, tx, cursor, scope.ChannelSequence(), spec.ReceivedAt,
		spec.ArrivalSource == model.ArrivalGossip)
	if err != nil {
		return PutPeerInboxResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("put Peer Inbox: commit: %w", err)
	}
	result.Cursor = cursor
	return result, nil
}

func normalizePeerInboxSpec(spec PutPeerInboxSpec) (PutPeerInboxSpec, error) {
	if spec.TransportPeerID.IsZero() || !spec.ArrivalSource.Valid() {
		return PutPeerInboxSpec{}, ErrPeerInboxInput
	}
	receivedAt, err := canonicalStoreTime(spec.ReceivedAt)
	if err != nil || receivedAt.IsZero() {
		return PutPeerInboxSpec{}, ErrPeerInboxInput
	}
	publication, err := model.ProjectImportedPublication(&spec.Publication)
	if err != nil {
		return PutPeerInboxSpec{}, fmt.Errorf("%w: publication: %v", ErrPeerInboxInput, err)
	}
	spec.Publication, spec.ReceivedAt = publication, receivedAt
	if spec.ArrivalSource == model.ArrivalPull &&
		spec.TransportPeerID != publication.Event().Scope().OriginPeerID() {
		return PutPeerInboxSpec{}, ErrPeerInboxInput
	}
	return spec, nil
}

func readPeerInboxArrivalAuthority(ctx context.Context, tx *sql.Tx,
	spec PutPeerInboxSpec,
) (peerInboxArrivalAuthority, error) {
	scope := spec.Publication.Event().Scope()
	node, err := readNode(ctx, tx)
	if err != nil {
		return peerInboxArrivalAuthority{}, fmt.Errorf("%w: Node: %v", ErrPeerInboxAuthority, err)
	}
	if node.PeerID() == scope.OriginPeerID() || node.PeerID() == spec.TransportPeerID {
		return peerInboxArrivalAuthority{}, ErrPeerInboxInput
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), scope.ChannelID())
	if err != nil {
		return peerInboxArrivalAuthority{}, fmt.Errorf("%w: %v", ErrPeerInboxAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined ||
		spec.ReceivedAt.Before(authority.channel.UpdatedAt()) {
		return peerInboxArrivalAuthority{}, ErrPeerInboxAuthority
	}
	originBinding, ok := activePeerInboxBinding(authority.bindings, scope.OriginPeerID())
	if !ok || originBinding.OriginEpoch() != scope.OriginEpoch() {
		return peerInboxArrivalAuthority{}, ErrPeerInboxAuthority
	}
	if _, ok := activePeerInboxBinding(authority.bindings, spec.TransportPeerID); !ok {
		return peerInboxArrivalAuthority{}, ErrPeerInboxAuthority
	}
	result := peerInboxArrivalAuthority{node: node, channel: authority,
		originKey: originBinding.PublicKey()}
	if err := authenticatePeerInboxPublication(result, spec.Publication); err != nil {
		return peerInboxArrivalAuthority{}, err
	}
	return result, nil
}

func authenticatePeerInboxPublication(authority peerInboxArrivalAuthority,
	publication model.SignedPublication,
) error {
	evidence, err := model.ParsePublicationEvidence(publication.WireJSON().Bytes())
	if err != nil {
		return fmt.Errorf("%w: publication evidence: %v", ErrPeerInboxAuthority, err)
	}
	if !evidence.IsSupported() {
		return fmt.Errorf("%w: publication evidence lost supported semantics", ErrPeerInboxAuthority)
	}
	return authenticatePeerInboxEvidence(authority, evidence)
}

func authenticatePeerInboxEvidence(authority peerInboxArrivalAuthority,
	evidence model.PublicationEvidence,
) error {
	originMember, err := authenticatePublicationEvidenceRoster(authority.channel.roster, evidence)
	if err != nil || !bytes.Equal(originMember.PublicKey(), authority.originKey) {
		return fmt.Errorf("%w: publication authority: %v", ErrPeerInboxAuthority, err)
	}
	if err := model.VerifyPublicationEvidence(originMember.PublicKey(), evidence); err != nil {
		return fmt.Errorf("%w: publication signature: %v", ErrPeerInboxAuthority, err)
	}
	return nil
}

func putPeerInboxTx(ctx context.Context, tx *sql.Tx, spec PutPeerInboxSpec,
	authority peerInboxArrivalAuthority, cursor PeerCursorProjection,
) (PutPeerInboxResult, error) {
	publication := spec.Publication
	event, scope := publication.Event(), publication.Event().Scope()
	evidence, err := model.ParsePublicationEvidence(publication.WireJSON().Bytes())
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: supported publication evidence: %v",
			ErrPeerInboxInput, err)
	}
	if !evidence.IsSupported() {
		return PutPeerInboxResult{}, fmt.Errorf("%w: publication lost supported semantics",
			ErrPeerInboxInput)
	}
	// A binding baseline deliberately covers history that predates membership;
	// unlike a contiguous post-baseline cursor, it does not require an Inbox row.
	if scope.ChannelSequence() <= cursor.BaselineChannelSequence {
		return PutPeerInboxResult{Disposition: PeerInboxCovered}, nil
	}
	quarantine, err := readPeerOriginQuarantine(ctx, tx, evidence)
	if err != nil {
		return PutPeerInboxResult{}, err
	}
	if quarantine != nil {
		if quarantine.publicationDigest == publication.Digest() {
			return PutPeerInboxResult{Disposition: PeerInboxConflicted,
				ConflictID: quarantine.conflictID}, nil
		}
		return PutPeerInboxResult{}, ErrPeerInboxQuarantined
	}
	incumbents, err := readPeerInboxIncumbents(ctx, tx, evidence)
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("put Peer Inbox: read incumbents: %w", err)
	}
	if len(incumbents) > 0 {
		if exactPeerInboxReplay(incumbents, evidence) {
			if incumbents[0].inboxID == "" {
				return PutPeerInboxResult{}, fmt.Errorf("%w: Event exists without permanent Inbox evidence",
					ErrPeerInboxConflict)
			}
			inboxID, err := model.ParseInboxID(incumbents[0].inboxID)
			if err != nil {
				return PutPeerInboxResult{}, fmt.Errorf("%w: durable Inbox ID: %v", ErrPeerInboxConflict, err)
			}
			return PutPeerInboxResult{InboxID: inboxID, Disposition: PeerInboxDuplicate}, nil
		}
		return recordPeerPublicationConflict(ctx, tx, evidence, spec.TransportPeerID,
			spec.ArrivalSource, spec.ReceivedAt, incumbents)
	}
	if scope.ChannelSequence() <= cursor.ContiguousChannelSequence {
		return PutPeerInboxResult{}, fmt.Errorf("%w: contiguous cursor lacks permanent Inbox evidence",
			ErrPeerInboxConflict)
	}
	inboxID, err := deterministicPeerInboxID(evidence)
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: Inbox ID: %v", ErrPeerInboxInput, err)
	}
	requiredRoots := peerInboxArtifactRoots(event)
	status, disposition := model.InboxStored, PeerInboxStored
	if !event.Audience().Contains(authority.node.PeerID()) {
		status, disposition = model.InboxIgnored, PeerInboxIgnored
	}
	inbox, err := model.NewPeerInbox(authority.node.PeerID(), model.PeerInboxSpec{ID: inboxID,
		Publication: publication, TransportPeerID: spec.TransportPeerID,
		ArrivalSource: spec.ArrivalSource, IsAudience: event.Audience().Contains(authority.node.PeerID()),
		RequiredArtifactRoots: requiredRoots, Status: status, NextAttemptAt: spec.ReceivedAt,
		ReceivedAt: spec.ReceivedAt, UpdatedAt: spec.ReceivedAt})
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: construct Inbox: %v", ErrPeerInboxInput, err)
	}
	rootJSON, err := model.JSONFrom(requiredRoots)
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: Artifact roots: %v", ErrPeerInboxInput, err)
	}
	var semanticNonce []byte
	if inbox.IsAudience() {
		semanticNonce = make([]byte, 32)
		if _, err := rand.Read(semanticNonce); err != nil {
			return PutPeerInboxResult{}, fmt.Errorf("put Peer Inbox: generate semantic nonce: %w", err)
		}
	}
	if err := insertPeerInbox(ctx, tx, inbox, semanticNonce, rootJSON); err != nil {
		return PutPeerInboxResult{}, err
	}
	return PutPeerInboxResult{InboxID: inboxID, Disposition: disposition}, nil
}

func putUnsupportedPeerInboxTx(ctx context.Context, tx *sql.Tx,
	evidence model.PublicationEvidence, transportPeerID model.PeerID,
	arrivalSource model.ArrivalSource, receivedAt time.Time,
	authority peerInboxArrivalAuthority, cursor PeerCursorProjection,
) (PutPeerInboxResult, error) {
	if evidence.IsZero() || evidence.IsSupported() {
		return PutPeerInboxResult{}, ErrPeerInboxInput
	}
	if evidence.ChannelSequence() <= cursor.BaselineChannelSequence {
		return PutPeerInboxResult{Disposition: PeerInboxCovered}, nil
	}
	quarantine, err := readPeerOriginQuarantine(ctx, tx, evidence)
	if err != nil {
		return PutPeerInboxResult{}, err
	}
	if quarantine != nil {
		if quarantine.publicationDigest == evidence.Digest() {
			return PutPeerInboxResult{Disposition: PeerInboxConflicted,
				ConflictID: quarantine.conflictID}, nil
		}
		return PutPeerInboxResult{}, ErrPeerInboxQuarantined
	}
	incumbents, err := readPeerInboxIncumbents(ctx, tx, evidence)
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("put unsupported Peer Inbox: read incumbents: %w", err)
	}
	if len(incumbents) > 0 {
		if exactPeerInboxReplay(incumbents, evidence) {
			if incumbents[0].inboxID == "" {
				return PutPeerInboxResult{}, fmt.Errorf("%w: Event exists without permanent Inbox evidence",
					ErrPeerInboxConflict)
			}
			inboxID, err := model.ParseInboxID(incumbents[0].inboxID)
			if err != nil {
				return PutPeerInboxResult{}, fmt.Errorf("%w: durable Inbox ID: %v",
					ErrPeerInboxConflict, err)
			}
			return PutPeerInboxResult{InboxID: inboxID, Disposition: PeerInboxDuplicate}, nil
		}
		return recordPeerPublicationConflict(ctx, tx, evidence, transportPeerID,
			arrivalSource, receivedAt, incumbents)
	}
	if evidence.ChannelSequence() <= cursor.ContiguousChannelSequence {
		return PutPeerInboxResult{}, fmt.Errorf("%w: contiguous cursor lacks permanent Inbox evidence",
			ErrPeerInboxConflict)
	}
	inboxID, err := deterministicPeerInboxID(evidence)
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: unsupported Inbox ID: %v", ErrPeerInboxInput, err)
	}
	if err := insertUnsupportedPeerInbox(ctx, tx, inboxID, evidence, transportPeerID,
		arrivalSource, receivedAt, evidence.Audience().Contains(authority.node.PeerID())); err != nil {
		return PutPeerInboxResult{}, err
	}
	return PutPeerInboxResult{InboxID: inboxID, Disposition: PeerInboxQuarantined}, nil
}

func activePeerInboxBinding(bindings []model.PeerBinding, peerID model.PeerID) (model.PeerBinding, bool) {
	for _, binding := range bindings {
		if binding.PeerID() == peerID && binding.State() == model.BindingActive {
			return binding, true
		}
	}
	return model.PeerBinding{}, false
}

func authenticatePublicationEvidenceRoster(roster model.VerifiedRoster,
	evidence model.PublicationEvidence,
) (model.Member, error) {
	members := roster.Members()
	head := evidence.PublicationRoster()
	if head.Revision() == 0 || head.Revision() > uint64(len(members)) ||
		members[head.Revision()-1].Head() != head {
		return model.Member{}, errors.New("publication roster is not a verified committed prefix")
	}
	current := make(map[model.PeerID]model.Member)
	for _, member := range members[:head.Revision()] {
		current[member.PeerID()] = member
	}
	origin, ok := current[evidence.OriginPeerID()]
	if !ok || origin.Status() != model.MemberActive || origin.OriginEpoch() != evidence.OriginEpoch() ||
		origin.Head() != evidence.OriginMember() {
		return model.Member{}, errors.New("origin is not active at the publication roster head")
	}
	for _, peerID := range evidence.Audience().Peers() {
		member, ok := current[peerID]
		if !ok || member.Status() != model.MemberActive {
			return model.Member{}, errors.New("audience contains a peer without publication-time authority")
		}
	}
	return origin, nil
}

func readPeerCursor(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	originPeerID model.PeerID, originEpoch model.OriginEpoch,
) (PeerCursorProjection, error) {
	var baseline, contiguous, observed uint64
	var updatedText string
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,contiguous_channel_seq,
		observed_channel_seq,updated_at FROM peer_cursors WHERE channel_id=? AND origin_peer_id=?
		AND origin_epoch=?`, channelID.String(), originPeerID.String(), originEpoch.String()).
		Scan(&baseline, &contiguous, &observed, &updatedText)
	updatedAt, parseErr := parseCanonicalStoreTime(updatedText)
	if err != nil || parseErr != nil || baseline > contiguous || contiguous > observed ||
		observed > model.MaxSQLiteInteger {
		return PeerCursorProjection{}, fmt.Errorf("invalid durable cursor: %v / %v", err, parseErr)
	}
	return PeerCursorProjection{ChannelID: channelID, OriginPeerID: originPeerID,
		OriginEpoch: originEpoch, BaselineChannelSequence: baseline,
		ContiguousChannelSequence: contiguous, ObservedChannelSequence: observed,
		UpdatedAt: updatedAt}, nil
}

type peerOriginQuarantine struct {
	conflictID        string
	publicationDigest model.Digest
}

func readPeerOriginQuarantine(ctx context.Context, tx *sql.Tx,
	evidence model.PublicationEvidence,
) (*peerOriginQuarantine, error) {
	var conflictID string
	var digestRaw []byte
	err := tx.QueryRowContext(ctx, `SELECT quarantine.first_conflict_id,
		conflict.claimed_publication_digest FROM origin_quarantines quarantine
		JOIN publication_conflicts conflict ON conflict.conflict_id=quarantine.first_conflict_id
		AND conflict.channel_id=quarantine.channel_id
		AND conflict.origin_peer_id=quarantine.origin_peer_id
		AND conflict.origin_epoch=quarantine.origin_epoch
		WHERE quarantine.channel_id=? AND quarantine.origin_peer_id=? AND quarantine.origin_epoch=?`,
		evidence.ChannelID().String(), evidence.OriginPeerID().String(), evidence.OriginEpoch().String()).
		Scan(&conflictID, &digestRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read quarantine: %v", ErrPeerInboxConflict, err)
	}
	digest, err := model.DigestFromBytes(digestRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: quarantine digest: %v", ErrPeerInboxConflict, err)
	}
	return &peerOriginQuarantine{conflictID: conflictID, publicationDigest: digest}, nil
}

func readPeerInboxIncumbents(ctx context.Context, tx *sql.Tx,
	evidence model.PublicationEvidence,
) ([]peerInboxIncumbent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT inbox_id,channel_id,origin_peer_id,origin_epoch,
		origin_seq,channel_seq,event_id,event_digest,publication_digest,origin_signature,publication_json
		FROM peer_inbox WHERE
		(origin_peer_id=? AND origin_epoch=? AND origin_seq=?) OR
		(channel_id=? AND origin_peer_id=? AND origin_epoch=? AND channel_seq=?) OR
		(event_id=?) ORDER BY inbox_id`,
		evidence.OriginPeerID().String(), evidence.OriginEpoch().String(), evidence.OriginSequence(),
		evidence.ChannelID().String(), evidence.OriginPeerID().String(), evidence.OriginEpoch().String(),
		evidence.ChannelSequence(), evidence.EventID().String())
	if err != nil {
		return nil, err
	}
	var result []peerInboxIncumbent
	for rows.Next() {
		var incumbent peerInboxIncumbent
		if err := rows.Scan(&incumbent.inboxID, &incumbent.channelID, &incumbent.originPeerID,
			&incumbent.originEpoch, &incumbent.originSequence, &incumbent.channelSequence,
			&incumbent.eventID, &incumbent.eventDigest, &incumbent.publicationDigest,
			&incumbent.signature, &incumbent.wire); err != nil {
			return nil, err
		}
		if err := validatePeerInboxIncumbent(incumbent); err != nil {
			return nil, err
		}
		result = append(result, incumbent)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT channel_id,origin_peer_id,origin_epoch,
		origin_seq,channel_seq,event_id,event_digest,publication_digest,origin_signature,
		canonical_publication_json FROM events event WHERE
		NOT EXISTS(SELECT 1 FROM peer_inbox inbox WHERE inbox.event_id=event.event_id) AND (
		(event.origin_peer_id=? AND event.origin_epoch=? AND event.origin_seq=?) OR
		(event.channel_id=? AND event.origin_peer_id=? AND event.origin_epoch=? AND event.channel_seq=?) OR
		(event.event_id=?)) ORDER BY event_id`,
		evidence.OriginPeerID().String(), evidence.OriginEpoch().String(), evidence.OriginSequence(),
		evidence.ChannelID().String(), evidence.OriginPeerID().String(), evidence.OriginEpoch().String(),
		evidence.ChannelSequence(), evidence.EventID().String())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var incumbent peerInboxIncumbent
		var bodyRaw []byte
		if err := rows.Scan(&incumbent.channelID, &incumbent.originPeerID,
			&incumbent.originEpoch, &incumbent.originSequence, &incumbent.channelSequence,
			&incumbent.eventID, &incumbent.eventDigest, &incumbent.publicationDigest,
			&incumbent.signature, &bodyRaw); err != nil {
			rows.Close()
			return nil, err
		}
		body, bodyErr := model.NewJSON(bodyRaw)
		digest, digestErr := model.DigestFromBytes(incumbent.publicationDigest)
		wire, wireErr := model.JSONFrom(struct {
			Publication       model.JSON   `json:"publication"`
			PublicationDigest model.Digest `json:"publication_digest"`
			OriginSignature   []byte       `json:"origin_signature"`
		}{body, digest, incumbent.signature})
		if bodyErr != nil || digestErr != nil || wireErr != nil || !bytes.Equal(body.Bytes(), bodyRaw) {
			rows.Close()
			return nil, fmt.Errorf("invalid durable Event publication projection: %v / %v / %v",
				bodyErr, digestErr, wireErr)
		}
		incumbent.wire = wire.Bytes()
		if err := validatePeerInboxIncumbent(incumbent); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, incumbent)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func validatePeerInboxIncumbent(incumbent peerInboxIncumbent) error {
	evidence, err := model.ParsePublicationEvidence(incumbent.wire)
	if err != nil {
		return fmt.Errorf("invalid durable publication evidence: %w", err)
	}
	if incumbent.channelID != evidence.ChannelID().String() ||
		incumbent.originPeerID != evidence.OriginPeerID().String() ||
		incumbent.originEpoch != evidence.OriginEpoch().String() ||
		incumbent.originSequence != evidence.OriginSequence() ||
		incumbent.channelSequence != evidence.ChannelSequence() ||
		incumbent.eventID != evidence.EventID().String() ||
		!bytes.Equal(incumbent.eventDigest, evidence.EventDigest().Bytes()) ||
		!bytes.Equal(incumbent.publicationDigest, evidence.Digest().Bytes()) ||
		!bytes.Equal(incumbent.signature, evidence.OriginSignature()) ||
		!bytes.Equal(incumbent.wire, evidence.WireJSON().Bytes()) {
		return errors.New("durable publication evidence projection mismatch")
	}
	if evidence.IsSupported() {
		if _, err := model.ParseSignedPublication(evidence.WireJSON().Bytes()); err != nil {
			return fmt.Errorf("invalid durable supported publication evidence: %w", err)
		}
	}
	return nil
}

func exactPeerInboxReplay(incumbents []peerInboxIncumbent, evidence model.PublicationEvidence) bool {
	if len(incumbents) != 1 {
		return false
	}
	incumbent := incumbents[0]
	return incumbent.channelID == evidence.ChannelID().String() &&
		incumbent.originPeerID == evidence.OriginPeerID().String() &&
		incumbent.originEpoch == evidence.OriginEpoch().String() &&
		incumbent.originSequence == evidence.OriginSequence() &&
		incumbent.channelSequence == evidence.ChannelSequence() &&
		incumbent.eventID == evidence.EventID().String() &&
		bytes.Equal(incumbent.eventDigest, evidence.EventDigest().Bytes()) &&
		bytes.Equal(incumbent.publicationDigest, evidence.Digest().Bytes()) &&
		bytes.Equal(incumbent.signature, evidence.OriginSignature()) &&
		bytes.Equal(incumbent.wire, evidence.WireJSON().Bytes())
}

func deterministicPeerInboxID(evidence model.PublicationEvidence) (model.InboxID, error) {
	identity, err := model.JSONFrom(struct {
		Domain      string            `json:"domain"`
		ChannelID   model.ChannelID   `json:"channel_id"`
		OriginPeer  model.PeerID      `json:"origin_peer_id"`
		OriginEpoch model.OriginEpoch `json:"origin_epoch"`
		ChannelSeq  uint64            `json:"channel_seq"`
		Digest      model.Digest      `json:"publication_digest"`
	}{"mnemon/r5/peer-inbox-id/1", evidence.ChannelID(), evidence.OriginPeerID(), evidence.OriginEpoch(),
		evidence.ChannelSequence(), evidence.Digest()})
	if err != nil {
		return model.InboxID{}, err
	}
	return model.ParseInboxID("inbox-" + model.Sum(identity.Bytes()).String()[len("sha256:"):])
}

func peerInboxArtifactRoots(event model.Event) []model.Digest {
	refs := event.Artifacts()
	result := make([]model.Digest, len(refs))
	for index, ref := range refs {
		result[index] = ref.RootDigest()
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result
}

func insertPeerInbox(ctx context.Context, tx *sql.Tx, inbox model.PeerInbox,
	semanticNonce []byte, requiredRoots model.JSON,
) error {
	publication, event := inbox.Publication(), inbox.Publication().Event()
	scope := event.Scope()
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_inbox(inbox_id,channel_id,transport_peer_id,
		origin_peer_id,origin_epoch,origin_seq,channel_seq,event_id,event_digest,
		origin_member_revision,origin_member_record_hash,publication_roster_revision,
		publication_roster_hash,publication_digest,origin_signature,publication_json,arrival_source,
		is_audience,semantic_nonce,required_artifact_roots_json,status,attempts,next_attempt_at,lease_owner,
		lease_until,local_event_id,decision_json,receipt_event_id,diagnostic,received_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,NULL,NULL,NULL,NULL,NULL,?,?,?)`,
		inbox.ID().String(), scope.ChannelID().String(), inbox.TransportPeerID().String(),
		scope.OriginPeerID().String(), scope.OriginEpoch().String(), scope.OriginSequence(),
		scope.ChannelSequence(), event.ID().String(), event.Digest().Bytes(),
		scope.OriginMember().Revision(), scope.OriginMember().Digest().Bytes(),
		scope.PublicationRoster().Revision(), scope.PublicationRoster().Digest().Bytes(),
		publication.Digest().Bytes(), publication.OriginSignature(), publication.WireJSON().Bytes(),
		string(inbox.ArrivalSource()), boolInt(inbox.IsAudience()), semanticNonce, requiredRoots.Bytes(),
		string(inbox.Status()), storeTime(inbox.NextAttemptAt()), nullText(inbox.Diagnostic()),
		storeTime(inbox.ReceivedAt()), storeTime(inbox.UpdatedAt()))
	if err != nil {
		return mapPeerInboxMutationError("insert Inbox", err)
	}
	return nil
}

func insertUnsupportedPeerInbox(ctx context.Context, tx *sql.Tx, inboxID model.InboxID,
	evidence model.PublicationEvidence, transportPeerID model.PeerID,
	arrivalSource model.ArrivalSource, receivedAt time.Time, isAudience bool,
) error {
	diagnostic := "unsupported_publication_schema"
	if evidence.SchemaVersion() == model.SchemaVersion {
		diagnostic = "invalid_publication_schema_v1"
	}
	emptyRoots, err := model.NewJSON([]byte(`[]`))
	if err != nil {
		return fmt.Errorf("%w: empty unsupported Artifact roots: %v", ErrPeerInboxInput, err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO peer_inbox(inbox_id,channel_id,transport_peer_id,
		origin_peer_id,origin_epoch,origin_seq,channel_seq,event_id,event_digest,
		origin_member_revision,origin_member_record_hash,publication_roster_revision,
		publication_roster_hash,publication_digest,origin_signature,publication_json,arrival_source,
		is_audience,semantic_nonce,required_artifact_roots_json,status,attempts,next_attempt_at,lease_owner,
		lease_until,local_event_id,decision_json,receipt_event_id,diagnostic,received_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,'quarantined',0,?,NULL,NULL,NULL,NULL,NULL,?,?,?)`,
		inboxID.String(), evidence.ChannelID().String(), transportPeerID.String(),
		evidence.OriginPeerID().String(), evidence.OriginEpoch().String(), evidence.OriginSequence(),
		evidence.ChannelSequence(), evidence.EventID().String(), evidence.EventDigest().Bytes(),
		evidence.OriginMember().Revision(), evidence.OriginMember().Digest().Bytes(),
		evidence.PublicationRoster().Revision(), evidence.PublicationRoster().Digest().Bytes(),
		evidence.Digest().Bytes(), evidence.OriginSignature(), evidence.WireJSON().Bytes(),
		string(arrivalSource), boolInt(isAudience), emptyRoots.Bytes(), storeTime(receivedAt), diagnostic,
		storeTime(receivedAt), storeTime(receivedAt))
	if err != nil {
		return mapPeerInboxMutationError("insert unsupported Inbox quarantine", err)
	}
	return nil
}

func advancePeerCursor(ctx context.Context, tx *sql.Tx, cursor PeerCursorProjection,
	observed uint64, at time.Time, scheduleLiveRepair bool,
) (PeerCursorProjection, error) {
	if observed > cursor.ObservedChannelSequence {
		cursor.ObservedChannelSequence = observed
	}
	// Probe only the next expected sequence. A far-ahead arrival therefore costs
	// one indexed lookup; filling a gap crosses each newly contiguous sequence
	// once over the cursor's lifetime. Conflict evidence is durable coverage too.
	for cursor.ContiguousChannelSequence < cursor.ObservedChannelSequence {
		next := cursor.ContiguousChannelSequence + 1
		var covered int
		err := tx.QueryRowContext(ctx, `SELECT CASE WHEN
			EXISTS(SELECT 1 FROM peer_inbox WHERE channel_id=? AND origin_peer_id=?
				AND origin_epoch=? AND channel_seq=?) OR
			EXISTS(SELECT 1 FROM publication_conflicts WHERE channel_id=? AND origin_peer_id=?
				AND origin_epoch=? AND claimed_channel_seq=?)
			THEN 1 ELSE 0 END`, cursor.ChannelID.String(), cursor.OriginPeerID.String(),
			cursor.OriginEpoch.String(), next, cursor.ChannelID.String(),
			cursor.OriginPeerID.String(), cursor.OriginEpoch.String(), next).
			Scan(&covered)
		if err != nil {
			return PeerCursorProjection{}, fmt.Errorf("%w: derive cursor coverage: %v", ErrPeerInboxConflict, err)
		}
		if covered == 0 {
			break
		}
		cursor.ContiguousChannelSequence = next
	}
	var durableContiguous, durableObserved uint64
	var updatedText string
	err := tx.QueryRowContext(ctx, `SELECT contiguous_channel_seq,observed_channel_seq,updated_at
		FROM peer_cursors WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`,
		cursor.ChannelID.String(), cursor.OriginPeerID.String(), cursor.OriginEpoch.String()).
		Scan(&durableContiguous, &durableObserved, &updatedText)
	if err != nil {
		return PeerCursorProjection{}, fmt.Errorf("%w: reread cursor: %v", ErrPeerInboxConflict, err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return PeerCursorProjection{}, fmt.Errorf("%w: cursor time: %v", ErrPeerInboxConflict, err)
	}
	if cursor.ContiguousChannelSequence == durableContiguous && cursor.ObservedChannelSequence == durableObserved {
		cursor.UpdatedAt = updatedAt
		return cursor, nil
	}
	if at.Before(updatedAt) {
		at = updatedAt
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_cursors SET contiguous_channel_seq=?,
		observed_channel_seq=?,updated_at=? WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?
		AND contiguous_channel_seq=? AND observed_channel_seq=? AND updated_at=?`,
		cursor.ContiguousChannelSequence, cursor.ObservedChannelSequence, storeTime(at),
		cursor.ChannelID.String(), cursor.OriginPeerID.String(), cursor.OriginEpoch.String(),
		durableContiguous, durableObserved, updatedText)
	if err != nil {
		return PeerCursorProjection{}, fmt.Errorf("update Peer Inbox cursor: %w", err)
	}
	if err := exactlyOne(result); err != nil {
		return PeerCursorProjection{}, fmt.Errorf("%w: update cursor: %v", ErrPeerInboxConflict, err)
	}
	cursor.UpdatedAt = at
	if scheduleLiveRepair {
		if err := schedulePeerRepairAfterLiveCursorAdvance(ctx, tx, cursor, at); err != nil {
			return PeerCursorProjection{}, err
		}
	}
	return cursor, nil
}

func schedulePeerRepairAfterLiveCursorAdvance(ctx context.Context, tx *sql.Tx,
	cursor PeerCursorProjection, at time.Time,
) error {
	// Live Gossip has durably advanced this receiver's inbound cursor, but the
	// origin only settles peer_deliveries through the direct Events repair ACK.
	// Make an already-caught-up/progress checkpoint due now without mutating
	// Pull-page repair transactions that already own their target fence.
	result, err := tx.ExecContext(ctx, `UPDATE peer_repairs
		SET generation=generation+1,next_attempt_at=?,updated_at=?
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?
		AND status IN ('progress','caught_up') AND next_attempt_at>? AND updated_at<=?`,
		storeTime(at), storeTime(at), cursor.ChannelID.String(), cursor.OriginPeerID.String(),
		cursor.OriginEpoch.String(), storeTime(at), storeTime(at))
	if err != nil {
		return fmt.Errorf("%w: schedule live cursor repair: %v", ErrPeerInboxConflict, err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("%w: schedule live cursor repair rows: %v", ErrPeerInboxConflict, err)
	}
	return nil
}

func recordPeerPublicationConflict(ctx context.Context, tx *sql.Tx,
	evidence model.PublicationEvidence, transportPeerID model.PeerID,
	arrivalSource model.ArrivalSource, receivedAt time.Time, incumbents []peerInboxIncumbent,
) (PutPeerInboxResult, error) {
	reason := peerPublicationConflictReason(incumbents, evidence)
	conflictIdentity, err := model.JSONFrom(struct {
		Domain      string            `json:"domain"`
		ChannelID   model.ChannelID   `json:"channel_id"`
		OriginPeer  model.PeerID      `json:"origin_peer_id"`
		OriginEpoch model.OriginEpoch `json:"origin_epoch"`
		Digest      model.Digest      `json:"publication_digest"`
	}{"mnemon/r5/publication-conflict-id/1", evidence.ChannelID(), evidence.OriginPeerID(),
		evidence.OriginEpoch(), evidence.Digest()})
	if err != nil {
		return PutPeerInboxResult{}, fmt.Errorf("%w: conflict identity: %v", ErrPeerInboxConflict, err)
	}
	conflictID := "publication-conflict-" + model.Sum(conflictIdentity.Bytes()).String()[len("sha256:"):]
	var existing any
	for _, incumbent := range incumbents {
		if incumbent.channelID == evidence.ChannelID().String() &&
			incumbent.originPeerID == evidence.OriginPeerID().String() &&
			incumbent.originEpoch == evidence.OriginEpoch().String() && incumbent.inboxID != "" {
			existing = incumbent.inboxID
			break
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO publication_conflicts(conflict_id,channel_id,
		origin_peer_id,origin_epoch,claimed_origin_seq,claimed_channel_seq,claimed_event_id,
		claimed_event_digest,claimed_publication_digest,origin_signature,
		conflicting_publication_json,transport_peer_id,arrival_source,existing_inbox_id,reason,detected_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, conflictID, evidence.ChannelID().String(),
		evidence.OriginPeerID().String(), evidence.OriginEpoch().String(), evidence.OriginSequence(),
		evidence.ChannelSequence(), evidence.EventID().String(), evidence.EventDigest().Bytes(), evidence.Digest().Bytes(),
		evidence.OriginSignature(), evidence.WireJSON().Bytes(), transportPeerID.String(),
		string(arrivalSource), existing, reason, storeTime(receivedAt))
	if err != nil {
		return PutPeerInboxResult{}, mapPeerInboxMutationError("insert publication conflict", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO origin_quarantines(channel_id,origin_peer_id,
		origin_epoch,first_conflict_id,reason,detected_at) VALUES(?,?,?,?,?,?)`,
		evidence.ChannelID().String(), evidence.OriginPeerID().String(), evidence.OriginEpoch().String(),
		conflictID, reason, storeTime(receivedAt))
	if err != nil {
		return PutPeerInboxResult{}, mapPeerInboxMutationError("quarantine publication origin", err)
	}
	return PutPeerInboxResult{Disposition: PeerInboxConflicted, ConflictID: conflictID}, nil
}

func peerPublicationConflictReason(incumbents []peerInboxIncumbent,
	evidence model.PublicationEvidence,
) string {
	for _, incumbent := range incumbents {
		if incumbent.channelID == evidence.ChannelID().String() &&
			incumbent.originPeerID == evidence.OriginPeerID().String() &&
			incumbent.originEpoch == evidence.OriginEpoch().String() &&
			incumbent.originSequence == evidence.OriginSequence() &&
			incumbent.channelSequence == evidence.ChannelSequence() &&
			incumbent.eventID == evidence.EventID().String() {
			return "digest_conflict"
		}
	}
	for _, incumbent := range incumbents {
		if incumbent.originPeerID == evidence.OriginPeerID().String() &&
			incumbent.originEpoch == evidence.OriginEpoch().String() &&
			incumbent.originSequence == evidence.OriginSequence() {
			return "origin_key_conflict"
		}
	}
	for _, incumbent := range incumbents {
		if incumbent.channelID == evidence.ChannelID().String() &&
			incumbent.originPeerID == evidence.OriginPeerID().String() &&
			incumbent.originEpoch == evidence.OriginEpoch().String() &&
			incumbent.channelSequence == evidence.ChannelSequence() {
			return "publication_key_conflict"
		}
	}
	for _, incumbent := range incumbents {
		if incumbent.eventID == evidence.EventID().String() {
			return "event_key_conflict"
		}
	}
	return "digest_conflict"
}

func mapPeerInboxMutationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "peer inbox pending budget exceeded") {
		return fmt.Errorf("%w: %s", ErrPeerInboxPressure, operation)
	}
	if strings.Contains(message, "constraint failed") ||
		strings.Contains(message, "inbox ") ||
		strings.Contains(message, "publication conflict") ||
		strings.Contains(message, "origin quarantine") {
		return fmt.Errorf("%w: %s: %v", ErrPeerInboxConflict, operation, err)
	}
	return fmt.Errorf("put Peer Inbox: %s: %w", operation, err)
}
