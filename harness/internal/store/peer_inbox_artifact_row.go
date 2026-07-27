package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type storedPeerInboxArtifactRow struct {
	inboxText             string
	channelText           string
	transportText         string
	originText            string
	epochText             string
	eventText             string
	originSequence        uint64
	channelSequence       uint64
	eventDigestRaw        []byte
	originMemberDigestRaw []byte
	rosterDigestRaw       []byte
	publicationDigestRaw  []byte
	originMemberRevision  uint64
	rosterRevision        uint64
	signature             []byte
	publicationRaw        []byte
	rootsRaw              []byte
	semanticNonceRaw      []byte
	audience              int
	statusText            string
	nextText              string
	receivedText          string
	updatedText           string
	attempts              int64
	leaseOwner            sql.NullString
	leaseUntil            sql.NullString
	localEvent            sql.NullString
	receipt               sql.NullString
	diagnostic            sql.NullString
	decision              []byte
}

func scanPeerInboxArtifactStoredRow(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (storedPeerInboxArtifactRow, error) {
	var stored storedPeerInboxArtifactRow
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,channel_id,transport_peer_id,origin_peer_id,
		origin_epoch,origin_seq,channel_seq,event_id,event_digest,origin_member_revision,
		origin_member_record_hash,publication_roster_revision,publication_roster_hash,
		publication_digest,origin_signature,publication_json,is_audience,semantic_nonce,
		required_artifact_roots_json,status,attempts,next_attempt_at,lease_owner,lease_until,
		local_event_id,decision_json,receipt_event_id,diagnostic,received_at,updated_at
		FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).Scan(
		&stored.inboxText, &stored.channelText, &stored.transportText, &stored.originText,
		&stored.epochText, &stored.originSequence, &stored.channelSequence, &stored.eventText,
		&stored.eventDigestRaw, &stored.originMemberRevision, &stored.originMemberDigestRaw,
		&stored.rosterRevision, &stored.rosterDigestRaw, &stored.publicationDigestRaw,
		&stored.signature, &stored.publicationRaw, &stored.audience, &stored.semanticNonceRaw,
		&stored.rootsRaw, &stored.statusText, &stored.attempts, &stored.nextText,
		&stored.leaseOwner, &stored.leaseUntil, &stored.localEvent, &stored.decision,
		&stored.receipt, &stored.diagnostic, &stored.receivedText, &stored.updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return storedPeerInboxArtifactRow{}, ErrPeerInboxArtifactStale
	}
	if err != nil {
		return storedPeerInboxArtifactRow{}, fmt.Errorf("read Peer Inbox Artifact: %w", err)
	}
	return stored, nil
}

func parsePeerInboxArtifactBase(stored storedPeerInboxArtifactRow,
	inboxID model.InboxID,
) (peerInboxArtifactRow, bool, error) {
	parsedInbox, inboxErr := model.ParseInboxID(stored.inboxText)
	channelID, channelErr := model.ParseChannelID(stored.channelText)
	transport, transportErr := model.ParsePeerID(stored.transportText)
	originPeer, originErr := model.ParsePeerID(stored.originText)
	originEpoch, epochErr := model.ParseOriginEpoch(stored.epochText)
	status := model.InboxStatus(stored.statusText)
	nextAttempt, nextErr := parseCanonicalStoreTime(stored.nextText)
	receivedAt, receivedErr := parseCanonicalStoreTime(stored.receivedText)
	updatedAt, updatedErr := parseCanonicalStoreTime(stored.updatedText)
	terminal := peerInboxArtifactTerminalStatus(status)
	if !validPeerInboxArtifactIdentity(inboxID, parsedInbox, inboxErr,
		channelErr, transport, transportErr, originErr, epochErr) {
		return peerInboxArtifactRow{}, false, malformedPeerInboxArtifactProjection()
	}
	if !validPeerInboxArtifactState(stored, status, nextAttempt, nextErr,
		receivedAt, receivedErr, updatedAt, updatedErr, terminal) {
		return peerInboxArtifactRow{}, false, malformedPeerInboxArtifactProjection()
	}
	return peerInboxArtifactRow{
		inboxID: parsedInbox, channelID: channelID, originPeerID: originPeer,
		originEpoch: originEpoch, status: status, attempts: uint32(stored.attempts),
		nextAttemptAt: nextAttempt, receivedAt: receivedAt, updatedAt: updatedAt,
	}, terminal, nil
}

func validPeerInboxArtifactIdentity(expected, parsed model.InboxID, inboxErr,
	channelErr error, transport model.PeerID, transportErr, originErr, epochErr error,
) bool {
	return inboxErr == nil && parsed == expected && channelErr == nil &&
		transportErr == nil && !transport.IsZero() && originErr == nil && epochErr == nil
}

func validPeerInboxArtifactState(stored storedPeerInboxArtifactRow,
	status model.InboxStatus, nextAttempt time.Time, nextErr error,
	receivedAt time.Time, receivedErr error, updatedAt time.Time, updatedErr error,
	terminal bool,
) bool {
	hasTerminalEvidence := stored.localEvent.Valid || stored.receipt.Valid ||
		len(stored.decision) != 0
	return status.Valid() && stored.attempts >= 0 &&
		uint64(stored.attempts) <= math.MaxUint32 && len(stored.semanticNonceRaw) == 32 &&
		nextErr == nil && receivedErr == nil && updatedErr == nil &&
		!updatedAt.Before(receivedAt) && stored.audience == 1 &&
		(terminal || !hasTerminalEvidence)
}

func malformedPeerInboxArtifactProjection() error {
	return fmt.Errorf("%w: malformed Inbox projection", ErrPeerInboxArtifactInvariant)
}

func peerInboxArtifactTerminalStatus(status model.InboxStatus) bool {
	return status == model.InboxAccepted ||
		status == model.InboxRejected ||
		status == model.InboxConflicted
}

func parsePeerInboxArtifactPublication(stored storedPeerInboxArtifactRow,
	base peerInboxArtifactRow,
) (model.SignedPublication, error) {
	incumbent := peerInboxIncumbent{
		inboxID: stored.inboxText, channelID: stored.channelText,
		originPeerID: stored.originText, originEpoch: stored.epochText,
		originSequence: stored.originSequence, channelSequence: stored.channelSequence,
		eventID: stored.eventText, eventDigest: stored.eventDigestRaw,
		publicationDigest: stored.publicationDigestRaw, signature: stored.signature,
		wire: stored.publicationRaw,
	}
	if err := validatePeerInboxIncumbent(incumbent); err != nil {
		return model.SignedPublication{}, fmt.Errorf("%w: signed publication tuple: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	parsed, err := model.ParseSignedPublication(stored.publicationRaw)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf("%w: signed publication: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	publication, err := model.ProjectImportedPublication(&parsed)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf("%w: imported publication: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requirePeerInboxArtifactPublicationAuthority(stored, base, publication); err != nil {
		return model.SignedPublication{}, err
	}
	return publication, nil
}

func requirePeerInboxArtifactPublicationAuthority(stored storedPeerInboxArtifactRow,
	base peerInboxArtifactRow, publication model.SignedPublication,
) error {
	originDigest, originDigestErr := model.DigestFromBytes(stored.originMemberDigestRaw)
	rosterDigest, rosterDigestErr := model.DigestFromBytes(stored.rosterDigestRaw)
	originHead, originHeadErr := model.NewRecordHead(stored.originMemberRevision, originDigest)
	rosterHead, rosterHeadErr := model.NewRecordHead(stored.rosterRevision, rosterDigest)
	scope := publication.Event().Scope()
	if originDigestErr != nil || rosterDigestErr != nil ||
		originHeadErr != nil || rosterHeadErr != nil {
		return fmt.Errorf("%w: publication authority tuple", ErrPeerInboxArtifactInvariant)
	}
	if scope.ChannelID() != base.channelID || scope.OriginPeerID() != base.originPeerID ||
		scope.OriginEpoch() != base.originEpoch || scope.OriginMember() != originHead ||
		scope.PublicationRoster() != rosterHead {
		return fmt.Errorf("%w: publication authority tuple", ErrPeerInboxArtifactInvariant)
	}
	return nil
}

func parsePeerInboxArtifactRoots(stored storedPeerInboxArtifactRow,
	publication model.SignedPublication,
) ([]model.Digest, error) {
	expected := peerInboxArtifactRoots(publication.Event())
	expectedJSON, err := model.JSONFrom(expected)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical roots: %v", ErrPeerInboxArtifactInvariant, err)
	}
	canonical, err := model.NewJSON(stored.rootsRaw)
	if err != nil || !bytes.Equal(canonical.Bytes(), stored.rootsRaw) ||
		!bytes.Equal(expectedJSON.Bytes(), stored.rootsRaw) {
		return nil, fmt.Errorf("%w: required roots differ from immutable Event",
			ErrPeerInboxArtifactInvariant)
	}
	return expected, nil
}

func parsePeerInboxArtifactLease(stored storedPeerInboxArtifactRow,
	row peerInboxArtifactRow,
) (time.Time, bool, error) {
	hasLease := stored.leaseOwner.Valid || stored.leaseUntil.Valid
	if stored.leaseOwner.Valid != stored.leaseUntil.Valid {
		return time.Time{}, false, fmt.Errorf("%w: partial Inbox lease",
			ErrPeerInboxArtifactInvariant)
	}
	if !hasLease {
		if row.status == model.InboxWaitingArtifact || row.status == model.InboxProcessing {
			return time.Time{}, false, fmt.Errorf("%w: missing Inbox lease",
				ErrPeerInboxArtifactInvariant)
		}
		return time.Time{}, false, nil
	}
	leaseUntil, err := parseCanonicalStoreTime(stored.leaseUntil.String)
	if err != nil || !validPublicationIdentifier(stored.leaseOwner.String) ||
		!leaseUntil.After(row.updatedAt) ||
		(row.status != model.InboxWaitingArtifact && row.status != model.InboxProcessing) {
		return time.Time{}, false, fmt.Errorf("%w: malformed Inbox lease",
			ErrPeerInboxArtifactInvariant)
	}
	return leaseUntil, true, nil
}

func requirePeerInboxArtifactDiagnostic(stored storedPeerInboxArtifactRow) error {
	if stored.diagnostic.Valid &&
		(stored.diagnostic.String == "" || !validPublicationDiagnostic(stored.diagnostic.String)) {
		return fmt.Errorf("%w: invalid Inbox diagnostic", ErrPeerInboxArtifactInvariant)
	}
	return nil
}

func requirePeerInboxArtifactTerminalProof(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, terminal bool,
) error {
	if !terminal {
		return nil
	}
	stored, found, err := readPeerInboxSemanticTerminalRow(ctx, tx, row.inboxID)
	if err != nil || !found || stored.status != row.status ||
		stored.attempt != row.attempts || stored.semanticNonce != row.semanticNonce ||
		!stored.updatedAt.Equal(row.updatedAt) ||
		!equalPeerInboxArtifactRoots(stored.requiredRoots, row.requiredRoots) {
		return fmt.Errorf("%w: malformed terminal Inbox proof: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	return nil
}
