package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrPeerInboxPage = errors.New("invalid Peer Inbox Pull page")

type PutPeerInboxPageSpec struct {
	ChannelID            model.ChannelID
	OriginPeerID         model.PeerID
	OriginEpoch          model.OriginEpoch
	TransportPeerID      model.PeerID
	AfterChannelSequence uint64
	ScannedChannelSeq    uint64
	SourceFloor          uint64
	SourceHead           uint64
	Publications         []model.PublicationEvidence
	ReceivedAt           time.Time
}

type PutPeerInboxPageResult struct {
	Items       []PutPeerInboxResult
	Cursor      PeerCursorProjection
	Quarantined bool
}

// PutPeerInboxPage authenticates and commits one origin Pull page as a single
// SQLite unit. No item or cursor movement survives an ordinary page failure.
// Authenticated unsupported entries are terminally quarantined without domain
// effect; signed equivocation is durably retained and fences the origin epoch.
func (s *Store) PutPeerInboxPage(ctx context.Context,
	spec PutPeerInboxPageSpec,
) (PutPeerInboxPageResult, error) {
	receivedAt, err := validatePeerInboxPageSpec(s, ctx, spec)
	if err != nil {
		return PutPeerInboxPageResult{}, err
	}
	spec.ReceivedAt = receivedAt
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PutPeerInboxPageResult{}, fmt.Errorf("put Peer Inbox page: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readPeerInboxPageAuthority(ctx, tx, spec)
	if err != nil {
		return PutPeerInboxPageResult{}, err
	}
	cursor, err := readPeerCursor(ctx, tx, spec.ChannelID, spec.OriginPeerID, spec.OriginEpoch)
	if err != nil {
		return PutPeerInboxPageResult{}, fmt.Errorf("%w: inbound baseline: %v", ErrPeerInboxAuthority, err)
	}
	if cursor.ContiguousChannelSequence < spec.AfterChannelSequence {
		return PutPeerInboxPageResult{}, fmt.Errorf("%w: page starts beyond the durable cursor", ErrPeerInboxPage)
	}

	results := make([]PutPeerInboxResult, 0, len(spec.Publications))
	observed := spec.ScannedChannelSeq
	quarantined := false
	for _, evidence := range spec.Publications {
		if err := authenticatePeerInboxEvidence(authority, evidence); err != nil {
			return PutPeerInboxPageResult{}, err
		}
		var result PutPeerInboxResult
		if evidence.IsSupported() {
			publication, parseErr := model.ParseSignedPublication(evidence.WireJSON().Bytes())
			if parseErr != nil {
				return PutPeerInboxPageResult{}, fmt.Errorf("%w: supported publication: %v",
					ErrPeerInboxPage, parseErr)
			}
			arrival := PutPeerInboxSpec{Publication: publication,
				TransportPeerID: spec.TransportPeerID, ArrivalSource: model.ArrivalPull,
				ReceivedAt: spec.ReceivedAt}
			arrival, err = normalizePeerInboxSpec(arrival)
			if err == nil {
				result, err = putPeerInboxTx(ctx, tx, arrival, authority, cursor)
			}
		} else {
			result, err = putUnsupportedPeerInboxTx(ctx, tx, evidence, spec.TransportPeerID,
				model.ArrivalPull, spec.ReceivedAt, authority, cursor)
		}
		if err != nil {
			return PutPeerInboxPageResult{}, err
		}
		results = append(results, result)
		if result.Disposition == PeerInboxConflicted {
			observed = evidence.ChannelSequence()
			quarantined = true
			break
		}
		if result.Disposition == PeerInboxQuarantined {
			quarantined = true
		}
	}
	cursor, err = advancePeerCursor(ctx, tx, cursor, observed, spec.ReceivedAt)
	if err != nil {
		return PutPeerInboxPageResult{}, err
	}
	for index := range results {
		results[index].Cursor = cursor
	}
	if err := tx.Commit(); err != nil {
		return PutPeerInboxPageResult{}, fmt.Errorf("put Peer Inbox page: commit: %w", err)
	}
	return PutPeerInboxPageResult{Items: results, Cursor: cursor, Quarantined: quarantined}, nil
}

func validatePeerInboxPageSpec(s *Store, ctx context.Context,
	spec PutPeerInboxPageSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() ||
		spec.OriginPeerID.IsZero() || spec.OriginEpoch.IsZero() ||
		spec.TransportPeerID != spec.OriginPeerID || spec.SourceFloor == 0 ||
		spec.AfterChannelSequence > model.MaxSQLiteInteger ||
		spec.ScannedChannelSeq > model.MaxSQLiteInteger || spec.SourceHead > model.MaxSQLiteInteger ||
		spec.SourceFloor > spec.SourceHead+1 || spec.AfterChannelSequence > spec.ScannedChannelSeq ||
		spec.ScannedChannelSeq > spec.SourceHead ||
		spec.AfterChannelSequence < spec.SourceFloor-1 ||
		len(spec.Publications) > maxPeerPullPagePublications {
		return time.Time{}, ErrPeerInboxPage
	}
	receivedAt, err := canonicalStoreTime(spec.ReceivedAt)
	if err != nil || receivedAt.IsZero() {
		return time.Time{}, ErrPeerInboxPage
	}
	if len(spec.Publications) == 0 {
		if spec.ScannedChannelSeq != spec.AfterChannelSequence {
			return time.Time{}, ErrPeerInboxPage
		}
		return receivedAt, nil
	}
	expected := spec.AfterChannelSequence + 1
	totalBytes := 0
	for _, evidence := range spec.Publications {
		if evidence.IsZero() || evidence.ChannelID() != spec.ChannelID ||
			evidence.OriginPeerID() != spec.OriginPeerID ||
			evidence.OriginEpoch() != spec.OriginEpoch || evidence.ChannelSequence() != expected {
			return time.Time{}, ErrPeerInboxPage
		}
		if evidence.IsSupported() {
			publication, err := model.ParseSignedPublication(evidence.WireJSON().Bytes())
			if err != nil {
				return time.Time{}, fmt.Errorf("%w: publication: %v", ErrPeerInboxPage, err)
			}
			if _, err := model.ProjectImportedPublication(&publication); err != nil {
				return time.Time{}, fmt.Errorf("%w: publication: %v", ErrPeerInboxPage, err)
			}
		}
		totalBytes += len(evidence.WireJSON().Bytes())
		if totalBytes > maxPeerPullPageBytes {
			return time.Time{}, ErrPeerInboxPage
		}
		expected++
	}
	if expected-1 != spec.ScannedChannelSeq {
		return time.Time{}, ErrPeerInboxPage
	}
	return receivedAt, nil
}

func readPeerInboxPageAuthority(ctx context.Context, tx *sql.Tx,
	spec PutPeerInboxPageSpec,
) (peerInboxArrivalAuthority, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return peerInboxArrivalAuthority{}, fmt.Errorf("%w: Node: %v", ErrPeerInboxAuthority, err)
	}
	if node.PeerID() == spec.OriginPeerID || node.PeerID() == spec.TransportPeerID {
		return peerInboxArrivalAuthority{}, ErrPeerInboxPage
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), spec.ChannelID)
	if err != nil {
		return peerInboxArrivalAuthority{}, fmt.Errorf("%w: %v", ErrPeerInboxAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined ||
		spec.ReceivedAt.Before(authority.channel.UpdatedAt()) {
		return peerInboxArrivalAuthority{}, ErrPeerInboxAuthority
	}
	originBinding, ok := activePeerInboxBinding(authority.bindings, spec.OriginPeerID)
	if !ok || originBinding.OriginEpoch() != spec.OriginEpoch {
		return peerInboxArrivalAuthority{}, ErrPeerInboxAuthority
	}
	return peerInboxArrivalAuthority{node: node, channel: authority,
		originKey: originBinding.PublicKey()}, nil
}
