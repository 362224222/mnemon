package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ChannelStatusProgress is a compact aggregate over the same read transaction
// as the signed Channel authority. Counts expose lifecycle distinctions without
// leaking payloads, lease capabilities, diagnostics, paths, or raw database rows.
type ChannelStatusProgress struct {
	readiness   []ChannelPeerReadiness
	commit      ChannelStatusCommitProgress
	publication ChannelStatusPublicationProgress
	cursor      ChannelStatusCursorProgress
	inbox       ChannelStatusInboxProgress
	artifact    ChannelStatusArtifactProgress
	runtime     ChannelStatusRuntimeProgress
	leave       ChannelStatusLeaveProgress
}

type ChannelStatusCommitProgress struct{ Accepted uint64 }

type ChannelStatusPublicationProgress struct {
	Queued             uint64
	Published          uint64
	Blocked            uint64
	RemotePending      uint64
	RemoteAcknowledged uint64
	RemoteBlocked      uint64
}

type ChannelStatusCursorProgress struct {
	InboundOrigins       uint64
	InboundCaughtUp      uint64
	InboundPending       uint64
	InboundTerminal      uint64
	InboundGapped        uint64
	OutboundPeers        uint64
	OutboundAcknowledged uint64
	OutboundPending      uint64
}

type ChannelStatusInboxProgress struct {
	Durable         uint64
	Pending         uint64
	WaitingArtifact uint64
	Accepted        uint64
	Rejected        uint64
	Conflicted      uint64
	Ignored         uint64
	Quarantined     uint64
}

type ChannelStatusArtifactProgress struct {
	PinnedRoots   uint64
	VerifiedRoots uint64
}

type ChannelStatusRuntimeProgress struct {
	HandlingPending   uint64
	HandlingClaimed   uint64
	HandlingCompleted uint64
	HandlingRejected  uint64
	HandlingDead      uint64
	RunActive         uint64
	RunCompleted      uint64
	RunRetry          uint64
	RunRejected       uint64
	RunFailed         uint64
}

type ChannelStatusLeaveProgress struct {
	Status     string
	Attempts   uint64
	Diagnostic ChannelLeaveFailureCode
}

func (progress ChannelStatusProgress) Readiness() []ChannelPeerReadiness {
	return append([]ChannelPeerReadiness(nil), progress.readiness...)
}
func (progress ChannelStatusProgress) Commit() ChannelStatusCommitProgress { return progress.commit }
func (progress ChannelStatusProgress) Publication() ChannelStatusPublicationProgress {
	return progress.publication
}
func (progress ChannelStatusProgress) Cursor() ChannelStatusCursorProgress { return progress.cursor }
func (progress ChannelStatusProgress) Inbox() ChannelStatusInboxProgress   { return progress.inbox }
func (progress ChannelStatusProgress) Artifact() ChannelStatusArtifactProgress {
	return progress.artifact
}
func (progress ChannelStatusProgress) Runtime() ChannelStatusRuntimeProgress { return progress.runtime }
func (progress ChannelStatusProgress) Leave() ChannelStatusLeaveProgress     { return progress.leave }
func (progress ChannelStatusProgress) clone() ChannelStatusProgress {
	progress.readiness = progress.Readiness()
	return progress
}

func readChannelStatusProgress(ctx context.Context, tx *sql.Tx, node model.Node,
	control ChannelControlChannel,
) (ChannelStatusProgress, error) {
	channelID := control.Channel().ID()
	progress := ChannelStatusProgress{}
	if control.Channel().Status() == model.ChannelActive {
		verified := verifiedChannelAuthority{channel: control.Channel(), roster: control.Roster(),
			bindings: control.Bindings()}
		readiness, err := readChannelBaselineReadinessSnapshot(ctx, tx, node, verified)
		if err != nil {
			return ChannelStatusProgress{}, statusProgressError("baseline readiness", err)
		}
		progress.readiness = readiness
	}
	readers := []func(context.Context, *sql.Tx, model.ChannelID, *ChannelStatusProgress) error{
		readChannelStatusPublicationProgress,
		readChannelStatusCursorProgress,
		readChannelStatusInboxProgress,
		readChannelStatusArtifactProgress,
		readChannelStatusRuntimeProgress,
	}
	for _, read := range readers {
		if err := read(ctx, tx, channelID, &progress); err != nil {
			return ChannelStatusProgress{}, err
		}
	}
	if err := readChannelStatusLeaveProgress(ctx, tx, channelID, node.PeerID(), &progress); err != nil {
		return ChannelStatusProgress{}, err
	}
	return progress, nil
}

func readChannelStatusLeaveProgress(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, localPeerID model.PeerID, progress *ChannelStatusProgress,
) error {
	var count, attempts, generation int64
	var status, diagnostic string
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(status),''),
		COALESCE(MAX(attempts),0),COALESCE(MAX(retry_generation),0),
		COALESCE(MAX(failure_code),'')
		FROM channel_leave_requests WHERE channel_id=? AND member_peer_id=?`,
		channelID.String(), localPeerID.String()).Scan(&count, &status, &attempts,
		&generation, &diagnostic)
	if err != nil || count < 0 || count > 1 || attempts < 0 ||
		uint64(attempts) > ChannelLeaveMaximumAttempts || generation < 0 ||
		uint64(generation) > model.MaxSQLiteInteger {
		return statusProgressError("leave retry", err)
	}
	if count == 0 {
		progress.leave.Status = "none"
		return nil
	}
	failure := ChannelLeaveFailureCode(diagnostic)
	if !validChannelLeaveRetryState(status, uint64(attempts), failure, diagnostic != "") {
		return statusProgressError("leave retry", nil)
	}
	progress.leave = ChannelStatusLeaveProgress{Status: status, Attempts: uint64(attempts),
		Diagnostic: failure}
	return nil
}

func readChannelStatusPublicationProgress(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, progress *ChannelStatusProgress,
) error {
	values := make([]int64, 7)
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE channel_id=? AND source='local'),
		(SELECT COUNT(*) FROM gossip_publications WHERE channel_id=? AND status IN ('queued','leased')),
		(SELECT COUNT(*) FROM gossip_publications WHERE channel_id=? AND status='published'),
		(SELECT COUNT(*) FROM gossip_publications WHERE channel_id=? AND status IN ('blocked','abandoned')),
		(SELECT COUNT(*) FROM peer_deliveries WHERE channel_id=? AND status='pending'),
		(SELECT COUNT(*) FROM peer_deliveries WHERE channel_id=? AND status='scanned'),
		(SELECT COUNT(*) FROM peer_deliveries WHERE channel_id=? AND status IN ('blocked','abandoned'))`,
		channelID.String(), channelID.String(), channelID.String(), channelID.String(),
		channelID.String(), channelID.String(), channelID.String()).Scan(statusCountDestinations(values)...)
	if err != nil || !validStatusCounts(values) {
		return statusProgressError("publication", err)
	}
	progress.commit.Accepted = uint64(values[0])
	progress.publication = ChannelStatusPublicationProgress{Queued: uint64(values[1]),
		Published: uint64(values[2]), Blocked: uint64(values[3]), RemotePending: uint64(values[4]),
		RemoteAcknowledged: uint64(values[5]), RemoteBlocked: uint64(values[6])}
	if progress.commit.Accepted != progress.publication.Queued+progress.publication.Published+
		progress.publication.Blocked {
		return statusProgressError("publication cardinality", nil)
	}
	return nil
}

func readChannelStatusCursorProgress(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, progress *ChannelStatusProgress,
) error {
	values := make([]int64, 8)
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM peer_cursors WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_repairs WHERE channel_id=? AND status='caught_up'),
		(SELECT COUNT(*) FROM peer_repairs WHERE channel_id=? AND status IN ('ready','progress','retry','paused')),
		(SELECT COUNT(*) FROM peer_repairs WHERE channel_id=? AND status='terminal'),
		(SELECT COUNT(*) FROM peer_cursors WHERE channel_id=? AND observed_channel_seq>contiguous_channel_seq),
		(SELECT COUNT(*) FROM peer_pull_acks WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_pull_acks WHERE channel_id=? AND baseline_confirmed_at IS NOT NULL),
		(SELECT COUNT(*) FROM peer_pull_acks WHERE channel_id=? AND baseline_confirmed_at IS NULL)`,
		channelID.String(), channelID.String(), channelID.String(), channelID.String(),
		channelID.String(), channelID.String(), channelID.String(), channelID.String()).
		Scan(statusCountDestinations(values)...)
	if err != nil || !validStatusCounts(values) {
		return statusProgressError("cursor", err)
	}
	progress.cursor = ChannelStatusCursorProgress{InboundOrigins: uint64(values[0]),
		InboundCaughtUp: uint64(values[1]), InboundPending: uint64(values[2]),
		InboundTerminal: uint64(values[3]), InboundGapped: uint64(values[4]),
		OutboundPeers: uint64(values[5]), OutboundAcknowledged: uint64(values[6]),
		OutboundPending: uint64(values[7])}
	if progress.cursor.InboundOrigins != progress.cursor.InboundCaughtUp+
		progress.cursor.InboundPending+progress.cursor.InboundTerminal ||
		progress.cursor.OutboundPeers != progress.cursor.OutboundAcknowledged+
			progress.cursor.OutboundPending {
		return statusProgressError("cursor cardinality", nil)
	}
	return nil
}

func readChannelStatusInboxProgress(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, progress *ChannelStatusProgress,
) error {
	values := make([]int64, 8)
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN status IN ('stored','waiting_artifact','ready','processing','retry') THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='waiting_artifact' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='accepted' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='conflicted' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='ignored' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='quarantined' THEN 1 ELSE 0 END),0)
		FROM peer_inbox WHERE channel_id=?`, channelID.String()).Scan(statusCountDestinations(values)...)
	if err != nil || !validStatusCounts(values) {
		return statusProgressError("Inbox", err)
	}
	progress.inbox = ChannelStatusInboxProgress{Durable: uint64(values[0]), Pending: uint64(values[1]),
		WaitingArtifact: uint64(values[2]), Accepted: uint64(values[3]), Rejected: uint64(values[4]),
		Conflicted: uint64(values[5]), Ignored: uint64(values[6]), Quarantined: uint64(values[7])}
	if progress.inbox.Durable != progress.inbox.Pending+progress.inbox.Accepted+
		progress.inbox.Rejected+progress.inbox.Conflicted+progress.inbox.Ignored+
		progress.inbox.Quarantined || progress.inbox.WaitingArtifact > progress.inbox.Pending {
		return statusProgressError("Inbox cardinality", nil)
	}
	return nil
}

func readChannelStatusArtifactProgress(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, progress *ChannelStatusProgress,
) error {
	var pinned, verified int64
	err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT pins.root_digest),
		COUNT(DISTINCT CASE WHEN roots.state='verified' THEN pins.root_digest END)
		FROM artifact_pins pins JOIN artifact_roots roots ON roots.root_digest=pins.root_digest
		WHERE (pins.owner_kind='event' AND EXISTS(SELECT 1 FROM events e WHERE e.event_id=pins.owner_id AND e.channel_id=?))
		OR (pins.owner_kind='publication' AND EXISTS(SELECT 1 FROM events e WHERE e.event_id=pins.owner_id AND e.channel_id=?))
		OR (pins.owner_kind='delivery' AND EXISTS(SELECT 1 FROM peer_deliveries d WHERE d.delivery_id=pins.owner_id AND d.channel_id=?))
		OR (pins.owner_kind='inbox' AND EXISTS(SELECT 1 FROM peer_inbox i WHERE i.inbox_id=pins.owner_id AND i.channel_id=?))
		OR (pins.owner_kind='handling' AND EXISTS(SELECT 1 FROM agent_handlings h JOIN events e ON e.event_id=h.event_id
			WHERE h.handling_id=pins.owner_id AND e.channel_id=?))`, channelID.String(), channelID.String(),
		channelID.String(), channelID.String(), channelID.String()).Scan(&pinned, &verified)
	if err != nil || !validStatusCounts([]int64{pinned, verified}) || verified > pinned {
		return statusProgressError("Artifact", err)
	}
	progress.artifact = ChannelStatusArtifactProgress{PinnedRoots: uint64(pinned),
		VerifiedRoots: uint64(verified)}
	return nil
}

func readChannelStatusRuntimeProgress(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, progress *ChannelStatusProgress,
) error {
	values := make([]int64, 10)
	err := tx.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN h.status='pending' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN h.status='claimed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN h.status='completed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN h.status='rejected' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN h.status='dead' THEN 1 ELSE 0 END),0),
		COUNT(DISTINCT CASE WHEN r.status IN ('starting','running','runtime_finished') THEN r.run_id END),
		COUNT(DISTINCT CASE WHEN r.status='outcome_accepted' THEN r.run_id END),
		COUNT(DISTINCT CASE WHEN r.status='requeued' THEN r.run_id END),
		COUNT(DISTINCT CASE WHEN r.status='rejected' THEN r.run_id END),
		COUNT(DISTINCT CASE WHEN r.status IN ('failed','dead') THEN r.run_id END)
		FROM agent_handlings h JOIN events e ON e.event_id=h.event_id
		LEFT JOIN agent_runs r ON r.handling_id=h.handling_id
			AND r.handling_attempt=h.attempts
			AND r.handling_recovery=h.recovery_count
		WHERE e.channel_id=?`, channelID.String()).
		Scan(statusCountDestinations(values)...)
	if err != nil || !validStatusCounts(values) {
		return statusProgressError("Runtime", err)
	}
	progress.runtime = ChannelStatusRuntimeProgress{HandlingPending: uint64(values[0]),
		HandlingClaimed: uint64(values[1]), HandlingCompleted: uint64(values[2]),
		HandlingRejected: uint64(values[3]), HandlingDead: uint64(values[4]),
		RunActive: uint64(values[5]), RunCompleted: uint64(values[6]), RunRetry: uint64(values[7]),
		RunRejected: uint64(values[8]), RunFailed: uint64(values[9])}
	return nil
}

func statusCountDestinations(values []int64) []any {
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	return destinations
}

func validStatusCounts(values []int64) bool {
	for _, value := range values {
		if value < 0 {
			return false
		}
	}
	return true
}

func statusProgressError(stage string, err error) error {
	if err == nil {
		return fmt.Errorf("%w: %s progress invariant", ErrChannelStatusAuthority, stage)
	}
	return fmt.Errorf("%w: read %s progress: %v", ErrChannelStatusAuthority, stage, err)
}
