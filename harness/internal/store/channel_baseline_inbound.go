package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type InstallInboundChannelBaselineSpec struct {
	AuthenticatedPeerID model.PeerID
	Baseline            ChannelDataBaseline
	At                  time.Time
}

type InstallInboundChannelBaselineResult struct {
	Baseline    ChannelDataBaseline
	Installed   bool
	InstalledAt time.Time
}

// InstallInboundChannelBaseline installs a remote origin cursor and activates
// its pending binding in the same transaction. Exact replay is a read-only
// success; any different value for the immutable binding epoch fails closed.
func (s *Store) InstallInboundChannelBaseline(ctx context.Context,
	spec InstallInboundChannelBaselineSpec,
) (InstallInboundChannelBaselineResult, error) {
	baseline := spec.Baseline
	if s == nil || s.db == nil || ctx == nil || spec.AuthenticatedPeerID.IsZero() ||
		!validChannelDataBaseline(baseline) || spec.AuthenticatedPeerID != baseline.OriginPeerID {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	at, err := canonicalChannelBaselineTime(spec.At)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, fmt.Errorf("install inbound Channel baseline: begin: %w", err)
	}
	defer tx.Rollback()
	_, authority, binding, err := readChannelBaselineAuthority(ctx, tx, baseline.ChannelID,
		baseline.OriginPeerID)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}
	if binding.OriginEpoch() != baseline.OriginEpoch {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineEpochMismatch
	}
	if at.Before(authority.channel.UpdatedAt()) || binding.State() == model.BindingRevoked {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}

	replayed, found, err := replayInstalledInboundBaseline(ctx, tx, baseline, binding, at)
	if err != nil || found {
		return replayed, err
	}
	if binding.State() != model.BindingPending {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineConflict
	}
	if err := appendInboundCursorAndActivateBinding(ctx, tx, baseline, at); err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return InstallInboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"install inbound Channel baseline", err)
	}
	return InstallInboundChannelBaselineResult{Baseline: baseline, Installed: true, InstalledAt: at}, nil
}

// replayInstalledInboundBaseline treats an exact re-install of the durable
// cursor as a read-only success and any different value for the immutable
// baseline as a conflict.
func replayInstalledInboundBaseline(ctx context.Context, tx *sql.Tx,
	baseline ChannelDataBaseline, binding model.PeerBinding, at time.Time,
) (InstallInboundChannelBaselineResult, bool, error) {
	var installedSequence, contiguousSequence, observedSequence uint64
	var installedAtText string
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,contiguous_channel_seq,
		observed_channel_seq,updated_at FROM peer_cursors WHERE channel_id=? AND origin_peer_id=?
		AND origin_epoch=?`, baseline.ChannelID.String(), baseline.OriginPeerID.String(),
		baseline.OriginEpoch.String()).Scan(&installedSequence, &contiguousSequence,
		&observedSequence, &installedAtText)
	if errors.Is(err, sql.ErrNoRows) {
		return InstallInboundChannelBaselineResult{}, false, nil
	}
	if err != nil {
		return InstallInboundChannelBaselineResult{}, false,
			fmt.Errorf("install inbound Channel baseline: read replay: %w", err)
	}
	installedAt, parseErr := parseCanonicalStoreTime(installedAtText)
	if parseErr != nil || binding.State() != model.BindingActive ||
		installedSequence > model.MaxSQLiteInteger || contiguousSequence < installedSequence ||
		observedSequence < contiguousSequence || observedSequence > model.MaxSQLiteInteger {
		return InstallInboundChannelBaselineResult{}, false, ErrChannelBaselineAuthority
	}
	if installedSequence != baseline.BaselineChannelSequence {
		return InstallInboundChannelBaselineResult{}, false, ErrChannelBaselineConflict
	}
	if at.Before(installedAt) {
		return InstallInboundChannelBaselineResult{}, false, ErrChannelBaselineInput
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return InstallInboundChannelBaselineResult{}, false,
			fmt.Errorf("install inbound Channel baseline: commit replay: %w", commitErr)
	}
	return InstallInboundChannelBaselineResult{Baseline: baseline, InstalledAt: installedAt}, true, nil
}

// appendInboundCursorAndActivateBinding installs the remote origin cursor and
// activates its pending binding inside the same transaction, so a failed
// activation rolls the cursor back with it.
func appendInboundCursorAndActivateBinding(ctx context.Context, tx *sql.Tx,
	baseline ChannelDataBaseline, at time.Time,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,?,?,?,?)`, baseline.ChannelID.String(), baseline.OriginPeerID.String(),
		baseline.OriginEpoch.String(), baseline.BaselineChannelSequence,
		baseline.BaselineChannelSequence, baseline.BaselineChannelSequence, storeTime(at))
	if err != nil {
		return mapChannelBaselineMutationError("install inbound Channel baseline cursor", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_bindings SET state='active'
		WHERE channel_id=? AND peer_id=? AND origin_epoch=? AND state='pending'`,
		baseline.ChannelID.String(), baseline.OriginPeerID.String(), baseline.OriginEpoch.String())
	if err != nil || exactlyOne(result) != nil {
		if err == nil {
			err = errors.New("binding activation lost its pending authority")
		}
		return mapChannelBaselineMutationError("install inbound Channel baseline activation", err)
	}
	return nil
}
