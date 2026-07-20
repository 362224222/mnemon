package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRecordPeerInboxArtifactSourceIsFenceBoundAndReplaySafe(t *testing.T) {
	t.Parallel()
	fixture, claim, _, _ := newPeerInboxArtifactClosureClaim(t, "source-receipt", false)
	recordedAt := fixture.at.Add(2 * time.Second)
	receipt, err := fixture.store.RecordPeerInboxArtifactSource(context.Background(),
		RecordPeerInboxArtifactSourceSpec{Fence: claim.Fence(), SourcePeerID: claim.OriginPeerID(),
			At: recordedAt})
	if err != nil || !receipt.Changed() || receipt.Replayed() ||
		receipt.InboxID() != claim.InboxID() || receipt.SourcePeerID() != claim.OriginPeerID() ||
		!receipt.RecordedAt().Equal(recordedAt) {
		t.Fatalf("RecordPeerInboxArtifactSource() = (%#v,%v)", receipt, err)
	}

	replay, err := fixture.store.RecordPeerInboxArtifactSource(context.Background(),
		RecordPeerInboxArtifactSourceSpec{Fence: claim.Fence(), SourcePeerID: claim.OriginPeerID(),
			At: claim.Fence().LeaseUntil().Add(time.Second)})
	if err != nil || replay.Changed() || !replay.Replayed() ||
		replay.InboxID() != receipt.InboxID() || replay.SourcePeerID() != receipt.SourcePeerID() ||
		!replay.RecordedAt().Equal(receipt.RecordedAt()) {
		t.Fatalf("RecordPeerInboxArtifactSource() replay = (%#v,%v)", replay, err)
	}
	forged := claim.Fence()
	forged.attempt++
	if value, err := fixture.store.RecordPeerInboxArtifactSource(context.Background(),
		RecordPeerInboxArtifactSourceSpec{Fence: forged, SourcePeerID: claim.OriginPeerID(),
			At: recordedAt.Add(2 * time.Second)}); !errors.Is(err, ErrPeerInboxArtifactStale) ||
		value != (PeerInboxArtifactSourceReceipt{}) {
		t.Fatalf("RecordPeerInboxArtifactSource() different fence = (%#v,%v)", value, err)
	}

	var count int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_artifact_source_receipts
		WHERE inbox_id=?`, claim.InboxID().String()).Scan(&count); err != nil || count != 1 {
		t.Fatalf("Artifact source receipt count = (%d,%v)", count, err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE peer_inbox_artifact_source_receipts
		SET recorded_at=recorded_at WHERE inbox_id=?`, claim.InboxID().String()); err == nil {
		t.Fatal("immutable Artifact source receipt accepted UPDATE")
	}
	if _, err := fixture.store.db.Exec(`DELETE FROM peer_inbox_artifact_source_receipts
		WHERE inbox_id=?`, claim.InboxID().String()); err == nil {
		t.Fatal("permanent Artifact source receipt accepted DELETE")
	}
}

func TestRecordPeerInboxArtifactSourceRejectsWrongOriginAndStaleFence(t *testing.T) {
	t.Parallel()
	t.Run("wrong authenticated source", func(t *testing.T) {
		fixture, claim, _, _ := newPeerInboxArtifactClosureClaim(t, "source-wrong-origin", false)
		_, err := fixture.store.RecordPeerInboxArtifactSource(context.Background(),
			RecordPeerInboxArtifactSourceSpec{Fence: claim.Fence(),
				SourcePeerID: fixture.channel.Owner().PeerID(), At: fixture.at.Add(2 * time.Second)})
		if !errors.Is(err, ErrPeerInboxArtifactInput) {
			t.Fatalf("wrong source error = %v", err)
		}
		if _, err := fixture.store.db.Exec(`INSERT INTO peer_inbox_artifact_source_receipts(
			inbox_id,source_peer_id,attempt,lease_owner,lease_until,recorded_at) VALUES(?,?,?,?,?,?)`,
			claim.InboxID().String(), fixture.channel.Owner().PeerID().String(), claim.Fence().Attempt(),
			claim.Fence().LeaseOwner(), storeTime(claim.Fence().LeaseUntil()),
			storeTime(fixture.at.Add(2*time.Second))); err == nil {
			t.Fatal("schema accepted an Artifact source different from the signed origin")
		}
	})
	t.Run("expired fence", func(t *testing.T) {
		fixture, claim, _, _ := newPeerInboxArtifactClosureClaim(t, "source-stale", false)
		_, err := fixture.store.RecordPeerInboxArtifactSource(context.Background(),
			RecordPeerInboxArtifactSourceSpec{Fence: claim.Fence(), SourcePeerID: claim.OriginPeerID(),
				At: claim.Fence().LeaseUntil()})
		if !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("expired fence error = %v", err)
		}
	})
}
