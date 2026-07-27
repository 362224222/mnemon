package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestOperationOldStageCleanupIgnoresCurrentLeaseExtension(t *testing.T) {
	st, _, base := newArtifactStageCleanupStore(t, "old-generation-lease")
	operation := reserveArtifactStageCleanupOperation(t, st,
		"operation-cleanup-old-generation", "run-cleanup-old-generation-lease",
		"owner-cleanup-old-generation", base)
	firstLease, _ := operation.LeaseUntil()
	first, err := st.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: operation.LeaseOwner(), LeaseUntil: firstLease, At: base})
	if err != nil {
		t.Fatal(err)
	}

	currentLease := firstLease.Add(10 * time.Minute)
	reclaimed, err := model.NewOperation(model.OperationSpec{
		ID: operation.ID(), ProfileID: operation.ProfileID(), AgentRunID: operation.AgentRunID(),
		ClientKeyHash: operation.ClientKeyHash(), Kind: operation.Kind(),
		RequestDigest: operation.RequestDigest(), Status: model.OperationStarted,
		LeaseOwner: "owner-cleanup-current-generation", LeaseUntil: &currentLease,
		CreatedAt: operation.CreatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReserveOperation(context.Background(), reclaimed, firstLease); err != nil {
		t.Fatal(err)
	}
	second, err := st.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: reclaimed.LeaseOwner(), LeaseUntil: currentLease, At: firstLease})
	if err != nil || second.Fence().Owner().Generation() != first.Fence().Owner().Generation()+1 {
		t.Fatalf("current operation stage = (%#v,%v)", second, err)
	}

	cutoff := firstLease.Add(time.Second)
	page, err := st.ScanArtifactStageCleanupCandidates(context.Background(),
		ScanArtifactStageCleanupSpec{Cutoff: cutoff, At: cutoff.Add(time.Second),
			MaxExamined: 2})
	if err != nil || len(page.Candidates()) != 1 ||
		page.Candidates()[0].Owner() != first.Fence().Owner() {
		t.Fatalf("old generation behind extended lease = (%#v,%v)", page, err)
	}
}
