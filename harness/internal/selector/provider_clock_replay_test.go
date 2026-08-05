package selector

import (
	"errors"
	"testing"
	"time"
)

func TestProviderPendingReplaySurvivesClockRollbackButCannotSettle(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("rollback-replay",
		mustProfile(t, 2, 2, 2, 4), PreferenceB)
	pending, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	before, err := fixture.store.Selection(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}

	fixture.clock.Set(seeded.descriptor.CreatedAt().Add(-time.Nanosecond))
	fixture.reopen()
	replayed, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil || !samePending(replayed, pending) {
		t.Fatalf("pending replay after clock rollback = %#v, err %v", replayed, err)
	}
	if _, err := fixture.store.ApplyObservations(fixture.ctx, replayed,
		votesForPending(t, replayed, PreferenceA)); !errors.Is(err, ErrState) {
		t.Fatalf("settlement before descriptor creation error = %v, want ErrState", err)
	}
	after, err := fixture.store.Selection(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	afterPending, present := after.PendingRound()
	if !present || !samePending(afterPending, pending) || after.Revision() != before.Revision() {
		t.Fatalf("rejected settlement changed durable state: before=%#v after=%#v", before, after)
	}
}
