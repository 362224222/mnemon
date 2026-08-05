package selector

import (
	"bytes"
	"database/sql"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestProviderPersistsAwaitingSeedWithoutPreference(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	descriptor := fixture.descriptor("awaiting-seed", profile)
	created, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor, descriptor.roster[0])
	if err != nil || created.Phase() != PhaseAwaitingSeed || created.Revision() != 1 {
		t.Fatalf("create = phase %q revision %d err %v", created.Phase(), created.Revision(), err)
	}
	assertAwaitingSeedHasNoOpinion(t, created)
	fixture.reopen()
	restoredAwaiting, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil || restoredAwaiting.Phase() != PhaseAwaitingSeed || restoredAwaiting.Revision() != 1 {
		t.Fatalf("restored awaiting = phase %q revision %d err %v",
			restoredAwaiting.Phase(), restoredAwaiting.Revision(), err)
	}
	assertAwaitingSeedHasNoOpinion(t, restoredAwaiting)
	if _, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id); !errors.Is(err, ErrNotActive) {
		t.Fatalf("unseeded freeze error = %v", err)
	}
}

func TestProviderPersistsOwnerSeed(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	descriptor := fixture.descriptor("seed-lifecycle", profile)
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor,
		descriptor.roster[0]); err != nil {
		t.Fatal(err)
	}
	seed := providerSeed(t, descriptor.id, "seed-lifecycle", PreferenceB)
	seeded, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed)
	if err != nil || seeded.Phase() != PhaseActive || seeded.Revision() != 2 {
		t.Fatalf("seed = phase %q revision %d err %v", seeded.Phase(), seeded.Revision(), err)
	}
	assertBoundSeed(t, seeded, seed, 2)

	fixture.reopen()
	restored, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	assertBoundSeed(t, restored, seed, 2)
	if replay, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed); err != nil ||
		replay.Revision() != restored.Revision() {
		t.Fatalf("restored seed replay = revision %d err %v", replay.Revision(), err)
	}
}

func TestProviderSeedReplayIsStableAndDifferentSeedFailsClosed(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, seed := fixture.createAndSeed("seed-replay", mustProfile(t, 1, 1, 1, 1),
		PreferenceB)
	replay, err := fixture.store.SeedSelection(fixture.ctx, seeded.descriptor.id, seed)
	if err != nil || replay.Revision() != seeded.Revision() || replay.Phase() != PhaseActive {
		t.Fatalf("seed replay = revision %d err %v", replay.Revision(), err)
	}
	differentSeed := providerSeed(t, seeded.descriptor.id, "seed-replay-different", PreferenceA)
	if _, err := fixture.store.SeedSelection(fixture.ctx, seeded.descriptor.id,
		differentSeed); !errors.Is(err, ErrConflict) {
		t.Fatalf("different seed error = %v, want ErrConflict", err)
	}
	unchanged, err := fixture.store.Selection(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	assertBoundSeed(t, unchanged, seed, seeded.Revision())
}

func TestProviderConcurrentSeedReplayHasOneEffect(t *testing.T) {
	fixture := newProviderFixture(t)
	descriptor := fixture.descriptor("seed-concurrent", mustProfile(t, 1, 1, 1, 1))
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor,
		descriptor.roster[0]); err != nil {
		t.Fatal(err)
	}
	seed := providerSeed(t, descriptor.id, "seed-concurrent", PreferenceA)
	const callers = 8
	results := make(chan error, callers)
	var workers sync.WaitGroup
	workers.Add(callers)
	for range callers {
		go func() {
			defer workers.Done()
			_, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed)
			results <- err
		}()
	}
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent seed replay: %v", err)
		}
	}
	stored, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	assertBoundSeed(t, stored, seed, 2)
}

func TestProviderSeedMustBindExactSelection(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 1, 1)
	target := fixture.descriptor("seed-target", profile)
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, target,
		target.roster[0]); err != nil {
		t.Fatal(err)
	}
	other := fixture.descriptor("seed-other", profile)
	wrongSeed := providerSeed(t, other.id, "seed-target", PreferenceA)
	if _, err := fixture.store.SeedSelection(fixture.ctx, target.id,
		wrongSeed); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched seed error = %v, want ErrConflict", err)
	}
	unchanged, err := fixture.store.Selection(fixture.ctx, target.id)
	if err != nil || unchanged.Phase() != PhaseAwaitingSeed || unchanged.Revision() != 1 {
		t.Fatalf("target after mismatched seed = phase %q revision %d err %v",
			unchanged.Phase(), unchanged.Revision(), err)
	}
	assertAwaitingSeedHasNoOpinion(t, unchanged)
}

func TestSeedOpinionRequiresSelection(t *testing.T) {
	if _, err := NewSeedOpinion(SelectionID{}, PreferenceA); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero selection binding error = %v, want ErrInvalid", err)
	}
}

func TestProviderRejectsCorruptPersistedSeedBinding(t *testing.T) {
	tests := []struct {
		name   string
		slug   string
		update func(*testing.T, *sql.DB, SelectionSnapshot)
	}{
		{name: "different opinion digest", slug: "opinion", update: func(t *testing.T, db *sql.DB, seeded SelectionSnapshot) {
			if _, err := db.Exec(`UPDATE selections SET seed_opinion_digest = ? WHERE selection_id = ?`,
				agencySelectionID("different-opinion").String(), seeded.descriptor.id.String()); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProviderFixture(t)
			seeded, _ := fixture.createAndSeed("seed-corruption-"+test.slug,
				mustProfile(t, 1, 1, 1, 1), PreferenceA)
			if err := fixture.store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", providerSQLiteDSN(fixture.path))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA ignore_check_constraints = ON"); err != nil {
				t.Fatal(err)
			}
			test.update(t, db, seeded)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := openStore(fixture.ctx, fixture.path, fixture.clock.Now,
				&providerTestEntropy{reader: bytes.NewReader(bytes.Repeat([]byte{2}, 4096))})
			if err == nil {
				_, err = store.Selection(fixture.ctx, seeded.descriptor.id)
			}
			if store != nil {
				_ = store.Close()
			}
			if !errors.Is(err, ErrState) {
				t.Fatalf("corrupt persisted seed error = %v, want ErrState", err)
			}
		})
	}
}

func TestProviderRejectsCorruptAwaitingSeedState(t *testing.T) {
	fixture := newProviderFixture(t)
	descriptor := fixture.descriptor("awaiting-corruption", mustProfile(t, 1, 1, 1, 1))
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor,
		descriptor.roster[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.ExecContext(fixture.ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.ExecContext(fixture.ctx, `UPDATE selections
		SET signed_margin = 1 WHERE selection_id = ?`, descriptor.id.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Selection(fixture.ctx, descriptor.id); !errors.Is(err, ErrState) {
		t.Fatalf("corrupt awaiting state error = %v, want ErrState", err)
	}
}

func TestProviderRejectsMinInt64PersistedMargin(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("margin-min-int64",
		mustProfile(t, 1, 1, 1, 2), PreferenceB)
	if _, err := fixture.store.db.ExecContext(fixture.ctx, `UPDATE selections
		SET signed_margin = ?, completed_rounds = 0, current_preference = 'B'
		WHERE selection_id = ?`, int64(math.MinInt64), seeded.descriptor.id.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Selection(fixture.ctx,
		seeded.descriptor.id); !errors.Is(err, ErrState) {
		t.Fatalf("minimum int64 margin error = %v, want ErrState", err)
	}
}

func agencySelectionID(value string) SelectionID {
	return SelectionID{digest: agency.Sum([]byte(value))}
}

func assertAwaitingSeedHasNoOpinion(t testing.TB, snapshot SelectionSnapshot) {
	t.Helper()
	if seed, present := snapshot.Seed(); present {
		t.Fatalf("awaiting selection exposed seed %#v", seed)
	}
	if state, present := snapshot.State(); present {
		t.Fatalf("awaiting selection exposed preference state %#v", state)
	}
}

func assertBoundSeed(t testing.TB, snapshot SelectionSnapshot, want AcceptedSeedOpinion,
	wantRevision uint64,
) {
	t.Helper()
	seed, seedPresent := snapshot.Seed()
	state, statePresent := snapshot.State()
	if !seedPresent || !statePresent || snapshot.Revision() != wantRevision ||
		!sameSeed(seed, want) || seed.SelectionID() != snapshot.descriptor.id ||
		state.SelectionID() != snapshot.descriptor.id || state.Preference() != want.Preference() {
		t.Fatalf("seed binding = seed %#v present %v state %#v present %v revision %d",
			seed, seedPresent, state, statePresent, snapshot.Revision())
	}
}
