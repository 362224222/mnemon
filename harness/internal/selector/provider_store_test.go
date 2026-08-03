package selector

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

type providerFixture struct {
	t     *testing.T
	ctx   context.Context
	clock *providerTestClock
	path  string
	store *Store
}

type providerTestClock struct {
	mu    sync.Mutex
	value time.Time
}

type providerTestEntropy struct {
	mu     sync.Mutex
	reader io.Reader
}

type providerBlockingClock struct {
	value   time.Time
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newProviderBlockingClock(value time.Time) *providerBlockingClock {
	return &providerBlockingClock{value: value, entered: make(chan struct{}), release: make(chan struct{})}
}

func (c *providerBlockingClock) Now() time.Time {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.value
}

func assertMutationOwnsClock(t *testing.T, fixture *providerFixture, value time.Time,
	mutation func() error,
) error {
	t.Helper()
	clock := newProviderBlockingClock(value)
	fixture.store.now = clock.Now
	result := make(chan error, 1)
	go func() { result <- mutation() }()
	select {
	case <-clock.entered:
	case <-time.After(time.Second):
		t.Fatal("mutation did not read the controlled clock")
	}
	lockHeld := !fixture.store.mu.TryLock()
	if !lockHeld {
		fixture.store.mu.Unlock()
	}
	close(clock.release)
	err := <-result
	fixture.store.now = fixture.clock.Now
	if !lockHeld {
		t.Fatal("mutation read trusted time before acquiring the Store lock")
	}
	return err
}

func (e *providerTestEntropy) Read(value []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.reader.Read(value)
}

func (c *providerTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func (c *providerTestClock) Set(value time.Time) {
	c.mu.Lock()
	c.value = value
	c.mu.Unlock()
}

func newProviderFixture(t *testing.T) *providerFixture {
	t.Helper()
	clock := &providerTestClock{value: time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)}
	directory := t.TempDir()
	if err := os.Chmod(directory, providerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "selector.db")
	store, err := openStore(context.Background(), path, clock.Now,
		&providerTestEntropy{reader: bytes.NewReader(bytes.Repeat([]byte{1}, 128<<10))})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &providerFixture{t: t, ctx: context.Background(), clock: clock,
		path: path, store: store}
	t.Cleanup(func() { _ = fixture.store.Close() })
	return fixture
}

func (f *providerFixture) descriptor(name string, profile Profile) SelectionDescriptor {
	f.t.Helper()
	descriptor, err := NewSelectionDescriptor(agency.Sum([]byte("question-"+name)),
		agency.Sum([]byte("candidate-a-"+name)), agency.Sum([]byte("candidate-b-"+name)),
		testPeers(f.t, int(profile.SampleSize())*MinEligiblePeersPerSample+1), profile,
		f.clock.Now(), f.clock.Now().Add(time.Hour))
	if err != nil {
		f.t.Fatal(err)
	}
	return descriptor
}

func (f *providerFixture) createAndSeed(name string, profile Profile,
	preference Preference,
) (SelectionSnapshot, AcceptedSeedOpinion) {
	f.t.Helper()
	descriptor := f.descriptor(name, profile)
	created, err := f.store.CreateOwnerSelection(f.ctx, descriptor, descriptor.roster[0])
	if err != nil {
		f.t.Fatal(err)
	}
	seed := providerSeed(f.t, name, preference)
	seeded, err := f.store.SeedSelection(f.ctx, created.descriptor.id, seed)
	if err != nil {
		f.t.Fatal(err)
	}
	return seeded, seed
}

func providerSeed(t testing.TB, name string, preference Preference) AcceptedSeedOpinion {
	t.Helper()
	principal, err := agency.NewAgentPrincipalID("principal-" + name)
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := agency.NewEventID("event-" + name)
	if err != nil {
		t.Fatal(err)
	}
	event, err := agency.NewEventRef(eventID, agency.Sum([]byte("accepted-event-"+name)))
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewAcceptedSeedOpinion(principal, event, preference)
	if err != nil {
		t.Fatal(err)
	}
	return seed
}

func votesForPending(t testing.TB, pending PendingRound, preference Preference) []AuthenticatedVote {
	t.Helper()
	sample := pending.Sample()
	votes := make([]AuthenticatedVote, len(sample))
	for index, peer := range sample {
		wire, err := NewSampleVote(pending.query.selectionID, pending.query.round,
			pending.query.nonce, preference, peer)
		if err != nil {
			t.Fatal(err)
		}
		vote, err := AuthenticateSampleVote(peer, wire)
		if err != nil {
			t.Fatal(err)
		}
		votes[index] = vote
	}
	return votes
}

func TestProviderPersistsOwnerSeedAndPendingRound(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	descriptor := fixture.descriptor("lifecycle", profile)
	created, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor, descriptor.roster[0])
	if err != nil || created.Phase() != PhaseAwaitingSeed || created.Revision() != 1 {
		t.Fatalf("create = phase %q revision %d err %v", created.Phase(), created.Revision(), err)
	}
	if _, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id); !errors.Is(err, ErrNotActive) {
		t.Fatalf("unseeded freeze error = %v", err)
	}
	seed := providerSeed(t, "lifecycle", PreferenceB)
	seeded, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed)
	if err != nil || seeded.Phase() != PhaseActive || seeded.Revision() != 2 {
		t.Fatalf("seed = phase %q revision %d err %v", seeded.Phase(), seeded.Revision(), err)
	}
	if gotSeed, ok := seeded.Seed(); !ok || !sameSeed(gotSeed, seed) {
		t.Fatalf("seed provenance = %#v, %v", gotSeed, ok)
	}
	if replay, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed); err != nil ||
		replay.Revision() != seeded.Revision() {
		t.Fatalf("seed replay = revision %d err %v", replay.Revision(), err)
	}

	first, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id)
	if err != nil || len(first.Sample()) != int(profile.SampleSize()) ||
		containsPeer(first.Sample(), descriptor.roster[0]) {
		t.Fatalf("first pending = %#v err %v", first, err)
	}
	if replay, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id); err != nil ||
		!samePending(first, replay) {
		t.Fatalf("pending replay changed = %#v err %v", replay, err)
	}

	fixture.reopen()
	restored, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if pending, ok := restored.PendingRound(); err != nil || !ok || !samePending(first, pending) {
		t.Fatalf("restored pending = %#v, %v, err %v", pending, ok, err)
	}
	if restored.Descriptor().ID() != descriptor.ID() ||
		!restored.Descriptor().CreatedAt().Equal(descriptor.CreatedAt()) {
		t.Fatalf("restored descriptor window = %s / %s, want %s / %s",
			restored.Descriptor().CreatedAt(), restored.Descriptor().ExpiresAt(),
			descriptor.CreatedAt(), descriptor.ExpiresAt())
	}
}

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

func TestProviderRejectsDescriptorCreatedAfterTrustedClock(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 1, 1)
	createdAt := fixture.clock.Now().Add(time.Minute)
	descriptor, err := NewSelectionDescriptor(agency.Sum([]byte("future-question")),
		agency.Sum([]byte("future-a")), agency.Sum([]byte("future-b")),
		testPeers(t, int(profile.SampleSize())*MinEligiblePeersPerSample+1), profile,
		createdAt, createdAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor,
		descriptor.roster[0]); !errors.Is(err, ErrActivation) {
		t.Fatalf("future descriptor activation error = %v, want ErrActivation", err)
	}
}

func TestProviderSettlementReplayBindsVoteSet(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	seeded, _ := fixture.createAndSeed("settlement", profile, PreferenceB)
	descriptor := seeded.descriptor
	first, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	afterFirst, err := fixture.store.ApplyObservations(fixture.ctx, first,
		votesForPending(t, first, PreferenceA))
	if err != nil || afterFirst.Phase() != PhaseActive {
		t.Fatalf("first apply = phase %q err %v", afterFirst.Phase(), err)
	}
	state, ok := afterFirst.State()
	if !ok || state.Round() != 1 || state.Margin() != 1 || state.Preference() != PreferenceA {
		t.Fatalf("first state = %#v, %v", state, ok)
	}
	if replay, err := fixture.store.ApplyObservations(fixture.ctx, first,
		votesForPending(t, first, PreferenceA)); err != nil ||
		replay.Revision() != afterFirst.Revision() {
		t.Fatalf("settled round replay = revision %d err %v", replay.Revision(), err)
	}
	if _, err := fixture.store.ApplyObservations(fixture.ctx, first,
		votesForPending(t, first, PreferenceB)); !errors.Is(err, ErrConflict) {
		t.Fatalf("settled round changed-vote error = %v", err)
	}
}

func TestProviderPersistsThresholdObservation(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	seeded, _ := fixture.createAndSeed("threshold", profile, PreferenceB)
	descriptor := seeded.descriptor
	first, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ApplyObservations(fixture.ctx, first,
		votesForPending(t, first, PreferenceA)); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	secondVotes := votesForPending(t, second, PreferenceA)
	observed, err := fixture.store.ApplyObservations(fixture.ctx, second, secondVotes)
	if err != nil || observed.Phase() != PhaseObserved {
		t.Fatalf("second apply = phase %q err %v", observed.Phase(), err)
	}
	observation, ok := observed.Observation()
	if preference, reached := observation.ThresholdPreference(); !ok || !reached ||
		preference != PreferenceA || observation.Digest() != agency.Sum(observation.CanonicalBytes()) {
		t.Fatalf("terminal observation = %#v, present %v", observation, ok)
	}
	if replay, err := fixture.store.ApplyObservations(fixture.ctx, second, secondVotes); err != nil ||
		replay.Revision() != observed.Revision() {
		t.Fatalf("terminal settlement replay = revision %d err %v", replay.Revision(), err)
	}
	fixture.reopen()
	restored, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if persisted, ok := restored.Observation(); err != nil || !ok ||
		persisted.Digest() != observation.Digest() {
		t.Fatalf("persisted observation = %#v, %v, err %v", persisted, ok, err)
	}
}

func TestProviderTimeoutIsNoMajorityAndStalePendingFailsClosed(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("timeout", mustProfile(t, 2, 2, 2, 3), PreferenceB)
	pending, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	stale := pending
	stale.stateRevision++
	if _, err := fixture.store.ApplyObservations(fixture.ctx, stale,
		votesForPending(t, pending, PreferenceA)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale pending error = %v", err)
	}
	fixture.clock.Set(pending.deadline)
	settled, err := fixture.store.ApplyObservations(fixture.ctx, pending,
		votesForPending(t, pending, PreferenceA))
	if err != nil {
		t.Fatal(err)
	}
	state, _ := settled.State()
	if state.Round() != 1 || state.Margin() != 0 || state.Preference() != PreferenceB {
		t.Fatalf("timed-out votes affected state = %#v", state)
	}
	if _, present := settled.PendingRound(); present {
		t.Fatal("timed-out round remained pending")
	}
}

func TestProviderTimedOutSettlementReplayIgnoresAllLateVotes(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("timeout-replay", mustProfile(t, 2, 2, 2, 3), PreferenceB)
	pending, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	fixture.clock.Set(pending.deadline)
	settled, err := fixture.store.ApplyObservations(fixture.ctx, pending,
		votesForPending(t, pending, PreferenceA))
	if err != nil {
		t.Fatal(err)
	}
	state, _ := settled.State()
	if state.Round() != 1 || state.Margin() != 0 || state.Preference() != PreferenceB {
		t.Fatalf("timed-out state = %#v", state)
	}
	for _, replayVotes := range [][]AuthenticatedVote{nil, votesForPending(t, pending, PreferenceB)} {
		replayed, err := fixture.store.ApplyObservations(fixture.ctx, pending, replayVotes)
		if err != nil || replayed.Revision() != settled.Revision() {
			t.Fatalf("late replay = revision %d err %v", replayed.Revision(), err)
		}
	}
}

func TestProviderExactSettlementReplaySurvivesLaterRounds(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("old-replay", mustProfile(t, 1, 1, 3, 4), PreferenceB)
	first, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	firstVotes := votesForPending(t, first, PreferenceA)
	if _, err := fixture.store.ApplyObservations(fixture.ctx, first, firstVotes); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	afterSecond, err := fixture.store.ApplyObservations(fixture.ctx, second,
		votesForPending(t, second, PreferenceB))
	if err != nil {
		t.Fatal(err)
	}
	fixture.reopen()
	replayed, err := fixture.store.ApplyObservations(fixture.ctx, first, firstVotes)
	if err != nil || replayed.Revision() != afterSecond.Revision() {
		t.Fatalf("old settlement replay = revision %d, want %d, err %v",
			replayed.Revision(), afterSecond.Revision(), err)
	}
}

func TestProviderClockIsReadWhileMutationOwnsStore(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		fixture := newProviderFixture(t)
		descriptor := fixture.descriptor("clock-create", mustProfile(t, 1, 1, 1, 1))
		err := assertMutationOwnsClock(t, fixture, fixture.clock.Now(), func() error {
			_, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor, descriptor.roster[0])
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("seed", func(t *testing.T) {
		fixture := newProviderFixture(t)
		descriptor := fixture.descriptor("clock-seed", mustProfile(t, 1, 1, 1, 1))
		if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor,
			descriptor.roster[0]); err != nil {
			t.Fatal(err)
		}
		seed := providerSeed(t, "clock-seed", PreferenceA)
		err := assertMutationOwnsClock(t, fixture, descriptor.ExpiresAt(), func() error {
			_, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed)
			return err
		})
		if !errors.Is(err, ErrNotActive) {
			t.Fatalf("seed at expiry error = %v", err)
		}
		if _, err := fixture.store.Selection(fixture.ctx, descriptor.id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expired unseeded selection remained = %v", err)
		}
	})
	t.Run("freeze", func(t *testing.T) {
		fixture := newProviderFixture(t)
		seeded, _ := fixture.createAndSeed("clock-freeze", mustProfile(t, 1, 1, 1, 1), PreferenceA)
		err := assertMutationOwnsClock(t, fixture, fixture.clock.Now(), func() error {
			_, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("apply", func(t *testing.T) {
		fixture := newProviderFixture(t)
		seeded, _ := fixture.createAndSeed("clock-apply", mustProfile(t, 1, 1, 1, 2), PreferenceA)
		pending, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
		if err != nil {
			t.Fatal(err)
		}
		votes := votesForPending(t, pending, PreferenceA)
		err = assertMutationOwnsClock(t, fixture, pending.deadline, func() error {
			_, err := fixture.store.ApplyObservations(fixture.ctx, pending, votes)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestProviderExpirationProducesOnlyObservationalResult(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("expiry", mustProfile(t, 2, 2, 1, 3), PreferenceA)
	fixture.clock.Set(seeded.descriptor.ExpiresAt())
	if _, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id); !errors.Is(err, ErrNotActive) {
		t.Fatalf("expired freeze error = %v", err)
	}
	selection, err := fixture.store.Selection(fixture.ctx, seeded.descriptor.id)
	if err != nil || selection.Phase() != PhaseObserved {
		t.Fatalf("expired selection = phase %q err %v", selection.Phase(), err)
	}
	observation, ok := selection.Observation()
	if !ok || observation.Result() != ObservationInconclusive || observation.Reason() != ReasonExpired {
		t.Fatalf("expiry observation = %#v, %v", observation, ok)
	}
}

func TestProviderActivationAndDurableCapacityAreBounded(t *testing.T) {
	fixture := newProviderFixture(t)
	small := mustDescriptor(t, mustProfile(t, 2, 2, 1, 2), testPeers(t, 4),
		fixture.clock.Now().Add(time.Hour))
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, small, small.roster[0]); !errors.Is(err, ErrActivation) {
		t.Fatalf("small-roster activation error = %v", err)
	}
	largeBudget := fixture.descriptor("budget", mustProfile(t, 2, 2, 1, 3000))
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, largeBudget, largeBudget.roster[0]); !errors.Is(err, ErrActivation) {
		t.Fatalf("message-budget activation error = %v", err)
	}

	profile := mustProfile(t, 1, 1, 1, 1)
	for index := 0; index < MaxActiveSelections; index++ {
		descriptor := fixture.descriptor(fmt.Sprintf("capacity-%02d", index), profile)
		if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor,
			descriptor.roster[0]); err != nil {
			t.Fatalf("create selection %d: %v", index, err)
		}
	}
	overflow := fixture.descriptor("capacity-overflow", profile)
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, overflow, overflow.roster[0]); !errors.Is(err, ErrStoreCapacity) {
		t.Fatalf("active capacity error = %v", err)
	}
}

func TestProviderExpiredAwaitingSelectionsDoNotConsumeCapacity(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 1, 1)
	first := fillProviderCapacity(t, fixture, profile, "awaiting", nil)
	fixture.clock.Set(fixture.clock.Now().Add(2 * time.Hour))
	createFreshProviderSelection(t, fixture, profile, "fresh-awaiting")
	if _, err := fixture.store.Selection(fixture.ctx, first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired unseeded selection remained = %v", err)
	}
}

func TestProviderExpiredActiveSelectionsSettleAndReleaseCapacity(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 1, 1)
	var firstPending PendingRound
	first := fillProviderCapacity(t, fixture, profile, "active", func(index int,
		created SelectionSnapshot,
	) {
		if _, err := fixture.store.SeedSelection(fixture.ctx, created.descriptor.id,
			providerSeed(t, fmt.Sprintf("expired-%02d", index), PreferenceA)); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			var err error
			firstPending, err = fixture.store.FreezeRound(fixture.ctx, created.descriptor.id)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
	fixture.clock.Set(fixture.clock.Now().Add(2 * time.Hour))
	createFreshProviderSelection(t, fixture, profile, "fresh-active")
	old, err := fixture.store.Selection(fixture.ctx, first)
	observation, present := old.Observation()
	if err != nil || old.Phase() != PhaseObserved || !present ||
		observation.Reason() != ReasonExpired {
		t.Fatalf("expired active selection = %#v observation %#v present %v err %v",
			old, observation, present, err)
	}
	if replay, err := fixture.store.ApplyObservations(fixture.ctx, firstPending,
		votesForPending(t, firstPending, PreferenceA)); err != nil ||
		replay.Revision() != old.Revision() {
		t.Fatalf("swept pending replay = revision %d, want %d, err %v",
			replay.Revision(), old.Revision(), err)
	}
}

func fillProviderCapacity(t *testing.T, fixture *providerFixture, profile Profile,
	prefix string, activate func(index int, created SelectionSnapshot),
) SelectionID {
	t.Helper()
	var first SelectionID
	for index := 0; index < MaxActiveSelections; index++ {
		descriptor := fixture.descriptor(fmt.Sprintf("expired-%s-%02d", prefix, index), profile)
		created, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor, descriptor.roster[0])
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			first = descriptor.id
		}
		if activate != nil {
			activate(index, created)
		}
	}
	return first
}

func createFreshProviderSelection(t *testing.T, fixture *providerFixture, profile Profile,
	name string,
) {
	t.Helper()
	fresh := fixture.descriptor(name, profile)
	if _, err := fixture.store.CreateOwnerSelection(fixture.ctx, fresh, fresh.roster[0]); err != nil {
		t.Fatalf("fresh creation after expiry = %v", err)
	}
}

func TestProviderPrivateRetentionIsBoundedAndOldestObservedIsPruned(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 1, 1)
	var first, latest SelectionID
	for index := 0; index < MaxStoredSelections+3; index++ {
		seeded, _ := fixture.createAndSeed(fmt.Sprintf("retained-%03d", index), profile, PreferenceA)
		if index == 0 {
			first = seeded.descriptor.id
		}
		latest = seeded.descriptor.id
		pending, err := fixture.store.FreezeRound(fixture.ctx, latest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ApplyObservations(fixture.ctx, pending,
			votesForPending(t, pending, PreferenceA)); err != nil {
			t.Fatal(err)
		}
		fixture.clock.Set(fixture.clock.Now().Add(time.Second))
	}
	if _, err := fixture.store.Selection(fixture.ctx, first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest observation was not pruned: %v", err)
	}
	if latestSelection, err := fixture.store.Selection(fixture.ctx, latest); err != nil ||
		latestSelection.Phase() != PhaseObserved {
		t.Fatalf("latest observation = phase %q err %v", latestSelection.Phase(), err)
	}
	fixture.store.mu.Lock()
	defer fixture.store.mu.Unlock()
	var selections, settlements int
	if err := fixture.store.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM selections), (SELECT COUNT(*) FROM settled_rounds)`).
		Scan(&selections, &settlements); err != nil {
		t.Fatal(err)
	}
	if selections != MaxStoredSelections || settlements > MaxStoredRoundSettlements {
		t.Fatalf("retained state = selections %d settlements %d", selections, settlements)
	}
}

func TestProviderPendingConcurrencyIsDurablyBounded(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 1, 2)
	selections := make([]SelectionSnapshot, MaxPendingRounds+1)
	for index := range selections {
		selections[index], _ = fixture.createAndSeed(fmt.Sprintf("pending-%02d", index),
			profile, PreferenceA)
	}
	for index := 0; index < MaxPendingRounds; index++ {
		if _, err := fixture.store.FreezeRound(fixture.ctx, selections[index].descriptor.id); err != nil {
			t.Fatalf("freeze %d: %v", index, err)
		}
	}
	if _, err := fixture.store.FreezeRound(fixture.ctx,
		selections[MaxPendingRounds].descriptor.id); !errors.Is(err, ErrStoreCapacity) {
		t.Fatalf("pending capacity error = %v", err)
	}
}

func TestProviderDuePendingRoundsSettleBeforeCapacityCheck(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 1, 1, 2, 3)
	selections := make([]SelectionSnapshot, MaxPendingRounds+1)
	pending := make([]PendingRound, MaxPendingRounds)
	for index := range selections {
		selections[index], _ = fixture.createAndSeed(fmt.Sprintf("due-pending-%02d", index),
			profile, PreferenceB)
	}
	for index := range pending {
		var err error
		pending[index], err = fixture.store.FreezeRound(fixture.ctx, selections[index].descriptor.id)
		if err != nil {
			t.Fatal(err)
		}
	}
	fixture.clock.Set(pending[0].deadline)
	if _, err := fixture.store.FreezeRound(fixture.ctx,
		selections[MaxPendingRounds].descriptor.id); err != nil {
		t.Fatalf("freeze after pending deadlines = %v", err)
	}
	settled, err := fixture.store.Selection(fixture.ctx, selections[0].descriptor.id)
	state, present := settled.State()
	if err != nil || !present || state.Round() != 1 || state.Margin() != 0 {
		t.Fatalf("due settlement = state %#v present %v err %v", state, present, err)
	}
	if _, present := settled.PendingRound(); present {
		t.Fatal("due pending round still occupies capacity")
	}
	if replay, err := fixture.store.ApplyObservations(fixture.ctx, pending[0],
		votesForPending(t, pending[0], PreferenceA)); err != nil ||
		replay.Revision() != settled.Revision() {
		t.Fatalf("due settlement replay = revision %d, want %d, err %v",
			replay.Revision(), settled.Revision(), err)
	}
}

func TestProviderConcurrentFreezeReturnsOneDurableRound(t *testing.T) {
	fixture := newProviderFixture(t)
	seeded, _ := fixture.createAndSeed("concurrent", mustProfile(t, 2, 2, 1, 2), PreferenceA)
	const callers = 8
	type freezeResult struct {
		pending PendingRound
		err     error
	}
	results := make(chan freezeResult, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			pending, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
			results <- freezeResult{pending: pending, err: err}
		}()
	}
	group.Wait()
	close(results)
	stored, err := fixture.store.Selection(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	want, ok := stored.PendingRound()
	if !ok {
		t.Fatal("concurrent freeze did not persist a round")
	}
	for result := range results {
		if result.err != nil || !result.pending.valid() {
			t.Fatalf("concurrent freeze returned pending %#v, err %v", result.pending, result.err)
		}
		if !samePending(result.pending, want) {
			t.Fatalf("concurrent freeze returned a different round: %#v", result.pending)
		}
	}
}

func TestProviderStoreIsPrivateSingleWriterAndRejectsCorruptRows(t *testing.T) {
	fixture := newProviderFixture(t)
	info, err := os.Stat(fixture.path)
	if err != nil || info.Mode().Perm() != providerFileMode {
		t.Fatalf("selector.db mode = %v, err %v", info.Mode().Perm(), err)
	}
	if _, err := OpenStore(fixture.ctx, fixture.path); err == nil {
		t.Fatal("second selector writer was accepted")
	}
	seeded, _ := fixture.createAndSeed("corrupt", mustProfile(t, 1, 1, 1, 2), PreferenceA)
	pending, err := fixture.store.FreezeRound(fixture.ctx, seeded.descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", providerSQLiteDSN(fixture.path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE pending_rounds SET sample_json = ? WHERE selection_id = ?",
		[]byte(`["not-in-roster"]`),
		seeded.descriptor.id.String()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store, err = openStore(fixture.ctx, fixture.path, fixture.clock.Now,
		&providerTestEntropy{reader: bytes.NewReader(bytes.Repeat([]byte{2}, 4096))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Selection(fixture.ctx, pending.query.selectionID); !errors.Is(err, ErrState) {
		t.Fatalf("corrupt pending row error = %v", err)
	}
}

func (f *providerFixture) reopen() {
	f.t.Helper()
	if err := f.store.Close(); err != nil {
		f.t.Fatal(err)
	}
	store, err := openStore(f.ctx, f.path, f.clock.Now,
		&providerTestEntropy{reader: bytes.NewReader(bytes.Repeat([]byte{2}, 128<<10))})
	if err != nil {
		f.t.Fatal(err)
	}
	f.store = store
}
