package selector

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestSampleFramesUseStrictCanonicalClosedJSON(t *testing.T) {
	fixture := newProviderFixture(t)
	descriptor := fixture.descriptor("wire", mustProfile(t, 2, 2, 2, 4))
	nonce := agency.Sum([]byte("wire-nonce"))
	query := mustQuery(t, descriptor.id, 1, nonce)
	queryBytes, err := query.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	parsedQuery, err := ParseSampleQueryCanonical(queryBytes)
	if err != nil || parsedQuery != query {
		t.Fatalf("query round trip = %#v, err %v", parsedQuery, err)
	}

	source := descriptor.roster[1]
	vote, err := NewSampleVote(descriptor.id, 1, nonce, PreferenceB, source)
	if err != nil {
		t.Fatal(err)
	}
	voteBytes, err := vote.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	parsedVote, err := ParseSampleVoteCanonical(voteBytes)
	if err != nil || parsedVote != vote {
		t.Fatalf("vote round trip = %#v, err %v", parsedVote, err)
	}
	if _, err := AuthenticateSampleVote(descriptor.roster[2], parsedVote); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched authenticated identity error = %v, want ErrInvalid", err)
	}
	if _, err := AuthenticateSampleVote(source, parsedVote); err != nil {
		t.Fatalf("matching authenticated identity: %v", err)
	}

	assertRejectedFrame(t, "query unknown field",
		bytes.Replace(queryBytes, []byte("}"), []byte(",\"payload\":\"ignored\"}"), 1),
		func(value []byte) error { _, err := ParseSampleQueryCanonical(value); return err })
	assertRejectedFrame(t, "query noncanonical whitespace", append(queryBytes, '\n'),
		func(value []byte) error { _, err := ParseSampleQueryCanonical(value); return err })
	assertRejectedFrame(t, "vote unknown field",
		bytes.Replace(voteBytes, []byte("}"), []byte(",\"artifact\":\"forbidden\"}"), 1),
		func(value []byte) error { _, err := ParseSampleVoteCanonical(value); return err })
	assertRejectedFrame(t, "vote trailing value", append(voteBytes, []byte("{}")...),
		func(value []byte) error { _, err := ParseSampleVoteCanonical(value); return err })
}

func TestSampleFrameBoundsFailClosed(t *testing.T) {
	if _, err := ParseSampleQueryCanonical(bytes.Repeat([]byte{'x'},
		MaxSampleQueryFrameBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized query error = %v, want ErrLimit", err)
	}
	if _, err := ParseSampleVoteCanonical(bytes.Repeat([]byte{'x'},
		MaxSampleVoteFrameBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized vote error = %v, want ErrLimit", err)
	}
	selectionID := SelectionID{digest: agency.Sum([]byte("wire-bounds"))}
	nonce := agency.Sum([]byte("wire-bounds-nonce"))
	tooLate := mustQuery(t, selectionID, MaxRounds+1, nonce)
	if _, err := tooLate.CanonicalBytes(); !errors.Is(err, ErrLimit) {
		t.Fatalf("out-of-bounds query round error = %v, want ErrLimit", err)
	}
	source, err := NewParticipantID(strings.Repeat("<", MaxParticipantIDBytes))
	if err != nil {
		t.Fatal(err)
	}
	vote, err := NewSampleVote(selectionID, MaxRounds, nonce, PreferenceA, source)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := vote.CanonicalBytes()
	if err != nil || len(encoded) > MaxSampleVoteFrameBytes {
		t.Fatalf("maximum escaped participant vote bytes = %d, err %v", len(encoded), err)
	}
	if _, err := ParseSampleVoteCanonical(encoded); err != nil {
		t.Fatalf("parse maximum escaped participant: %v", err)
	}
}

func TestSampleResponderNoVotesWithoutLeakOrMutation(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	descriptor := fixture.descriptor("no-vote", profile)
	created, err := fixture.store.CreateOwnerSelection(fixture.ctx, descriptor, descriptor.roster[0])
	if err != nil {
		t.Fatal(err)
	}
	requester := descriptor.roster[1]
	nonce := agency.Sum([]byte("no-vote-nonce"))

	assertNoVote(t, fixture.store, fixture.ctx, requester,
		mustQuery(t, descriptor.id, 1, nonce))
	unchanged, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil || unchanged.Revision() != created.Revision() ||
		unchanged.Phase() != PhaseAwaitingSeed {
		t.Fatalf("awaiting selection changed = phase %q revision %d err %v",
			unchanged.Phase(), unchanged.Revision(), err)
	}

	unknown := fixture.descriptor("unknown-no-vote", profile)
	assertNoVote(t, fixture.store, fixture.ctx, requester,
		mustQuery(t, unknown.id, 1, nonce))

	seed := providerSeed(t, descriptor.id, "no-vote", PreferenceB)
	seeded, err := fixture.store.SeedSelection(fixture.ctx, descriptor.id, seed)
	if err != nil {
		t.Fatal(err)
	}
	nonRoster, err := NewParticipantID("authenticated-but-not-in-roster")
	if err != nil {
		t.Fatal(err)
	}
	assertNoVote(t, fixture.store, fixture.ctx, nonRoster,
		mustQuery(t, descriptor.id, 1, nonce))
	assertNoVote(t, fixture.store, fixture.ctx, requester,
		mustQuery(t, descriptor.id, profile.MaxRounds()+1, nonce))

	fixture.clock.Set(descriptor.ExpiresAt())
	assertNoVote(t, fixture.store, fixture.ctx, requester,
		mustQuery(t, descriptor.id, 1, nonce))
	after, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil || after.Revision() != seeded.Revision() || after.Phase() != PhaseActive {
		t.Fatalf("read-only responder changed expired selection = phase %q revision %d err %v",
			after.Phase(), after.Revision(), err)
	}
}

func TestSampleResponderVotesFromDurableActiveAndObservedState(t *testing.T) {
	fixture := newProviderFixture(t)
	profile := mustProfile(t, 2, 2, 2, 4)
	seeded, _ := fixture.createAndSeed("responder", profile, PreferenceB)
	descriptor := seeded.descriptor
	requester := descriptor.roster[1]
	query := mustQuery(t, descriptor.id, 1, agency.Sum([]byte("responder-query")))

	assertResponderVote(t, fixture.store, fixture.ctx, requester, query,
		PreferenceB, descriptor.roster[0])
	beforeRestart, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil {
		t.Fatal(err)
	}
	fixture.reopen()
	assertResponderVote(t, fixture.store, fixture.ctx, requester, query,
		PreferenceB, descriptor.roster[0])
	afterRestart, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil || afterRestart.Revision() != beforeRestart.Revision() {
		t.Fatalf("response across restart changed revision = %d, want %d, err %v",
			afterRestart.Revision(), beforeRestart.Revision(), err)
	}

	for round := uint32(0); round < profile.Threshold(); round++ {
		pending, err := fixture.store.FreezeRound(fixture.ctx, descriptor.id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ApplyObservations(fixture.ctx, pending,
			votesForPending(t, pending, PreferenceA)); err != nil {
			t.Fatal(err)
		}
	}
	observed, err := fixture.store.Selection(fixture.ctx, descriptor.id)
	if err != nil || observed.Phase() != PhaseObserved {
		t.Fatalf("observed selection phase = %q, err %v", observed.Phase(), err)
	}
	observedQuery := mustQuery(t, descriptor.id, profile.MaxRounds(),
		agency.Sum([]byte("observed-query")))
	assertResponderVote(t, fixture.store, fixture.ctx, requester, observedQuery,
		PreferenceA, descriptor.roster[0])
}

func assertRejectedFrame(t testing.TB, name string, value []byte,
	parse func([]byte) error,
) {
	t.Helper()
	if err := parse(value); !errors.Is(err, ErrInvalid) {
		t.Fatalf("%s error = %v, want ErrInvalid", name, err)
	}
}

func assertNoVote(t testing.TB, store *Store, ctx context.Context,
	requester ParticipantID, query SampleQuery,
) {
	t.Helper()
	response, err := store.RespondSampleQuery(ctx, requester, query)
	if err != nil || !response.IsNoVote() {
		t.Fatalf("response = %#v, err %v, want no-vote", response, err)
	}
	if vote, present := response.Vote(); present || vote != (SampleVote{}) {
		t.Fatalf("no-vote exposed vote %#v, present %v", vote, present)
	}
}

func assertResponderVote(t testing.TB, store *Store, ctx context.Context,
	requester ParticipantID, query SampleQuery, preference Preference, source ParticipantID,
) {
	t.Helper()
	response, err := store.RespondSampleQuery(ctx, requester, query)
	vote, present := response.Vote()
	if err != nil || !present || response.IsNoVote() {
		t.Fatalf("response = %#v, err %v, want vote", response, err)
	}
	if vote.SelectionID() != query.SelectionID() || vote.Round() != query.Round() ||
		vote.Nonce() != query.Nonce() || vote.Preference() != preference ||
		vote.ClaimedSource() != source {
		t.Fatalf("vote = %#v, want query %#v preference %s source %s",
			vote, query, preference, source.String())
	}
	encoded, err := vote.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseSampleVoteCanonical(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AuthenticateSampleVote(source, decoded); err != nil {
		t.Fatalf("authenticate responder vote: %v", err)
	}
}

func FuzzParseSampleQueryCanonical(f *testing.F) {
	selectionID := SelectionID{digest: agency.Sum([]byte("fuzz-query-selection"))}
	query, _ := NewSampleQuery(selectionID, 1, agency.Sum([]byte("fuzz-query-nonce")))
	canonical, _ := query.CanonicalBytes()
	f.Add(canonical)
	f.Fuzz(func(t *testing.T, value []byte) {
		parsed, err := ParseSampleQueryCanonical(value)
		if err != nil {
			return
		}
		reencoded, err := parsed.CanonicalBytes()
		if err != nil || !bytes.Equal(value, reencoded) {
			t.Fatalf("accepted noncanonical query: %q -> %q, err %v", value, reencoded, err)
		}
	})
}

func FuzzParseSampleVoteCanonical(f *testing.F) {
	selectionID := SelectionID{digest: agency.Sum([]byte("fuzz-vote-selection"))}
	source, _ := NewParticipantID("fuzz-peer")
	vote, _ := NewSampleVote(selectionID, 1, agency.Sum([]byte("fuzz-vote-nonce")),
		PreferenceA, source)
	canonical, _ := vote.CanonicalBytes()
	f.Add(canonical)
	f.Fuzz(func(t *testing.T, value []byte) {
		parsed, err := ParseSampleVoteCanonical(value)
		if err != nil {
			return
		}
		reencoded, err := parsed.CanonicalBytes()
		if err != nil || !bytes.Equal(value, reencoded) {
			t.Fatalf("accepted noncanonical vote: %q -> %q, err %v", value, reencoded, err)
		}
	})
}
