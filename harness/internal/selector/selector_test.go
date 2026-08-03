package selector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestSelectionDescriptorCanonicalizesScope(t *testing.T) {
	profile := mustProfile(t, 3, 2, 2, 8)
	roster := testPeers(t, 5)
	expires := time.Date(2026, 8, 4, 1, 2, 3, 456_000_000, time.FixedZone("offset", 8*60*60))
	first := mustDescriptor(t, profile, []ParticipantID{roster[3], roster[0], roster[4], roster[2], roster[1]}, expires)
	second := mustDescriptor(t, profile, roster, expires.UTC())

	if first.ID() != second.ID() || !bytes.Equal(first.CanonicalBytes(), second.CanonicalBytes()) {
		t.Fatal("roster order or timezone changed canonical selection identity")
	}
	if first.ID().Digest() != agency.Sum(first.CanonicalBytes()) {
		t.Fatal("selection ID is not the descriptor digest")
	}
	if !first.CreatedAt().Equal(expires.UTC().Add(-time.Hour)) {
		t.Fatalf("created at = %s, want %s", first.CreatedAt(), expires.UTC().Add(-time.Hour))
	}
	canonical, err := canonicalJSONRoundTrip(first.CanonicalBytes())
	if err != nil || !bytes.Equal(canonical, first.CanonicalBytes()) {
		t.Fatalf("descriptor is not exact canonical JSON: %v", err)
	}
	gotRoster := first.ParticipantRoster()
	for index := 1; index < len(gotRoster); index++ {
		if gotRoster[index-1].String() >= gotRoster[index].String() {
			t.Fatalf("roster is not sorted and unique: %#v", gotRoster)
		}
	}
	gotRoster[0] = ParticipantID{}
	if first.ParticipantRoster()[0].IsZero() {
		t.Fatal("descriptor exposed mutable roster storage")
	}
	canonicalCopy := first.CanonicalBytes()
	canonicalCopy[0] = '!'
	if first.CanonicalBytes()[0] == '!' {
		t.Fatal("descriptor exposed mutable canonical bytes")
	}
}

func TestSelectionDescriptorLifetimeIsCanonicalAndBounded(t *testing.T) {
	createdAt := time.Date(2026, 8, 3, 1, 0, 0, 123, time.UTC)
	profile := mustProfile(t, 3, 2, 1, 2)
	roster := testPeers(t, 5)
	question := agency.Sum([]byte("question"))
	candidateA := agency.Sum([]byte("candidate-a"))
	candidateB := agency.Sum([]byte("candidate-b"))

	first, err := NewSelectionDescriptor(question, candidateA, candidateB, roster, profile,
		createdAt, createdAt.Add(MaxSelectionLifetime))
	if err != nil {
		t.Fatal(err)
	}
	shifted, err := NewSelectionDescriptor(question, candidateA, candidateB, roster, profile,
		createdAt.Add(time.Nanosecond), createdAt.Add(MaxSelectionLifetime))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() == shifted.ID() {
		t.Fatal("created_at is not bound into canonical selection identity")
	}
	for _, expiresAt := range []time.Time{createdAt, createdAt.Add(MaxSelectionLifetime + time.Nanosecond)} {
		if _, err := NewSelectionDescriptor(question, candidateA, candidateB, roster, profile,
			createdAt, expiresAt); !errors.Is(err, ErrLimit) {
			t.Fatalf("invalid lifetime ending %s error = %v, want ErrLimit", expiresAt, err)
		}
	}
}

func TestProfileAndDescriptorFailClosed(t *testing.T) {
	profileCases := []struct {
		name                        string
		k, alpha, threshold, rounds uint32
		timeout                     time.Duration
	}{
		{"zero sample", 0, 0, 1, 1, time.Second},
		{"alpha is not strict", 4, 2, 1, 2, time.Second},
		{"alpha exceeds sample", 3, 4, 1, 2, time.Second},
		{"zero threshold", 3, 2, 0, 2, time.Second},
		{"unreachable threshold", 3, 2, 3, 2, time.Second},
		{"zero rounds", 3, 2, 1, 0, time.Second},
		{"sub-millisecond timeout", 3, 2, 1, 2, time.Microsecond},
		{"nonintegral timeout", 3, 2, 1, 2, time.Millisecond + time.Microsecond},
		{"unbounded duration", 3, 2, 1, MaxRounds, MaxRoundTimeout},
	}
	for _, test := range profileCases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProfile(test.k, test.alpha, test.threshold, test.rounds, test.timeout); err == nil {
				t.Fatal("invalid profile was accepted")
			}
		})
	}

	profile := mustProfile(t, 3, 2, 1, 2)
	roster := testPeers(t, 4)
	createdAt := time.Now().UTC()
	expiresAt := createdAt.Add(time.Hour)
	if _, err := NewSelectionDescriptor(agency.Sum([]byte("question")), agency.Sum([]byte("same")),
		agency.Sum([]byte("same")), roster, profile, createdAt, expiresAt); err == nil {
		t.Fatal("identical candidates were accepted")
	}
	if _, err := NewSelectionDescriptor(agency.Sum([]byte("question")), agency.Sum([]byte("a")),
		agency.Sum([]byte("b")), roster[:3], profile, createdAt, expiresAt); err == nil {
		t.Fatal("roster that cannot exclude self from a full sample was accepted")
	}
	duplicate := append([]ParticipantID(nil), roster...)
	duplicate[3] = duplicate[2]
	if _, err := NewSelectionDescriptor(agency.Sum([]byte("question")), agency.Sum([]byte("a")),
		agency.Sum([]byte("b")), duplicate, profile, createdAt, expiresAt); err == nil {
		t.Fatal("duplicate roster peer was accepted")
	}
}

func TestApplyRoundRecolorsAndAccumulatesSignedMargin(t *testing.T) {
	now := time.Date(2026, 8, 3, 4, 0, 0, 0, time.UTC)
	profile := mustProfile(t, 3, 2, 2, 4)
	descriptor := mustDescriptor(t, profile, testPeers(t, 5), now.Add(time.Hour))
	roster := descriptor.ParticipantRoster()
	state := mustState(t, descriptor.ID(), PreferenceB)

	first := applyPreferences(t, descriptor, state, roster[0], roster[1:4], now, PreferenceA, PreferenceA, PreferenceB)
	if first.State().Preference() != PreferenceA || first.State().Margin() != 1 ||
		first.State().Round() != 1 || !first.Recolored() {
		t.Fatalf("first state = preference %s margin %d round %d recolored %v",
			first.State().Preference(), first.State().Margin(), first.State().Round(), first.Recolored())
	}
	if quorum, ok := first.Quorum(); !ok || quorum != PreferenceA {
		t.Fatalf("first quorum = %s, %v", quorum, ok)
	}

	second := applyPreferences(t, descriptor, first.State(), roster[0], roster[1:4], now, PreferenceB, PreferenceB)
	if second.State().Preference() != PreferenceB || second.State().Margin() != 0 || !second.Recolored() {
		t.Fatalf("second state = %#v", second.State())
	}
	third := applyPreferences(t, descriptor, second.State(), roster[0], roster[1:4], now, PreferenceA, PreferenceB)
	if third.State().Preference() != PreferenceB || third.State().Margin() != 0 || third.Recolored() {
		t.Fatalf("no-quorum state = %#v", third.State())
	}
}

func TestApplyRoundFiltersWrongDuplicateAndEquivocatingVotes(t *testing.T) {
	now := time.Date(2026, 8, 3, 5, 0, 0, 0, time.UTC)
	profile := mustProfile(t, 5, 3, 2, 4)
	descriptor := mustDescriptor(t, profile, testPeers(t, 7), now.Add(time.Hour))
	roster := descriptor.ParticipantRoster()
	state := mustState(t, descriptor.ID(), PreferenceB)
	nonce := agency.Sum([]byte("filter-round"))
	query := mustQuery(t, descriptor.ID(), 1, nonce)
	otherID, _ := ParseSelectionID(agency.Sum([]byte("other-selection")).String())
	wrongNonce := agency.Sum([]byte("wrong-nonce"))
	votes := []AuthenticatedVote{
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceA, roster[1]),
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceA, roster[1]),
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceB, roster[1]),
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceA, roster[2]),
		mustVote(t, otherID, 1, nonce, PreferenceA, roster[3]),
		mustVote(t, descriptor.ID(), 2, nonce, PreferenceA, roster[3]),
		mustVote(t, descriptor.ID(), 1, wrongNonce, PreferenceA, roster[3]),
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceA, roster[6]),
		{},
	}
	result, err := ApplyRound(descriptor, state, roster[0], query, roster[1:6], votes, now)
	if err != nil {
		t.Fatal(err)
	}
	want := VoteTally{a: 1, duplicates: 1, equivocations: 1, wrongSelection: 1,
		wrongRound: 1, wrongNonce: 1, unselected: 1, invalid: 1}
	if got := result.Tally(); got != want {
		t.Fatalf("tally = %#v, want %#v", got, want)
	}
	if result.State().Preference() != PreferenceB || result.State().Margin() != 0 {
		t.Fatalf("filtered votes changed state = %#v", result.State())
	}

	reversed := append([]AuthenticatedVote(nil), votes...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	reordered, err := ApplyRound(descriptor, state, roster[0], query, roster[1:6], reversed, now)
	if err != nil || reordered.Tally() != result.Tally() || reordered.State() != result.State() {
		t.Fatalf("vote order changed result: %#v / %#v / %v", reordered.Tally(), reordered.State(), err)
	}
}

func TestAuthenticatedVoteCannotImpersonateSampledPeers(t *testing.T) {
	now := time.Date(2026, 8, 3, 5, 30, 0, 0, time.UTC)
	descriptor := mustDescriptor(t, mustProfile(t, 3, 2, 2, 4), testPeers(t, 5), now.Add(time.Hour))
	roster := descriptor.ParticipantRoster()
	state := mustState(t, descriptor.ID(), PreferenceB)
	nonce := agency.Sum([]byte("authenticated-round"))
	query := mustQuery(t, descriptor.ID(), 1, nonce)
	sampled := roster[1:4]
	authenticatedPeer := sampled[0]

	var accepted []AuthenticatedVote
	for _, claimedPeer := range sampled {
		wire, err := NewSampleVote(descriptor.ID(), 1, nonce, PreferenceA, claimedPeer)
		if err != nil {
			t.Fatal(err)
		}
		vote, err := AuthenticateSampleVote(authenticatedPeer, wire)
		if claimedPeer == authenticatedPeer {
			if err != nil {
				t.Fatalf("matching authenticated source rejected: %v", err)
			}
			accepted = append(accepted, vote)
		} else if !errors.Is(err, ErrInvalid) {
			t.Fatalf("authenticated peer impersonated %s: %v", claimedPeer.String(), err)
		}
	}
	result, err := ApplyRound(descriptor, state, roster[0], query, sampled, accepted, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tally().A() != 1 || result.State().Margin() != 0 {
		t.Fatalf("one authenticated peer produced tally %#v and state %#v", result.Tally(), result.State())
	}
	duplicateClaims := make([]AuthenticatedVote, len(sampled))
	for index := range duplicateClaims {
		wire, err := NewSampleVote(descriptor.ID(), 1, nonce, PreferenceA, authenticatedPeer)
		if err != nil {
			t.Fatal(err)
		}
		duplicateClaims[index], err = AuthenticateSampleVote(authenticatedPeer, wire)
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err = ApplyRound(descriptor, state, roster[0], query, sampled, duplicateClaims, now)
	if err != nil || result.Tally().A() != 1 || result.Tally().Duplicates() != 2 ||
		result.State().Margin() != 0 {
		t.Fatalf("duplicate authenticated source = tally %#v state %#v err %v",
			result.Tally(), result.State(), err)
	}

	normal := []AuthenticatedVote{
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceA, sampled[0]),
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceA, sampled[1]),
		mustVote(t, descriptor.ID(), 1, nonce, PreferenceB, sampled[2]),
	}
	result, err = ApplyRound(descriptor, state, roster[0], query, sampled, normal, now)
	if err != nil || result.Tally().A() != 2 || result.State().Margin() != 1 {
		t.Fatalf("normal authenticated votes = tally %#v state %#v err %v",
			result.Tally(), result.State(), err)
	}
}

func TestApplyRoundRejectsInvalidRoundEnvelope(t *testing.T) {
	now := time.Date(2026, 8, 3, 6, 0, 0, 0, time.UTC)
	profile := mustProfile(t, 3, 2, 1, 2)
	descriptor := mustDescriptor(t, profile, testPeers(t, 5), now.Add(time.Hour))
	roster := descriptor.ParticipantRoster()
	state := mustState(t, descriptor.ID(), PreferenceA)
	nonce := agency.Sum([]byte("nonce"))

	tests := []struct {
		name    string
		query   SampleQuery
		sampled []ParticipantID
		now     time.Time
	}{
		{"wrong query round", mustQuery(t, descriptor.ID(), 2, nonce), roster[1:4], now},
		{"sample includes self", mustQuery(t, descriptor.ID(), 1, nonce), roster[:3], now},
		{"short sample", mustQuery(t, descriptor.ID(), 1, nonce), roster[1:3], now},
		{"expired", mustQuery(t, descriptor.ID(), 1, nonce), roster[1:4], descriptor.ExpiresAt()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ApplyRound(descriptor, state, roster[0], test.query, test.sampled, nil, test.now); err == nil {
				t.Fatal("invalid round envelope was accepted")
			}
		})
	}
}

func TestObserveThresholdReachedAndInconclusive(t *testing.T) {
	now := time.Date(2026, 8, 3, 7, 0, 0, 0, time.UTC)
	thresholdDescriptor := mustDescriptor(t, mustProfile(t, 3, 2, 2, 4), testPeers(t, 5), now.Add(time.Hour))
	roster := thresholdDescriptor.ParticipantRoster()
	state := mustState(t, thresholdDescriptor.ID(), PreferenceB)
	state = applyPreferences(t, thresholdDescriptor, state, roster[0], roster[1:4], now,
		PreferenceA, PreferenceA).State()
	state = applyPreferences(t, thresholdDescriptor, state, roster[0], roster[1:4], now,
		PreferenceA, PreferenceA).State()
	observation, ready, err := Observe(thresholdDescriptor, state, now)
	if err != nil || !ready || observation.Result() != ObservationThresholdReached {
		t.Fatalf("threshold observation = %#v, ready %v, err %v", observation, ready, err)
	}
	if preference, ok := observation.ThresholdPreference(); !ok || preference != PreferenceA {
		t.Fatalf("threshold preference = %s, %v", preference, ok)
	}
	assertObservationCanonical(t, observation)

	roundDescriptor := mustDescriptor(t, mustProfile(t, 3, 2, 2, 2), testPeers(t, 5), now.Add(time.Hour))
	roundRoster := roundDescriptor.ParticipantRoster()
	roundState := mustState(t, roundDescriptor.ID(), PreferenceA)
	for range 2 {
		roundState = applyPreferences(t, roundDescriptor, roundState, roundRoster[0], roundRoster[1:4], now).State()
	}
	observation, ready, err = Observe(roundDescriptor, roundState, now)
	if err != nil || !ready || observation.Result() != ObservationInconclusive ||
		observation.Reason() != ReasonRoundLimit {
		t.Fatalf("round-limit observation = %#v, ready %v, err %v", observation, ready, err)
	}

	expiring := mustDescriptor(t, mustProfile(t, 3, 2, 1, 2), testPeers(t, 5), now.Add(time.Minute))
	observation, ready, err = Observe(expiring, mustState(t, expiring.ID(), PreferenceB), expiring.ExpiresAt())
	if err != nil || !ready || observation.Reason() != ReasonExpired {
		t.Fatalf("expiry observation = %#v, ready %v, err %v", observation, ready, err)
	}
}

func TestDeterministicThirtyTwoNodeSelection(t *testing.T) {
	now := time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC)
	profile := mustProfile(t, 5, 3, 4, 12)
	descriptor := mustDescriptor(t, profile, testPeers(t, 32), now.Add(time.Hour))
	roster := descriptor.ParticipantRoster()
	states := make([]SelectionState, len(roster))
	for index := range states {
		initial := PreferenceA
		if index == len(states)-1 {
			initial = PreferenceB
		}
		states[index] = mustState(t, descriptor.ID(), initial)
	}

	for round := uint32(1); round <= profile.Threshold(); round++ {
		snapshot := append([]SelectionState(nil), states...)
		for node := range states {
			sampled := deterministicSample(roster, node, int(round), int(profile.SampleSize()))
			nonce := agency.Sum([]byte(fmt.Sprintf("node-%d-round-%d", node, round)))
			query := mustQuery(t, descriptor.ID(), round, nonce)
			votes := make([]AuthenticatedVote, len(sampled))
			for index, peer := range sampled {
				peerIndex := peerIndex(roster, peer)
				votes[index] = mustVote(t, descriptor.ID(), round, nonce, snapshot[peerIndex].Preference(), peer)
			}
			result, err := ApplyRound(descriptor, states[node], roster[node], query, sampled, votes, now)
			if err != nil {
				t.Fatalf("node %d round %d: %v", node, round, err)
			}
			states[node] = result.State()
		}
	}
	for node, state := range states {
		observation, ready, err := Observe(descriptor, state, now)
		if err != nil || !ready {
			t.Fatalf("node %d did not finish: state %#v err %v", node, state, err)
		}
		preference, thresholdReached := observation.ThresholdPreference()
		if !thresholdReached || preference != PreferenceA {
			t.Fatalf("node %d observation = %#v", node, observation)
		}
	}
}

func assertObservationCanonical(t *testing.T, observation PreferenceObservation) {
	t.Helper()
	canonical, err := canonicalJSONRoundTrip(observation.CanonicalBytes())
	if err != nil || !bytes.Equal(canonical, observation.CanonicalBytes()) ||
		observation.Digest() != agency.Sum(observation.CanonicalBytes()) {
		t.Fatalf("observation is not canonical: %v", err)
	}
	var wire observationWire
	if err := json.Unmarshal(observation.CanonicalBytes(), &wire); err != nil || wire.Preference == nil || wire.Reason != nil {
		t.Fatalf("threshold observation wire = %#v, err %v", wire, err)
	}
}

func TestThresholdReachedIsLocalEvidenceNotConsensusOrFinality(t *testing.T) {
	now := time.Date(2026, 8, 3, 7, 30, 0, 0, time.UTC)
	descriptor := mustDescriptor(t, mustProfile(t, 3, 2, 1, 3), testPeers(t, 5), now.Add(time.Hour))
	roster := descriptor.ParticipantRoster()

	local := applyPreferences(t, descriptor, mustState(t, descriptor.ID(), PreferenceB),
		roster[0], roster[1:4], now, PreferenceA, PreferenceA).State()
	localObservation, ready, err := Observe(descriptor, local, now)
	if err != nil || !ready || localObservation.Result() != ObservationThresholdReached {
		t.Fatalf("local threshold observation = %#v, ready %v, err %v", localObservation, ready, err)
	}

	remote := mustState(t, descriptor.ID(), PreferenceB)
	remoteObservation, remoteReady, err := Observe(descriptor, remote, now)
	if err != nil || remoteReady || remoteObservation.Result() != "" ||
		len(remoteObservation.CanonicalBytes()) != 0 || remote.Preference() != PreferenceB {
		t.Fatalf("local threshold changed independent participant: observation=%#v ready=%v state=%#v err=%v",
			remoteObservation, remoteReady, remote, err)
	}

	var wire observationWire
	if err := json.Unmarshal(localObservation.CanonicalBytes(), &wire); err != nil ||
		wire.Result != "threshold_reached" {
		t.Fatalf("threshold wire result = %q, err %v", wire.Result, err)
	}
}

func applyPreferences(t *testing.T, descriptor SelectionDescriptor, state SelectionState,
	self ParticipantID, sampled []ParticipantID, now time.Time, preferences ...Preference,
) RoundResult {
	t.Helper()
	nonce := agency.Sum([]byte(fmt.Sprintf("round-%d", state.Round()+1)))
	query := mustQuery(t, descriptor.ID(), state.Round()+1, nonce)
	votes := make([]AuthenticatedVote, len(preferences))
	for index, preference := range preferences {
		votes[index] = mustVote(t, descriptor.ID(), state.Round()+1, nonce, preference, sampled[index])
	}
	result, err := ApplyRound(descriptor, state, self, query, sampled, votes, now)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func deterministicSample(roster []ParticipantID, self, round, count int) []ParticipantID {
	result := make([]ParticipantID, 0, count)
	for offset := 1; len(result) < count; offset++ {
		index := (self + round*7 + offset*5) % len(roster)
		if index == self || containsPeer(result, roster[index]) {
			continue
		}
		result = append(result, roster[index])
	}
	return result
}

func containsPeer(peers []ParticipantID, wanted ParticipantID) bool {
	for _, peer := range peers {
		if peer == wanted {
			return true
		}
	}
	return false
}

func peerIndex(roster []ParticipantID, wanted ParticipantID) int {
	for index, peer := range roster {
		if peer == wanted {
			return index
		}
	}
	return -1
}

func mustProfile(t testing.TB, sample, alpha, threshold, rounds uint32) Profile {
	t.Helper()
	profile, err := NewProfile(sample, alpha, threshold, rounds, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func mustDescriptor(t testing.TB, profile Profile, roster []ParticipantID, expires time.Time) SelectionDescriptor {
	t.Helper()
	descriptor, err := NewSelectionDescriptor(agency.Sum([]byte("question")), agency.Sum([]byte("candidate-a")),
		agency.Sum([]byte("candidate-b")), roster, profile, expires.Add(-time.Hour), expires)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func mustState(t testing.TB, selectionID SelectionID, preference Preference) SelectionState {
	t.Helper()
	state, err := NewSelectionState(selectionID, preference)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func mustQuery(t testing.TB, selectionID SelectionID, round uint32, nonce agency.Digest) SampleQuery {
	t.Helper()
	query, err := NewSampleQuery(selectionID, round, nonce)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func mustVote(t testing.TB, selectionID SelectionID, round uint32, nonce agency.Digest,
	preference Preference, source ParticipantID,
) AuthenticatedVote {
	t.Helper()
	wire, err := NewSampleVote(selectionID, round, nonce, preference, source)
	if err != nil {
		t.Fatal(err)
	}
	vote, err := AuthenticateSampleVote(source, wire)
	if err != nil {
		t.Fatal(err)
	}
	return vote
}

func testPeers(t testing.TB, count int) []ParticipantID {
	t.Helper()
	result := make([]ParticipantID, count)
	for index := range result {
		peer, err := NewParticipantID(fmt.Sprintf("peer-%03d", index))
		if err != nil {
			t.Fatal(err)
		}
		result[index] = peer
	}
	return result
}

func canonicalJSONRoundTrip(raw []byte) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func TestParticipantIDIsSmallCanonicalAndSemanticallyNeutral(t *testing.T) {
	valid, err := NewParticipantID("transport:participant/001")
	if err != nil || valid.String() != "transport:participant/001" {
		t.Fatalf("NewParticipantID() = %q, %v", valid.String(), err)
	}
	for _, value := range []string{"", "contains space", "contains\nnewline", "非-ascii"} {
		if _, err := NewParticipantID(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("NewParticipantID(%q) error = %v, want ErrInvalid", value, err)
		}
	}
	if _, err := NewParticipantID(strings.Repeat("p", MaxParticipantIDBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized ParticipantID error = %v, want ErrLimit", err)
	}
}

func TestErrorCategoriesRemainInspectable(t *testing.T) {
	_, err := NewProfile(0, 0, 0, 0, 0)
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("profile error = %v", err)
	}
	if reflect.DeepEqual(PreferenceA, PreferenceB) {
		t.Fatal("closed preferences unexpectedly alias")
	}
}
