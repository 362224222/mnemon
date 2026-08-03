package r8_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

// These parameters are frozen before observing the matrix. They are the
// smallest R8 profile already exercised by the selector package, not tuned per
// scenario. Changing one is a new experiment, not a test repair.
const (
	sampleSize       = 5
	sampleAlpha      = 3
	marginThreshold  = 4
	maxRounds        = 12
	partitionRounds  = 3
	majorityPercent  = 55
	defaultFaultRate = 10
)

var experimentSeeds = []int64{190608936, 240102811, 20260803}

// holdoutSeeds are disjoint from experimentSeeds and are never regenerated in
// response to an observed result. They characterize, rather than prove, the
// frozen profile's behavior across a small fixed corpus.
var holdoutSeeds = []int64{
	104729, 130363, 155921, 196613,
	262147, 524309, 1048583, 2147483659,
}

type faultMode string

const (
	faultNone       faultMode = "normal"
	faultRefusal    faultMode = "refusal"
	faultEquivocate faultMode = "equivocation"
	faultStrategic  faultMode = "strategic-single-vote"
	faultPartition  faultMode = "partition-recovery"
)

type experiment struct {
	nodes             int
	percentA          int
	fault             faultMode
	faultPercent      int
	partitionDuration uint32
}

func (e experiment) name(seed int64) string {
	return fmt.Sprintf("N%d/%d-%d/%s/seed-%d", e.nodes, e.percentA, 100-e.percentA, e.faultLabel(), seed)

}

func (e experiment) faultLabel() string {
	switch e.fault {
	case faultRefusal, faultEquivocate, faultStrategic:
		return fmt.Sprintf("%s-%dpct", e.fault, e.effectiveFaultPercent())
	case faultPartition:
		if duration := e.effectivePartitionDuration(); duration != partitionRounds {
			return fmt.Sprintf("%s-%drounds", e.fault, duration)
		}
	}
	return string(e.fault)
}

func (e experiment) effectiveFaultPercent() int {
	if e.faultPercent > 0 {
		return e.faultPercent
	}
	return defaultFaultRate
}

func (e experiment) effectivePartitionDuration() uint32 {
	if e.partitionDuration > 0 {
		return e.partitionDuration
	}
	return partitionRounds
}

type experimentPlan struct {
	initial []selector.Preference
	faulty  map[int]bool
}

type selectionMetrics struct {
	thresholdA        int
	thresholdB        int
	inconclusive      int
	oppositeThreshold bool
	epochs            uint32
	nodeRounds        uint64
	messages          uint64
}

type slushMetrics struct {
	finalA        int
	finalB        int
	oppositeFinal bool
	rounds        uint32
	messages      uint64
}

type outcomeDistribution struct {
	trials            int
	unanimousA        int
	unanimousB        int
	oppositeThreshold int
	withInconclusive  int
	totalThresholdA   int
	totalThresholdB   int
	totalInconclusive int
	totalEpochs       uint64
	totalNodeRounds   uint64
	totalMessages     uint64
}

func (d *outcomeDistribution) add(metrics selectionMetrics) {
	d.trials++
	if metrics.thresholdA > 0 && metrics.thresholdB == 0 && metrics.inconclusive == 0 {
		d.unanimousA++
	}
	if metrics.thresholdB > 0 && metrics.thresholdA == 0 && metrics.inconclusive == 0 {
		d.unanimousB++
	}
	if metrics.oppositeThreshold {
		d.oppositeThreshold++
	}
	if metrics.inconclusive > 0 {
		d.withInconclusive++
	}
	d.totalThresholdA += metrics.thresholdA
	d.totalThresholdB += metrics.thresholdB
	d.totalInconclusive += metrics.inconclusive
	d.totalEpochs += uint64(metrics.epochs)
	d.totalNodeRounds += metrics.nodeRounds
	d.totalMessages += metrics.messages
}

func TestR8FalsifiableSimulationMatrix(t *testing.T) {
	for _, experiment := range experimentMatrix() {
		for _, seed := range experimentSeeds {
			t.Run(experiment.name(seed), func(t *testing.T) {
				sampled := runSelection(t, experiment, seed, false)
				census := runSelection(t, experiment, seed, true)
				slush := runPureSlush(t, experiment, seed)

				assertSelectionAccounting(t, experiment, sampled, sampleSize)
				assertSelectionAccounting(t, experiment, census, experiment.nodes-1)
				assertSlushAccounting(t, experiment, slush)
				assertFrozenAllowances(t, experiment, sampled, census, slush)

				t.Logf("sampled threshold=%dA/%dB opposite=%t inconclusive=%d epochs=%d node_rounds=%d messages=%d",
					sampled.thresholdA, sampled.thresholdB, sampled.oppositeThreshold, sampled.inconclusive,
					sampled.epochs, sampled.nodeRounds, sampled.messages)
				t.Logf("census threshold=%dA/%dB opposite=%t inconclusive=%d epochs=%d node_rounds=%d messages=%d",
					census.thresholdA, census.thresholdB, census.oppositeThreshold, census.inconclusive,
					census.epochs, census.nodeRounds, census.messages)
				t.Logf("slush final=%dA/%dB opposite=%t rounds=%d messages=%d",
					slush.finalA, slush.finalB, slush.oppositeFinal, slush.rounds, slush.messages)
			})
		}
	}
}

func TestR8SimulationIsDeterministic(t *testing.T) {
	experiment := experiment{nodes: 64, percentA: 55, fault: faultEquivocate}
	seed := experimentSeeds[1]
	if first, second := runSelection(t, experiment, seed, false),
		runSelection(t, experiment, seed, false); !reflect.DeepEqual(first, second) {
		t.Fatalf("sampled selector changed for fixed seed: first=%+v second=%+v", first, second)
	}
	if first, second := runPureSlush(t, experiment, seed),
		runPureSlush(t, experiment, seed); !reflect.DeepEqual(first, second) {
		t.Fatalf("pure Slush changed for fixed seed: first=%+v second=%+v", first, second)
	}
}

func TestR8FrozenProfileExposesOppositeThresholdCounterexample(t *testing.T) {
	experiment := experiment{nodes: 32, percentA: 50, fault: faultNone}
	got := runSelection(t, experiment, experimentSeeds[0], false)
	want := selectionMetrics{
		thresholdA: 1, thresholdB: 31, oppositeThreshold: true,
		epochs: 10, nodeRounds: 240, messages: 2400,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frozen no-fault counterexample changed: got=%+v want=%+v", got, want)
	}
}

func TestR8PartitionAtTauIsCharacterized(t *testing.T) {
	experiment := experiment{
		nodes: 64, percentA: 50, fault: faultPartition,
		partitionDuration: marginThreshold,
	}
	if experiment.effectivePartitionDuration() < marginThreshold {
		t.Fatalf("partition duration %d is below tau %d",
			experiment.effectivePartitionDuration(), marginThreshold)
	}
	for _, seed := range experimentSeeds {
		t.Run(experiment.name(seed), func(t *testing.T) {
			got := runSelection(t, experiment, seed, false)
			assertSelectionAccounting(t, experiment, got, sampleSize)
			t.Logf("tau-partition threshold=%dA/%dB opposite=%t inconclusive=%d epochs=%d node_rounds=%d messages=%d",
				got.thresholdA, got.thresholdB, got.oppositeThreshold, got.inconclusive,
				got.epochs, got.nodeRounds, got.messages)
		})
	}
}

func TestR8StrategicByzantineUsesOneRequesterSpecificVote(t *testing.T) {
	experiment := experiment{
		nodes: 32, percentA: 50, fault: faultStrategic, faultPercent: 20,
	}
	seed := experimentSeeds[0]
	plan := newExperimentPlan(experiment, seed)
	roster := newRoster(t, experiment.nodes)
	descriptor := newDescriptor(t, roster, newProfile(t, experiment.nodes, false))
	faulty := firstFaulty(t, plan)

	requesterA, requesterB := 0, 1
	snapshot := append([]selector.Preference(nil), plan.initial...)
	snapshot[requesterA] = selector.PreferenceA
	snapshot[requesterB] = selector.PreferenceB
	queryA := newQuery(t, descriptor.ID(), 1, seed, requesterA)
	queryB := newQuery(t, descriptor.ID(), 1, seed, requesterB)
	votesA, messagesA := selectionVotes(t, descriptor.ID(), queryA,
		[]selector.ParticipantID{roster[faulty]}, roster, snapshot, plan, experiment, requesterA, 1)
	votesB, messagesB := selectionVotes(t, descriptor.ID(), queryB,
		[]selector.ParticipantID{roster[faulty]}, roster, snapshot, plan, experiment, requesterB, 1)

	if len(votesA) != 1 || len(votesB) != 1 || messagesA != 2 || messagesB != 2 {
		t.Fatalf("strategic peer must emit one response per requester: A=%d/%d B=%d/%d",
			len(votesA), messagesA, len(votesB), messagesB)
	}
	if votesA[0].Preference() != selector.PreferenceA ||
		votesB[0].Preference() != selector.PreferenceB {
		t.Fatalf("strategic peer did not tailor its single vote by requester: A=%s B=%s",
			votesA[0].Preference(), votesB[0].Preference())
	}
}

func TestR8N128TwentyPercentFaultCharacterization(t *testing.T) {
	experiments := []experiment{
		{nodes: 128, percentA: 55, fault: faultRefusal, faultPercent: 20},
		{nodes: 128, percentA: 55, fault: faultEquivocate, faultPercent: 20},
		{nodes: 128, percentA: 55, fault: faultStrategic, faultPercent: 20},
		{
			nodes: 128, percentA: 50, fault: faultPartition,
			partitionDuration: marginThreshold,
		},
	}
	for _, experiment := range experiments {
		for _, seed := range experimentSeeds {
			t.Run(experiment.name(seed), func(t *testing.T) {
				sampled := runSelection(t, experiment, seed, false)
				assertSelectionAccounting(t, experiment, sampled, sampleSize)
				t.Logf("adversarial threshold=%dA/%dB opposite=%t inconclusive=%d epochs=%d node_rounds=%d messages=%d",
					sampled.thresholdA, sampled.thresholdB, sampled.oppositeThreshold, sampled.inconclusive,
					sampled.epochs, sampled.nodeRounds, sampled.messages)
			})
		}
	}
}

func TestR8FrozenHoldoutDistributions(t *testing.T) {
	experiments := []experiment{
		{nodes: 128, percentA: 55, fault: faultNone},
		{nodes: 128, percentA: 55, fault: faultRefusal, faultPercent: 20},
		{nodes: 128, percentA: 55, fault: faultEquivocate, faultPercent: 20},
		{nodes: 128, percentA: 55, fault: faultStrategic, faultPercent: 20},
		{
			nodes: 128, percentA: 50, fault: faultPartition,
			partitionDuration: marginThreshold,
		},
	}
	want := map[string]outcomeDistribution{
		"normal": {
			trials: 8, unanimousA: 6, oppositeThreshold: 2,
			totalThresholdA: 1016, totalThresholdB: 8,
			totalEpochs: 82, totalNodeRounds: 6014, totalMessages: 60140,
		},
		"refusal-20pct": {
			trials: 8, unanimousA: 5, oppositeThreshold: 1, withInconclusive: 3,
			totalThresholdA: 1002, totalThresholdB: 3, totalInconclusive: 19,
			totalEpochs: 82, totalNodeRounds: 6732, totalMessages: 60707,
		},
		"equivocation-20pct": {
			trials: 8, unanimousA: 5, oppositeThreshold: 1, withInconclusive: 3,
			totalThresholdA: 1002, totalThresholdB: 3, totalInconclusive: 19,
			totalEpochs: 82, totalNodeRounds: 6732, totalMessages: 73933,
		},
		"strategic-single-vote-20pct": {
			trials: 8, unanimousA: 3, oppositeThreshold: 5, withInconclusive: 1,
			totalThresholdA: 975, totalThresholdB: 40, totalInconclusive: 9,
			totalEpochs: 82, totalNodeRounds: 5870, totalMessages: 58700,
		},
		"partition-recovery-4rounds": {
			trials: 8, oppositeThreshold: 3, withInconclusive: 8,
			totalThresholdA: 588, totalThresholdB: 359, totalInconclusive: 77,
			totalEpochs: 96, totalNodeRounds: 9702, totalMessages: 86736,
		},
	}

	for _, experiment := range experiments {
		name := experiment.faultLabel()
		t.Run(name, func(t *testing.T) {
			got := outcomeDistribution{}
			for _, seed := range holdoutSeeds {
				metrics := runSelection(t, experiment, seed, false)
				assertSelectionAccounting(t, experiment, metrics, sampleSize)
				got.add(metrics)
			}
			t.Logf("holdout %s: %+v", name, got)
			if expected, ok := want[name]; !ok {
				t.Fatalf("holdout distribution for %s is not frozen: got=%+v", name, got)
			} else if !reflect.DeepEqual(got, expected) {
				t.Fatalf("holdout distribution changed for %s: got=%+v want=%+v", name, got, expected)
			}
		})
	}
}

func experimentMatrix() []experiment {
	result := make([]experiment, 0, 16)
	for _, nodes := range []int{32, 64} {
		for _, percentA := range []int{50, majorityPercent} {
			for _, fault := range []faultMode{faultNone, faultRefusal, faultEquivocate, faultPartition} {
				result = append(result, experiment{nodes: nodes, percentA: percentA, fault: fault})
			}
		}
	}
	return result
}

func runSelection(t testing.TB, experiment experiment, seed int64, census bool) selectionMetrics {
	t.Helper()
	plan := newExperimentPlan(experiment, seed)
	roster := newRoster(t, experiment.nodes)
	profile := newProfile(t, experiment.nodes, census)
	descriptor := newDescriptor(t, roster, profile)
	states := newStates(t, descriptor.ID(), plan.initial)
	terminal := make([]bool, experiment.nodes)
	now := descriptor.CreatedAt().Add(time.Minute)
	metrics := selectionMetrics{}

	for epoch := uint32(1); epoch <= maxRounds && !allTerminal(terminal); epoch++ {
		snapshot := selectionPreferences(states)
		for node := range states {
			if terminal[node] {
				continue
			}
			sampled := selectionSample(roster, node, epoch, seed, census)
			query := newQuery(t, descriptor.ID(), states[node].Round()+1, seed, node)
			votes, messages := selectionVotes(t, descriptor.ID(), query, sampled, roster,
				snapshot, plan, experiment, node, epoch)
			result, err := selector.ApplyRound(descriptor, states[node], roster[node], query, sampled, votes, now)
			if err != nil {
				t.Fatalf("node %d epoch %d: apply round: %v", node, epoch, err)
			}
			states[node] = result.State()
			metrics.messages += messages
			_, terminal[node], err = selector.Observe(descriptor, states[node], now)
			if err != nil {
				t.Fatalf("node %d epoch %d: observe: %v", node, epoch, err)
			}
		}
		metrics.epochs = epoch
	}
	classifySelection(t, descriptor, states, now, &metrics)
	return metrics
}

func newExperimentPlan(experiment experiment, seed int64) experimentPlan {
	random := rand.New(rand.NewSource(seed))
	order := random.Perm(experiment.nodes)
	countA := experiment.nodes / 2
	if experiment.percentA == majorityPercent {
		countA = (majorityPercent*experiment.nodes + 99) / 100
	}
	initial := make([]selector.Preference, experiment.nodes)
	for index := range initial {
		initial[index] = selector.PreferenceB
	}
	for _, index := range order[:countA] {
		initial[index] = selector.PreferenceA
	}
	faulty := make(map[int]bool)
	if experiment.fault == faultRefusal || experiment.fault == faultEquivocate ||
		experiment.fault == faultStrategic {
		faultOrder := rand.New(rand.NewSource(seed ^ 0x5bd1e995)).Perm(experiment.nodes)
		faultyCount := experiment.nodes * experiment.effectiveFaultPercent() / 100
		if faultyCount == 0 {
			faultyCount = 1
		}
		for _, index := range faultOrder[:faultyCount] {
			faulty[index] = true
		}
	}
	return experimentPlan{initial: initial, faulty: faulty}
}

func newRoster(t testing.TB, nodes int) []selector.ParticipantID {
	t.Helper()
	roster := make([]selector.ParticipantID, nodes)
	for node := range roster {
		participant, err := selector.NewParticipantID(fmt.Sprintf("peer-%03d", node))
		if err != nil {
			t.Fatal(err)
		}
		roster[node] = participant
	}
	return roster
}

func firstFaulty(t testing.TB, plan experimentPlan) int {
	t.Helper()
	for node := range plan.initial {
		if plan.faulty[node] {
			return node
		}
	}
	t.Fatal("experiment plan has no faulty peer")
	return -1
}

func newProfile(t testing.TB, nodes int, census bool) selector.Profile {
	t.Helper()
	k, alpha := uint32(sampleSize), uint32(sampleAlpha)
	if census {
		k = uint32(nodes - 1)
		alpha = k/2 + 1
	}
	profile, err := selector.NewProfile(k, alpha, marginThreshold, maxRounds, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func newDescriptor(t testing.TB, roster []selector.ParticipantID,
	profile selector.Profile,
) selector.SelectionDescriptor {
	t.Helper()
	createdAt := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	descriptor, err := selector.NewSelectionDescriptor(
		agency.Sum([]byte("r8-falsifiable-question")),
		agency.Sum([]byte("r8-candidate-a")),
		agency.Sum([]byte("r8-candidate-b")),
		roster, profile, createdAt, createdAt.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func newStates(t testing.TB, selectionID selector.SelectionID,
	initial []selector.Preference,
) []selector.SelectionState {
	t.Helper()
	states := make([]selector.SelectionState, len(initial))
	for node, preference := range initial {
		state, err := selector.NewSelectionState(selectionID, preference)
		if err != nil {
			t.Fatal(err)
		}
		states[node] = state
	}
	return states
}

func selectionSample(roster []selector.ParticipantID, self int, round uint32,
	seed int64, census bool,
) []selector.ParticipantID {
	candidates := make([]selector.ParticipantID, 0, len(roster)-1)
	for node, participant := range roster {
		if node != self {
			candidates = append(candidates, participant)
		}
	}
	if census {
		return candidates
	}
	random := rand.New(rand.NewSource(sampleSeed(seed, self, round)))
	random.Shuffle(len(candidates), func(left, right int) {
		candidates[left], candidates[right] = candidates[right], candidates[left]
	})
	return candidates[:sampleSize]
}

func sampleSeed(seed int64, node int, round uint32) int64 {
	return seed ^ int64(uint64(node+1)*0x9e3779b1) ^ int64(uint64(round)*0x85ebca77)
}

func newQuery(t testing.TB, selectionID selector.SelectionID, round uint32,
	seed int64, node int,
) selector.SampleQuery {
	t.Helper()
	nonce := agency.Sum([]byte(fmt.Sprintf("seed=%d/node=%d/round=%d", seed, node, round)))
	query, err := selector.NewSampleQuery(selectionID, round, nonce)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func selectionVotes(t testing.TB, selectionID selector.SelectionID, query selector.SampleQuery,
	sampled, roster []selector.ParticipantID, snapshot []selector.Preference,
	plan experimentPlan, experiment experiment, requester int, epoch uint32,
) ([]selector.AuthenticatedVote, uint64) {
	t.Helper()
	index := participantIndexes(roster)
	votes := make([]selector.AuthenticatedVote, 0, len(sampled))
	messages := uint64(len(sampled))
	for _, participant := range sampled {
		peer := index[participant]
		if !canRespond(experiment, plan, requester, peer, epoch) {
			continue
		}
		if experiment.fault == faultEquivocate && plan.faulty[peer] {
			votes = append(votes,
				newVote(t, selectionID, query, selector.PreferenceA, participant),
				newVote(t, selectionID, query, selector.PreferenceB, participant))
			messages += 2
			continue
		}
		preference := snapshot[peer]
		if experiment.fault == faultStrategic && plan.faulty[peer] {
			// One authenticated response is emitted, but it reinforces the
			// requester's current color. Different requesters can therefore see
			// different answers without a within-query double vote to discard.
			preference = snapshot[requester]
		}
		votes = append(votes, newVote(t, selectionID, query, preference, participant))
		messages++
	}
	return votes, messages
}

func newVote(t testing.TB, selectionID selector.SelectionID, query selector.SampleQuery,
	preference selector.Preference, source selector.ParticipantID,
) selector.AuthenticatedVote {
	t.Helper()
	wire, err := selector.NewSampleVote(selectionID, query.Round(), query.Nonce(), preference, source)
	if err != nil {
		t.Fatal(err)
	}
	vote, err := selector.AuthenticateSampleVote(source, wire)
	if err != nil {
		t.Fatal(err)
	}
	return vote
}

func canRespond(experiment experiment, plan experimentPlan, requester, peer int, epoch uint32) bool {
	if experiment.fault == faultRefusal && plan.faulty[peer] {
		return false
	}
	if experiment.fault == faultPartition && epoch <= experiment.effectivePartitionDuration() {
		return (requester < experiment.nodes/2) == (peer < experiment.nodes/2)
	}
	return true
}

func classifySelection(t testing.TB, descriptor selector.SelectionDescriptor,
	states []selector.SelectionState, now time.Time, metrics *selectionMetrics,
) {
	t.Helper()
	for node, state := range states {
		metrics.nodeRounds += uint64(state.Round())
		observation, ready, err := selector.Observe(descriptor, state, now)
		if err != nil || !ready {
			t.Fatalf("node %d has no terminal observation: ready=%t err=%v", node, ready, err)
		}
		preference, threshold := observation.ThresholdPreference()
		switch {
		case !threshold:
			metrics.inconclusive++
		case preference == selector.PreferenceA:
			metrics.thresholdA++
		case preference == selector.PreferenceB:
			metrics.thresholdB++
		}
	}
	metrics.oppositeThreshold = metrics.thresholdA > 0 && metrics.thresholdB > 0
}

func selectionPreferences(states []selector.SelectionState) []selector.Preference {
	preferences := make([]selector.Preference, len(states))
	for node, state := range states {
		preferences[node] = state.Preference()
	}
	return preferences
}

func participantIndexes(roster []selector.ParticipantID) map[selector.ParticipantID]int {
	result := make(map[selector.ParticipantID]int, len(roster))
	for index, participant := range roster {
		result[participant] = index
	}
	return result
}

func allTerminal(values []bool) bool {
	for _, value := range values {
		if !value {
			return false
		}
	}
	return true
}

func runPureSlush(t testing.TB, experiment experiment, seed int64) slushMetrics {
	t.Helper()
	plan := newExperimentPlan(experiment, seed)
	roster := newRoster(t, experiment.nodes)
	preferences := append([]selector.Preference(nil), plan.initial...)
	metrics := slushMetrics{rounds: maxRounds}
	for round := uint32(1); round <= maxRounds; round++ {
		snapshot := append([]selector.Preference(nil), preferences...)
		for node := range preferences {
			sampled := selectionSample(roster, node, round, seed, false)
			a, b, messages := slushVotes(sampled, roster, snapshot, plan, experiment, node, round)
			metrics.messages += messages
			if a >= sampleAlpha {
				preferences[node] = selector.PreferenceA
			} else if b >= sampleAlpha {
				preferences[node] = selector.PreferenceB
			}
		}
	}
	for _, preference := range preferences {
		if preference == selector.PreferenceA {
			metrics.finalA++
		} else {
			metrics.finalB++
		}
	}
	metrics.oppositeFinal = metrics.finalA > 0 && metrics.finalB > 0
	return metrics
}

func slushVotes(sampled, roster []selector.ParticipantID, snapshot []selector.Preference,
	plan experimentPlan, experiment experiment, requester int, round uint32,
) (int, int, uint64) {
	index := participantIndexes(roster)
	a, b := 0, 0
	messages := uint64(len(sampled))
	for _, participant := range sampled {
		peer := index[participant]
		if !canRespond(experiment, plan, requester, peer, round) {
			continue
		}
		if experiment.fault == faultEquivocate && plan.faulty[peer] {
			messages += 2
			continue
		}
		messages++
		preference := snapshot[peer]
		if experiment.fault == faultStrategic && plan.faulty[peer] {
			preference = snapshot[requester]
		}
		if preference == selector.PreferenceA {
			a++
		} else {
			b++
		}
	}
	return a, b, messages
}

func assertSelectionAccounting(t testing.TB, experiment experiment, metrics selectionMetrics,
	sample int,
) {
	t.Helper()
	if got := metrics.thresholdA + metrics.thresholdB + metrics.inconclusive; got != experiment.nodes {
		t.Fatalf("selection outcomes = %d, want %d", got, experiment.nodes)
	}
	if metrics.epochs == 0 || metrics.epochs > maxRounds || metrics.nodeRounds == 0 ||
		metrics.nodeRounds > uint64(experiment.nodes*maxRounds) {
		t.Fatalf("round accounting is out of bounds: %+v", metrics)
	}
	maximumMessages := uint64(sample * 3 * int(metrics.nodeRounds))
	if metrics.messages == 0 || metrics.messages > maximumMessages {
		t.Fatalf("message accounting is out of bounds: got %d, max %d", metrics.messages, maximumMessages)
	}
}

func assertSlushAccounting(t testing.TB, experiment experiment, metrics slushMetrics) {
	t.Helper()
	if metrics.finalA+metrics.finalB != experiment.nodes || metrics.rounds != maxRounds {
		t.Fatalf("Slush outcome accounting is invalid: %+v", metrics)
	}
	maximumMessages := uint64(experiment.nodes * maxRounds * sampleSize * 3)
	if metrics.messages == 0 || metrics.messages > maximumMessages {
		t.Fatalf("Slush message accounting is out of bounds: got %d, max %d", metrics.messages, maximumMessages)
	}
}

func assertFrozenAllowances(t testing.TB, experiment experiment, sampled, census selectionMetrics,
	slush slushMetrics,
) {
	t.Helper()
	// These are characterization allowances, not a claim of agreement or BFT.
	// In particular, the frozen profile demonstrably permits a small number of
	// opposite local threshold observations; the dedicated counterexample above
	// ensures that limitation remains visible rather than tuned away.
	if sampled.inconclusive > 2 || minimum(sampled.thresholdA, sampled.thresholdB) > 2 {
		t.Fatalf("sampled selector exceeded frozen divergence allowance: %+v", sampled)
	}
	if maximum(sampled.thresholdA, sampled.thresholdB) < experiment.nodes-4 {
		t.Fatalf("sampled selector lacked a dominant result within the frozen budget: %+v", sampled)
	}
	if experiment.percentA == majorityPercent &&
		(sampled.thresholdA < experiment.nodes-2 || sampled.thresholdB > 2 || sampled.inconclusive != 0) {
		t.Fatalf("55/45 sampled result exceeded frozen allowance: %+v", sampled)
	}
	if census.oppositeThreshold {
		t.Fatalf("all-to-all census produced opposite threshold observations: %+v", census)
	}
	if experiment.percentA == 50 && census.inconclusive != experiment.nodes {
		t.Fatalf("50/50 census escaped its frozen symmetric outcome: %+v", census)
	}
	if experiment.percentA == majorityPercent && !allAOrInconclusive(experiment.nodes, census) {
		t.Fatalf("55/45 census exceeded frozen outcome set: %+v", census)
	}
	if slush.oppositeFinal ||
		(experiment.percentA == majorityPercent && slush.finalA != experiment.nodes) {
		t.Fatalf("fixed-round Slush exceeded frozen final-color allowance: %+v", slush)
	}
	if sampled.messages >= census.messages {
		t.Fatalf("sampled selector messages %d are not below census messages %d", sampled.messages, census.messages)
	}
}

func allAOrInconclusive(nodes int, metrics selectionMetrics) bool {
	allA := metrics.thresholdA == nodes && metrics.thresholdB == 0 && metrics.inconclusive == 0
	allInconclusive := metrics.thresholdA == 0 && metrics.thresholdB == 0 && metrics.inconclusive == nodes
	return allA || allInconclusive
}

func minimum(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maximum(left, right int) int {
	if left > right {
		return left
	}
	return right
}
