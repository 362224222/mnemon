package selector

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func FuzzApplyRoundFiltersUntrustedVotes(f *testing.F) {
	f.Add([]byte{0, 4, 8, 12, 16})
	f.Add([]byte{1, 1, 2, 2, 3, 3})
	f.Fuzz(func(t *testing.T, input []byte) {
		now := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
		descriptor := mustDescriptor(t, mustProfile(t, 5, 3, 2, 4), testPeers(t, 7), now.Add(time.Hour))
		roster := descriptor.ParticipantRoster()
		state := mustState(t, descriptor.ID(), PreferenceA)
		nonce := model.Sum([]byte("fuzz-round"))
		query := mustQuery(t, descriptor.ID(), 1, nonce)
		otherID, err := ParseSelectionID(model.Sum([]byte("other")).String())
		if err != nil {
			t.Fatal(err)
		}
		if len(input) > 10 {
			input = input[:10]
		}
		votes := make([]SampleVote, len(input))
		for index, value := range input {
			votes[index] = SampleVote{
				selectionID: descriptor.ID(), round: 1, nonce: nonce,
				preference: Preference(value%2 + 1), source: roster[int(value)%len(roster)],
			}
			switch value % 7 {
			case 0:
				votes[index].selectionID = otherID
			case 1:
				votes[index].round = 2
			case 2:
				votes[index].nonce = model.Sum([]byte("wrong"))
			case 3:
				votes[index].preference = 0
			}
		}
		result, err := ApplyRound(descriptor, state, roster[0], query, roster[1:6], votes, now)
		if err != nil {
			t.Fatal(err)
		}
		next := result.State()
		if next.Round() != 1 || abs64(next.Margin()) > int64(next.Round()) || !validPreference(next.Preference()) {
			t.Fatalf("invalid next state: %#v", next)
		}
		tally := result.Tally()
		if tally.A()+tally.B()+tally.Equivocations() > uint32(len(roster[1:6])) {
			t.Fatalf("accepted more peer outcomes than sampled: %#v", tally)
		}
	})
}
