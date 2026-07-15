package teamwork

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPlanOfferCanonicalSingleReviewerExpansion(t *testing.T) {
	t.Parallel()

	acceptedAt := time.Date(2026, 7, 16, 1, 2, 3, 456, time.FixedZone("offset", 8*60*60))
	spec := OfferPlanSpec{
		ChannelID:       parseChannel(t, "channel-alpha"),
		RosterRevision:  9,
		HomePeerID:      parsePeer(t, "peer-home"),
		ReviewerPeerIDs: []model.PeerID{parsePeer(t, "peer-z"), parsePeer(t, "peer-a"), parsePeer(t, "peer-m")},
		AcceptedAt:      acceptedAt,
	}

	plan, err := PlanOffer(spec)
	if err != nil {
		t.Fatalf("PlanOffer() error = %v", err)
	}
	if got, want := plan.AcceptedAt(), acceptedAt.UTC(); !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("AcceptedAt() = %v, want canonical UTC %v", got, want)
	}
	if got := plan.DeadlineDuration(); got != DefaultOfferDeadline {
		t.Fatalf("DeadlineDuration() = %v, want %v", got, DefaultOfferDeadline)
	}
	if got, want := plan.DeadlineUnixNano(), acceptedAt.UTC().Add(DefaultOfferDeadline).UnixNano(); got != want {
		t.Fatalf("DeadlineUnixNano() = %d, want %d", got, want)
	}

	offers := plan.Offers()
	if len(offers) != 3 {
		t.Fatalf("len(Offers()) = %d, want 3", len(offers))
	}
	for index, want := range []string{"peer-a", "peer-m", "peer-z"} {
		if got := offers[index].Ordinal(); got != uint8(index) {
			t.Errorf("offer[%d].Ordinal() = %d", index, got)
		}
		participants := offers[index].Participants()
		if got := participants.ReviewerPeerID().String(); got != want {
			t.Errorf("offer[%d] reviewer = %q, want %q", index, got, want)
		}
		if participants.InitiatorPeerID() != spec.HomePeerID || participants.ChannelID() != spec.ChannelID || participants.RosterRevision() != 9 {
			t.Errorf("offer[%d] participant snapshot lost frozen scope", index)
		}
	}

	// Returned slices cannot rewrite the immutable plan.
	offers[0] = PlannedOffer{}
	if got := plan.Offers()[0].Participants().ReviewerPeerID().String(); got != "peer-a" {
		t.Fatalf("Offers() exposed mutable plan storage: got %q", got)
	}
}

func TestPlanOfferDeadlineBounds(t *testing.T) {
	t.Parallel()

	base := OfferPlanSpec{
		ChannelID:       parseChannel(t, "channel-alpha"),
		RosterRevision:  1,
		HomePeerID:      parsePeer(t, "peer-home"),
		ReviewerPeerIDs: []model.PeerID{parsePeer(t, "peer-reviewer")},
		AcceptedAt:      time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name     string
		deadline time.Duration
		want     time.Duration
		wantErr  bool
	}{
		{name: "default", deadline: 0, want: 24 * time.Hour},
		{name: "minimum inclusive", deadline: 5 * time.Minute, want: 5 * time.Minute},
		{name: "maximum inclusive", deadline: 168 * time.Hour, want: 168 * time.Hour},
		{name: "below minimum", deadline: 5*time.Minute - time.Nanosecond, wantErr: true},
		{name: "above maximum", deadline: 168*time.Hour + time.Nanosecond, wantErr: true},
		{name: "negative", deadline: -time.Second, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := base
			spec.Deadline = test.deadline
			plan, err := PlanOffer(spec)
			if test.wantErr {
				if !errors.Is(err, ErrDeadlineOutOfRange) {
					t.Fatalf("PlanOffer() error = %v, want ErrDeadlineOutOfRange", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("PlanOffer() error = %v", err)
			}
			if plan.DeadlineDuration() != test.want {
				t.Fatalf("DeadlineDuration() = %v, want %v", plan.DeadlineDuration(), test.want)
			}
		})
	}
}

func TestPlanOfferRejectsInvalidReviewerSets(t *testing.T) {
	t.Parallel()

	base := OfferPlanSpec{
		ChannelID:      parseChannel(t, "channel-alpha"),
		RosterRevision: 1,
		HomePeerID:     parsePeer(t, "peer-home"),
		AcceptedAt:     time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
	}
	seven := make([]model.PeerID, model.MaxChildWorks)
	for index := range seven {
		seven[index] = parsePeer(t, fmt.Sprintf("peer-%d", index))
	}
	eight := append(append([]model.PeerID(nil), seven...), parsePeer(t, "peer-7"))

	tests := []struct {
		name      string
		reviewers []model.PeerID
		wantOK    bool
	}{
		{name: "one", reviewers: seven[:1], wantOK: true},
		{name: "seven", reviewers: seven, wantOK: true},
		{name: "none"},
		{name: "eight", reviewers: eight},
		{name: "self", reviewers: []model.PeerID{base.HomePeerID}},
		{name: "duplicate", reviewers: []model.PeerID{seven[0], seven[0]}},
		{name: "zero", reviewers: []model.PeerID{{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := base
			spec.ReviewerPeerIDs = test.reviewers
			_, err := PlanOffer(spec)
			if test.wantOK && err != nil {
				t.Fatalf("PlanOffer() error = %v", err)
			}
			if !test.wantOK && !errors.Is(err, ErrInvalidOffer) {
				t.Fatalf("PlanOffer() error = %v, want ErrInvalidOffer", err)
			}
		})
	}
}

func parsePeer(t *testing.T, value string) model.PeerID {
	t.Helper()
	peer, err := model.ParsePeerID(value)
	if err != nil {
		t.Fatalf("ParsePeerID(%q): %v", value, err)
	}
	return peer
}

func parseChannel(t *testing.T, value string) model.ChannelID {
	t.Helper()
	channel, err := model.ParseChannelID(value)
	if err != nil {
		t.Fatalf("ParseChannelID(%q): %v", value, err)
	}
	return channel
}
