package teamwork

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPlanOfferCanonicalSingleReviewer(t *testing.T) {
	t.Parallel()

	acceptedAt := time.Date(2026, 7, 16, 1, 2, 3, 456, time.FixedZone("offset", 8*60*60))
	reviewer := canonicalOfferPeer(t, "peer-reviewer")
	spec := OfferPlanSpec{
		ChannelID:      parseChannel(t, "channel-alpha"),
		RosterRevision: 9,
		HomePeerID:     parsePeer(t, "peer-home"),
		ReviewerPeerID: reviewer,
		AcceptedAt:     acceptedAt,
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

	participants := plan.Participants()
	if got := participants.ReviewerPeerID(); got != reviewer {
		t.Errorf("reviewer = %q, want %q", got.String(), reviewer.String())
	}
	if participants.InitiatorPeerID() != spec.HomePeerID ||
		participants.ChannelID() != spec.ChannelID || participants.RosterRevision() != 9 {
		t.Error("participant snapshot lost frozen scope")
	}
}

func TestPlanOfferDeadlineBounds(t *testing.T) {
	t.Parallel()

	base := OfferPlanSpec{
		ChannelID:      parseChannel(t, "channel-alpha"),
		RosterRevision: 1,
		HomePeerID:     parsePeer(t, "peer-home"),
		ReviewerPeerID: canonicalOfferPeer(t, "peer-reviewer"),
		AcceptedAt:     time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
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

func TestPlanOfferRejectsInvalidReviewer(t *testing.T) {
	t.Parallel()

	base := OfferPlanSpec{
		ChannelID:      parseChannel(t, "channel-alpha"),
		RosterRevision: 1,
		HomePeerID:     parsePeer(t, "peer-home"),
		AcceptedAt:     time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name     string
		reviewer model.PeerID
		wantOK   bool
	}{
		{name: "valid", reviewer: canonicalOfferPeer(t, "peer-reviewer"), wantOK: true},
		{name: "zero"},
		{name: "self", reviewer: base.HomePeerID},
		{name: "non-libp2p", reviewer: parsePeer(t, "peer-invalid")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := base
			spec.ReviewerPeerID = test.reviewer
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

func canonicalOfferPeer(t *testing.T, label string) model.PeerID {
	t.Helper()
	seed := sha256.Sum256([]byte(label))
	standardPrivate := ed25519.NewKeyFromSeed(seed[:])
	privateKey, err := libp2pcrypto.UnmarshalEd25519PrivateKey(standardPrivate)
	if err != nil {
		t.Fatal(err)
	}
	id, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return parsePeer(t, id.String())
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
