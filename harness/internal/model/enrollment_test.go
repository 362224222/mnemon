package model

import (
	"errors"
	"testing"
	"time"
)

func TestOpenEnrollmentGrantIsBoundedAndSecretFree(t *testing.T) {
	t.Parallel()
	id, _ := ParseGrantID("grant-one")
	channelID, _ := ParseChannelID("channel-one")
	createdAt := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	grant, err := NewOpenEnrollmentGrant(id, channelID, Sum([]byte("protocol-verifier")),
		createdAt.Add(time.Hour), 7, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if grant.ID() != id || grant.ChannelID() != channelID || grant.MaxUses() != 7 ||
		!grant.ExpiresAt().Equal(createdAt.Add(time.Hour)) {
		t.Fatalf("grant = %#v", grant)
	}
	if _, err := NewOpenEnrollmentGrant(id, channelID, grant.Verifier(), createdAt, 1, createdAt); !errors.Is(err, ErrInvariant) {
		t.Fatalf("nonfuture expiry error = %v", err)
	}
	if _, err := NewOpenEnrollmentGrant(id, channelID, grant.Verifier(),
		createdAt.Add(time.Hour), 8, createdAt); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized use count error = %v", err)
	}
}
