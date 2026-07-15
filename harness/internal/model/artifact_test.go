package model

import (
	"errors"
	"testing"
	"time"
)

func TestArtifactRoleIsClosed(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		role ArtifactRole
		want bool
	}{
		{ArtifactProduced, true}, {ArtifactReferenced, true}, {ArtifactRole("memory"), false}, {"", false},
	} {
		if got := test.role.Valid(); got != test.want {
			t.Fatalf("ArtifactRole(%q).Valid() = %t, want %t", test.role, got, test.want)
		}
	}
}

func TestNormalizeArtifactRefs(t *testing.T) {
	t.Parallel()

	a, _ := NewArtifactRef(Sum([]byte("a")), ArtifactProduced)
	b, _ := NewArtifactRef(Sum([]byte("b")), ArtifactReferenced)
	got, err := normalizeArtifactRefs([]ArtifactRef{b, a}, MaxArtifactRefs)
	if err != nil {
		t.Fatalf("normalizeArtifactRefs() error = %v", err)
	}
	if got[0].RootDigest().String() > got[1].RootDigest().String() {
		t.Fatalf("artifact refs were not canonicalized: %#v", got)
	}
	conflict, _ := NewArtifactRef(a.RootDigest(), ArtifactReferenced)
	if _, err := normalizeArtifactRefs([]ArtifactRef{a, conflict}, MaxArtifactRefs); !errors.Is(err, ErrInvalid) {
		t.Fatalf("conflicting role error = %v", err)
	}
}

func TestArtifactProvenanceInvariants(t *testing.T) {
	t.Parallel()

	peerA, _ := ParsePeerID("peer-a")
	peerB, _ := ParsePeerID("peer-b")
	epoch, _ := ParseOriginEpoch("epoch-a")
	eventID, _ := ParseEventID("event-a")
	event, _ := NewEventKey(peerA, epoch, eventID)
	run, _ := ParseRunID("run-a")
	operation, _ := ParseOperationID("operation-a")
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		spec    ArtifactProvenanceSpec
		wantErr error
	}{
		{name: "local", spec: ArtifactProvenanceSpec{Sum([]byte("root")), event, peerA, &run, &operation, ProvenanceLocalCapture, now}},
		{name: "replica", spec: ArtifactProvenanceSpec{Sum([]byte("root")), event, peerA, nil, nil, ProvenanceReplica, now}},
		{name: "wrong origin", spec: ArtifactProvenanceSpec{Sum([]byte("root")), event, peerB, nil, nil, ProvenanceReplica, now}, wantErr: ErrInvariant},
		{name: "local missing run", spec: ArtifactProvenanceSpec{Sum([]byte("root")), event, peerA, nil, &operation, ProvenanceLocalCapture, now}, wantErr: ErrInvariant},
		{name: "replica claims run", spec: ArtifactProvenanceSpec{Sum([]byte("root")), event, peerA, &run, &operation, ProvenanceReplica, now}, wantErr: ErrInvariant},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewArtifactProvenance(test.spec)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewArtifactProvenance() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}
