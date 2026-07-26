package model

import (
	"errors"
	"testing"
	"time"
)

func TestHostRuntimeMappingIsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host    HostKind
		runtime RuntimeKind
		ok      bool
	}{
		{HostCodex, RuntimeCodexAppServer, true},
		{HostKind("claude-code"), "", false},
		{HostKind("multica"), "", false},
	}
	for _, test := range tests {
		got, ok := RuntimeForHost(test.host)
		if ok != test.ok || got != test.runtime {
			t.Fatalf("RuntimeForHost(%q) = %q, %t", test.host, got, ok)
		}
	}
}

func TestNodeInvariants(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	peer := mustPeer(t, "peer-a")
	epoch, _ := ParseOriginEpoch("epoch-a")
	spec := NodeSpec{peer, epoch, 1, "assets-v1", now, now}
	node, err := NewNode(spec)
	if err != nil || node.PeerID() != peer {
		t.Fatalf("NewNode() = %#v, %v", node, err)
	}
	spec.NextOriginSequence = 0
	if _, err := NewNode(spec); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero sequence error = %v", err)
	}
}

func TestProfileInvariants(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	budget := DefaultHandlingBudget().JSON()
	valid := ProfileSpec{TeamworkProfileID(), "principal-a", "/workspace/project", HostCodex,
		RuntimeCodexAppServer, Sum([]byte("credential")), "assets-v1", budget, true, now, now}
	if _, err := NewProfile(valid); err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*ProfileSpec)
		wantErr error
	}{
		{name: "relative workspace", mutate: func(spec *ProfileSpec) { spec.WorkspaceRoot = "project" }, wantErr: ErrInvalid},
		{name: "runtime fallback", mutate: func(spec *ProfileSpec) { spec.Runtime = RuntimeKind("claude-cli") }, wantErr: ErrInvariant},
		{name: "wrong profile", mutate: func(spec *ProfileSpec) { spec.ID, _ = ParseProfileID("other") }, wantErr: ErrInvariant},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if _, err := NewProfile(spec); !errors.Is(err, test.wantErr) {
				t.Fatalf("NewProfile() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}
