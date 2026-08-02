package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
)

func TestAdapterPutsReadsAndReverifiesExactBytes(t *testing.T) {
	adapter := newTestAdapter(t)
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	content := []byte("bounded R7 Artifact")
	verified, err := adapter.Put(context.Background(), content, now)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Digest() != agency.Sum(content) || verified.ByteSize() != int64(len(content)) {
		t.Fatalf("verified metadata = %s/%d", verified.Digest(), verified.ByteSize())
	}
	if err := adapter.VerifyArtifact(context.Background(), verified.Digest(), verified.ByteSize()); err != nil {
		t.Fatalf("VerifyArtifact() error = %v", err)
	}
	read, err := adapter.Read(context.Background(), verified.Digest())
	if err != nil || string(read) != string(content) {
		t.Fatalf("Read() = %q, %v", read, err)
	}
	read[0] = '!'
	again, err := adapter.Read(context.Background(), verified.Digest())
	if err != nil || string(again) != string(content) {
		t.Fatal("Read exposed mutable CAS bytes")
	}
}

func TestAdapterFailsClosedForMissingMismatchedAndBoundedObjects(t *testing.T) {
	adapter := newTestAdapter(t)
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	content := []byte("exact")
	verified, err := adapter.Put(context.Background(), content, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.VerifyArtifact(context.Background(), verified.Digest(), int64(len(content)-1)); err == nil {
		t.Fatal("mismatched catalog size was accepted")
	}
	missing := agency.Sum([]byte("missing"))
	if err := adapter.VerifyArtifact(context.Background(), missing, int64(len("missing"))); err == nil {
		t.Fatal("missing object was accepted")
	}
	if _, err := adapter.Put(context.Background(), make([]byte, authority.MaxArtifactBytes+1), now); err == nil {
		t.Fatal("oversized object was accepted")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := adapter.VerifyArtifact(cancelled, verified.Digest(), verified.ByteSize()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled VerifyArtifact() = %v", err)
	}
}

func newTestAdapter(t *testing.T) *r7ArtifactAdapter {
	t.Helper()
	cas, err := artifact.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := newR7ArtifactAdapter(cas)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}
