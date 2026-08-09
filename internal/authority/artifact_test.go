package authority

import (
	"testing"
	"time"
)

func TestVerifyArtifactEnforcesExistingCASObjectBound(t *testing.T) {
	if MaxArtifactBytes != 4<<20 {
		t.Fatalf("MaxArtifactBytes = %d, want existing 4 MiB CAS bound", MaxArtifactBytes)
	}
	if _, err := VerifyArtifact(make([]byte, MaxArtifactBytes), time.Now()); err != nil {
		t.Fatalf("VerifyArtifact(exact bound) = %v", err)
	}
	if _, err := VerifyArtifact(make([]byte, MaxArtifactBytes+1), time.Now()); err == nil {
		t.Fatal("VerifyArtifact(over bound) succeeded")
	}
}
