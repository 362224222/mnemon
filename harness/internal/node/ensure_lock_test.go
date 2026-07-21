package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnsureLockCreatesValidatedOwnerFile(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	lock, err := acquireEnsureLock(context.Background(), nodeState, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.close()
	if err := validateHeldEnsureLock(lock, nodeState); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(nodeState, ensureLockName))
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != ensureLockMode {
		t.Fatalf("ensure lock mode = %s", info.Mode())
	}
}
