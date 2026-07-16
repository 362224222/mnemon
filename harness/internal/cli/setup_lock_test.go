package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetupLockSerializesTwentyConcurrentTransactions(t *testing.T) {
	const callers = 20
	nodeState := newSetupLockNodeState(t)
	start := make(chan struct{})
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	var active atomic.Int32
	var maximum atomic.Int32
	var visits atomic.Int32
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			<-start
			lock, err := acquireSetupLock(context.Background(), nodeState)
			if err != nil {
				errorsFound <- err
				return
			}
			current := active.Add(1)
			for {
				seen := maximum.Load()
				if current <= seen || maximum.CompareAndSwap(seen, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			visits.Add(1)
			active.Add(-1)
			if err := lock.Close(); err != nil {
				errorsFound <- err
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("setup transaction: %v", err)
	}
	if visits.Load() != callers || maximum.Load() != 1 || active.Load() != 0 {
		t.Fatalf("visits=%d maximum=%d active=%d", visits.Load(), maximum.Load(), active.Load())
	}
	assertSetupLockFile(t, nodeState)
}

func TestSetupLockHonorsCancellationWithoutStealingAuthority(t *testing.T) {
	nodeState := newSetupLockNodeState(t)
	held, err := acquireSetupLock(context.Background(), nodeState)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	contender, err := acquireSetupLock(ctx, nodeState)
	if contender != nil || !errors.Is(err, context.DeadlineExceeded) ||
		time.Since(started) > 500*time.Millisecond {
		t.Fatalf("acquireSetupLock() = (%#v, %v), elapsed=%v", contender, err,
			time.Since(started))
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := acquireSetupLock(context.Background(), nodeState)
	if err != nil {
		t.Fatalf("lock was stolen or leaked: %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}

	canceled, stop := context.WithCancel(context.Background())
	stop()
	fresh := newSetupLockNodeState(t)
	if lock, err := acquireSetupLock(canceled, fresh); lock != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled acquireSetupLock() = (%#v, %v)", lock, err)
	}
	if _, err := os.Lstat(filepath.Join(fresh, setupLockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-canceled acquisition created a lock: %v", err)
	}
}

func TestSetupLockRejectsUnsafeNodeAndLockPaths(t *testing.T) {
	t.Run("relative Node", func(t *testing.T) {
		lock, err := acquireSetupLock(context.Background(), "relative")
		if lock != nil || !errors.Is(err, errSetupLock) {
			t.Fatalf("acquireSetupLock() = (%#v, %v)", lock, err)
		}
	})
	t.Run("missing Node", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing")
		lock, err := acquireSetupLock(context.Background(), path)
		if lock != nil || !errors.Is(err, errSetupLock) {
			t.Fatalf("acquireSetupLock() = (%#v, %v)", lock, err)
		}
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("acquisition created Node state: %v", statErr)
		}
	})
	t.Run("public Node", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "node")
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
		lock, err := acquireSetupLock(context.Background(), path)
		if lock != nil || !errors.Is(err, errSetupLock) {
			t.Fatalf("acquireSetupLock() = (%#v, %v)", lock, err)
		}
	})
	t.Run("symlink Node", func(t *testing.T) {
		target := newSetupLockNodeState(t)
		path := filepath.Join(t.TempDir(), "node-link")
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		lock, err := acquireSetupLock(context.Background(), path)
		if lock != nil || !errors.Is(err, errSetupLock) {
			t.Fatalf("acquireSetupLock() = (%#v, %v)", lock, err)
		}
	})

	tests := map[string]func(*testing.T, string){
		"symlink lock": func(t *testing.T, path string) {
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, nil, setupLockMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		},
		"public lock": func(t *testing.T, path string) {
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"directory lock": func(t *testing.T, path string) {
			if err := os.Mkdir(path, setupLockMode); err != nil {
				t.Fatal(err)
			}
		},
		"hard-linked lock": func(t *testing.T, path string) {
			if err := os.WriteFile(path, nil, setupLockMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(path, filepath.Join(filepath.Dir(path), "second-link")); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			nodeState := newSetupLockNodeState(t)
			prepare(t, filepath.Join(nodeState, setupLockName))
			lock, err := acquireSetupLock(context.Background(), nodeState)
			if lock != nil || !errors.Is(err, errSetupLock) {
				t.Fatalf("acquireSetupLock() = (%#v, %v)", lock, err)
			}
		})
	}
}

func TestSetupLockDetectsNodeAndLockReplacement(t *testing.T) {
	t.Run("lock replacement before acquisition", func(t *testing.T) {
		nodeState := newSetupLockNodeState(t)
		state, err := openSetupLockNodeState(nodeState)
		if err != nil {
			t.Fatal(err)
		}
		file, err := openSetupLockFile(state)
		if err != nil {
			_ = state.close()
			t.Fatal(err)
		}
		lock := &setupLock{state: state, file: file}
		original := filepath.Join(nodeState, setupLockName)
		moved := filepath.Join(nodeState, "old.setup.lock")
		if err := os.Rename(original, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(original, nil, setupLockMode); err != nil {
			t.Fatal(err)
		}
		if err := validateSetupLock(lock); !errors.Is(err, errSetupLock) {
			t.Fatalf("validateSetupLock() error = %v", err)
		}
		if err := lock.close(false); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("lock replacement while held", func(t *testing.T) {
		nodeState := newSetupLockNodeState(t)
		lock, err := acquireSetupLock(context.Background(), nodeState)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(nodeState, setupLockName)
		if err := os.Rename(path, filepath.Join(nodeState, "displaced.setup.lock")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, setupLockMode); err != nil {
			t.Fatal(err)
		}
		if err := lock.Close(); !errors.Is(err, errSetupLock) {
			t.Fatalf("Close() error = %v", err)
		}
	})
	t.Run("Node replacement while held", func(t *testing.T) {
		parent := t.TempDir()
		nodeState := filepath.Join(parent, "node")
		if err := os.Mkdir(nodeState, setupNodeMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(nodeState, setupNodeMode); err != nil {
			t.Fatal(err)
		}
		lock, err := acquireSetupLock(context.Background(), nodeState)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(nodeState, filepath.Join(parent, "old-node")); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(nodeState, setupNodeMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(nodeState, setupNodeMode); err != nil {
			t.Fatal(err)
		}
		if err := lock.Close(); !errors.Is(err, errSetupLock) {
			t.Fatalf("Close() error = %v", err)
		}
	})
}

func TestSetupLockCloseIsIdempotentAndPublishesExactMode(t *testing.T) {
	nodeState := newSetupLockNodeState(t)
	lock, err := acquireSetupLock(context.Background(), nodeState)
	if err != nil {
		t.Fatal(err)
	}
	assertSetupLockFile(t, nodeState)
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func newSetupLockNodeState(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(path, setupNodeMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, setupNodeMode); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertSetupLockFile(t *testing.T, nodeState string) {
	t.Helper()
	info, err := os.Lstat(filepath.Join(nodeState, setupLockName))
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != setupLockMode {
		t.Fatalf("setup.lock mode = %v", info.Mode())
	}
}
