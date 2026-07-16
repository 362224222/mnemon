package store

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenConfiguresPrivateStore(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "nested", "node")
	path := filepath.Join(stateDir, "node.db")

	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if !filepath.IsAbs(st.Path()) {
		t.Fatalf("Path() = %q, want absolute path", st.Path())
	}

	var journalMode string
	var synchronous, foreignKeys, busyTimeout, version int
	for query, destination := range map[string]any{
		"PRAGMA journal_mode": &journalMode,
		"PRAGMA synchronous":  &synchronous,
		"PRAGMA foreign_keys": &foreignKeys,
		"PRAGMA busy_timeout": &busyTimeout,
		"PRAGMA user_version": &version,
	} {
		if err := st.db.QueryRow(query).Scan(destination); err != nil {
			t.Fatalf("%s error = %v", query, err)
		}
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Errorf("journal_mode = %q, want wal", journalMode)
	}
	if synchronous != 2 {
		t.Errorf("synchronous = %d, want FULL (2)", synchronous)
	}
	if foreignKeys != 1 {
		t.Errorf("foreign_keys = %d, want 1", foreignKeys)
	}
	if busyTimeout != busyTimeoutMS {
		t.Errorf("busy_timeout = %d, want %d", busyTimeout, busyTimeoutMS)
	}
	if version != SchemaVersion {
		t.Errorf("user_version = %d, want %d", version, SchemaVersion)
	}

	assertMode(t, stateDir, directoryMode)
	assertMode(t, path, privateFileMode)
	assertMode(t, path+".writer.lock", privateFileMode)
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Lstat(sidecar); err == nil {
			assertMode(t, sidecar, privateFileMode)
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Lstat(%q) error = %v", sidecar, err)
		}
	}

	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestOpenWriterGuardAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node", "node.db")
	first, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}

	started := time.Now()
	second, err := Open(context.Background(), path)
	if second != nil {
		_ = second.Close()
		t.Fatal("second Open() returned a Store")
	}
	if !errors.Is(err, ErrWriterActive) {
		t.Fatalf("second Open() error = %v, want ErrWriterActive", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("second Open() took %v, want immediate rejection", elapsed)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestOpenWriterGuardHelper$")
	command.Env = append(os.Environ(), "MNEMON_TEST_LOCKED_DB="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("writer helper error = %v\n%s", err, output)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	restarted, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() after Close error = %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatalf("restarted Close() error = %v", err)
	}
}

func TestOpenWriterGuardHelper(t *testing.T) {
	path := os.Getenv("MNEMON_TEST_LOCKED_DB")
	if path == "" {
		return
	}
	st, err := Open(context.Background(), path)
	if st != nil {
		_ = st.Close()
		t.Fatal("Open() acquired a second process writer")
	}
	if !errors.Is(err, ErrWriterActive) {
		t.Fatalf("Open() error = %v, want ErrWriterActive", err)
	}
}

func TestOpenPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := st.db.Exec(
		"INSERT INTO node(singleton, peer_id, origin_epoch, active_asset_rev, created_at, updated_at) VALUES(1, ?, ?, ?, ?, ?)",
		"peer-one",
		"epoch-one",
		"asset-one",
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert node: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	st, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("restart Open() error = %v", err)
	}
	defer st.Close()
	var peerID, epoch string
	if err := st.db.QueryRow("SELECT peer_id, origin_epoch FROM node WHERE singleton = 1").Scan(&peerID, &epoch); err != nil {
		t.Fatalf("read node after restart: %v", err)
	}
	if peerID != "peer-one" || epoch != "epoch-one" {
		t.Fatalf("node after restart = (%q, %q), want persisted identity", peerID, epoch)
	}
}

func TestOpenRepairsOwnerOnlyModes(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, "node.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer st.Close()
	assertMode(t, stateDir, directoryMode)
	assertMode(t, path, privateFileMode)
	assertMode(t, path+".writer.lock", privateFileMode)
}

func TestOpenRejectsInvalidPaths(t *testing.T) {
	if st, err := Open(context.Background(), " "); err == nil || st != nil {
		if st != nil {
			_ = st.Close()
		}
		t.Fatalf("Open(empty) = (%v, %v), want error", st, err)
	}

	stateDir := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(stateDir, directoryMode); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(stateDir, "target")
	if err := os.WriteFile(target, nil, privateFileMode); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(stateDir, "node.db")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if st, err := Open(context.Background(), symlink); err == nil || st != nil {
		if st != nil {
			_ = st.Close()
		}
		t.Fatalf("Open(symlink) = (%v, %v), want error", st, err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(%q) error = %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %04o, want %04o", path, got, want)
	}
}
