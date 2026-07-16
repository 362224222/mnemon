package localapi

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestContextFileOwnerFencedLifecycle(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	runID, err := model.ParseRunID("run-current-1")
	if err != nil {
		t.Fatal(err)
	}
	token := testOpaqueValue(0x11)
	context, err := WriteContextFile(nodeState, runID, token)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(nodeState, "runs", runID.String()+contextFileSuffix)
	if context.Path() != wantPath || context.RunID() != runID || context.HeaderValue() != token {
		t.Fatalf("context identity = (%q, %q, %q)", context.Path(), context.RunID(), context.HeaderValue())
	}
	wantBytes := []byte(token + "\n")
	if context.Digest() != model.Sum(wantBytes) {
		t.Fatalf("context digest = %s, want %s", context.Digest(), model.Sum(wantBytes))
	}
	assertOwnerPath(t, filepath.Join(nodeState, "runs"), true, ownerDirectoryMode)
	assertOwnerPath(t, wantPath, false, ownerRegularFileMode)
	raw, err := os.ReadFile(wantPath)
	if err != nil || string(raw) != string(wantBytes) {
		t.Fatalf("context bytes = %q, %v", raw, err)
	}

	read, err := ReadContextFile(nodeState, wantPath)
	if err != nil {
		t.Fatal(err)
	}
	if read.Digest() != context.Digest() || read.HeaderValue() != token ||
		!os.SameFile(read.identity, context.identity) {
		t.Fatalf("read context does not preserve identity")
	}
	repeated, err := WriteContextFile(nodeState, runID, token)
	if err != nil || !os.SameFile(repeated.identity, context.identity) {
		t.Fatalf("idempotent write = %#v, %v", repeated, err)
	}
	if _, err := WriteContextFile(nodeState, runID, testOpaqueValue(0x22)); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("different token write error = %v", err)
	}
	if raw, err := os.ReadFile(wantPath); err != nil || string(raw) != string(wantBytes) {
		t.Fatalf("different write changed context = %q, %v", raw, err)
	}
	if err := RemoveContextFile(nodeState, read); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(wantPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("context remains after exact removal: %v", err)
	}
}

func TestContextFileRejectsPathsTypesModesOwnersAndBytes(t *testing.T) {
	t.Parallel()
	t.Run("paths", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		runID, _ := model.ParseRunID("run-path")
		context, err := WriteContextFile(nodeState, runID, testOpaqueValue(0x31))
		if err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(nodeState, "outside.context")
		if err := os.WriteFile(outside, []byte(testOpaqueValue(0x31)+"\n"), ownerRegularFileMode); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"relative.context", outside,
			filepath.Join(nodeState, "runs", "nested", "run.context"), context.Path() + string(filepath.Separator) + ".."} {
			if _, err := ReadContextFile(nodeState, path); !errors.Is(err, ErrUnsafeClientState) {
				t.Fatalf("ReadContextFile(%q) error = %v", path, err)
			}
		}
		unsafeRun, err := model.ParseRunID("../escape")
		if err != nil {
			t.Fatalf("fixture should show model ID is not a filesystem name: %v", err)
		}
		if _, err := WriteContextFile(nodeState, unsafeRun, testOpaqueValue(0x32)); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("unsafe Run write error = %v", err)
		}
	})

	mutations := []struct {
		name   string
		mutate func(*testing.T, string, string)
	}{
		{"world-readable", func(t *testing.T, path, _ string) { mustChmod(t, path, 0o644) }},
		{"extra-newline", func(t *testing.T, path, token string) {
			mustWrite(t, path, []byte(token+"\n\n"), ownerRegularFileMode)
		}},
		{"padded-base64", func(t *testing.T, path, token string) {
			mustWrite(t, path, []byte(token+"=\n"), ownerRegularFileMode)
		}},
		{"missing-newline", func(t *testing.T, path, token string) {
			mustWrite(t, path, []byte(token), ownerRegularFileMode)
		}},
		{"directory", func(t *testing.T, path, _ string) {
			mustRemove(t, path)
			if err := os.Mkdir(path, ownerRegularFileMode); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", func(t *testing.T, path, token string) {
			mustRemove(t, path)
			target := path + ".target"
			mustWrite(t, target, []byte(token+"\n"), ownerRegularFileMode)
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{"fifo", func(t *testing.T, path, _ string) {
			mustRemove(t, path)
			if err := syscall.Mkfifo(path, uint32(ownerRegularFileMode)); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			nodeState := newClientNodeState(t)
			runID, _ := model.ParseRunID("run-unsafe")
			token := testOpaqueValue(0x41)
			context, err := WriteContextFile(nodeState, runID, token)
			if err != nil {
				t.Fatal(err)
			}
			mutation.mutate(t, context.Path(), token)
			if _, err := ReadContextFile(nodeState, context.Path()); !errors.Is(err, ErrUnsafeClientState) {
				t.Fatalf("unsafe context error = %v", err)
			}
		})
	}

	t.Run("unsafe-run-directory", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		runs := filepath.Join(nodeState, "runs")
		if err := os.Mkdir(runs, ownerDirectoryMode); err != nil {
			t.Fatal(err)
		}
		mustChmod(t, runs, 0o755)
		if _, err := ReadContextFile(nodeState, filepath.Join(runs, "run.context")); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("unsafe runs directory error = %v", err)
		}
	})

	wrongUID := uint32(os.Geteuid()) ^ 1
	fake := clientFileInfo{mode: ownerRegularFileMode, sys: &syscall.Stat_t{Uid: wrongUID}}
	if err := validateOwnerRegularFile(fake, uint32(os.Geteuid())); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("wrong-owner file error = %v", err)
	}
	fake.mode = os.ModeDir | ownerDirectoryMode
	if err := validateOwnerDirectory(fake, uint32(os.Geteuid())); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("wrong-owner directory error = %v", err)
	}
}

func TestContextFileRemovalPreservesReplacementIdentity(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	runID, _ := model.ParseRunID("run-replacement")
	expected, err := WriteContextFile(nodeState, runID, testOpaqueValue(0x51))
	if err != nil {
		t.Fatal(err)
	}
	moved := expected.Path() + ".original"
	if err := os.Rename(expected.Path(), moved); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(moved)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, expected.Path(), raw, ownerRegularFileMode)
	if err := RemoveContextFile(nodeState, expected); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("replacement removal error = %v", err)
	}
	if current, err := os.ReadFile(expected.Path()); err != nil || string(current) != string(raw) {
		t.Fatalf("replacement was not preserved = %q, %v", current, err)
	}
	mustRemove(t, expected.Path())
	if err := os.Rename(moved, expected.Path()); err != nil {
		t.Fatal(err)
	}
	if err := RemoveContextFile(nodeState, expected); err != nil {
		t.Fatalf("restored exact identity removal = %v", err)
	}
}

func newClientNodeState(t *testing.T) string {
	t.Helper()
	nodeState := filepath.Join(shortTempDir(t), "node")
	if err := os.Mkdir(nodeState, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return nodeState
}

func testOpaqueValue(fill byte) string {
	raw := make([]byte, opaqueSecretBytes)
	for index := range raw {
		raw[index] = fill
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func assertOwnerPath(t *testing.T, path string, directory bool, mode os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() != directory || info.Mode().Perm() != mode {
		t.Fatalf("%s mode = %v, directory = %v", path, info.Mode(), info.IsDir())
	}
	uid, err := fileOwnerUID(info)
	if err != nil || uid != uint32(os.Geteuid()) {
		t.Fatalf("%s owner = %d, %v", path, uid, err)
	}
}

func mustWrite(t *testing.T, path string, raw []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func mustChmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

type clientFileInfo struct {
	mode os.FileMode
	sys  any
}

func (f clientFileInfo) Name() string       { return "managed" }
func (f clientFileInfo) Size() int64        { return 0 }
func (f clientFileInfo) Mode() os.FileMode  { return f.mode }
func (f clientFileInfo) ModTime() time.Time { return time.Time{} }
func (f clientFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f clientFileInfo) Sys() any           { return f.sys }

func TestContextFileErrorsNeverExposeCapability(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	runID, _ := model.ParseRunID("run-secret")
	secret := testOpaqueValue(0x61)
	if _, err := WriteContextFile(nodeState, runID, secret+"="); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("invalid context error leaks capability: %v", err)
	}
}
