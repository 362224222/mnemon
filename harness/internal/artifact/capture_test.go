package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

func TestCaptureWorkspaceRootsBuildsVerifiableReplayableClosure(t *testing.T) {
	workspace := t.TempDir()
	mustMkdir(t, filepath.Join(workspace, "bundle", "empty"))
	large := make([]byte, BlockSize+17)
	for index := range large {
		large[index] = byte(index % 251)
	}
	mustWrite(t, filepath.Join(workspace, "bundle", "result.bin"), large, 0o640)
	mustWrite(t, filepath.Join(workspace, "bundle", "empty.txt"), nil, 0o600)
	mustWrite(t, filepath.Join(workspace, "single.txt"), []byte("standalone"), 0o644)
	cas := newCaptureCAS(t)
	now := time.Date(2026, 7, 16, 18, 0, 0, 123, time.UTC)
	capturer, err := NewCapturer(workspace, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(context.Background(),
		[]string{"./bundle", "single.txt"}, cas)
	if err != nil {
		t.Fatal(err)
	}
	if closure.IsZero() || len(closure.Roots()) != 2 || len(closure.Blocks()) != 3 ||
		len(closure.BlockMap()) != 3 || closure.CapturedAt() != now {
		t.Fatalf("captured closure = roots %d blocks %d map %d at %s checkpoint %s",
			len(closure.Roots()), len(closure.Blocks()), len(closure.BlockMap()),
			closure.CapturedAt(), closure.Checkpoint().String())
	}
	if err := cas.VerifyClosure(context.Background(), closure); err != nil {
		t.Fatalf("VerifyClosure() error = %v", err)
	}
	for index := 1; index < len(closure.Roots()); index++ {
		if closure.Roots()[index-1].RootDigest.String() >= closure.Roots()[index].RootDigest.String() {
			t.Fatal("closure roots are not checkpoint-sorted")
		}
	}
	var bundle Manifest
	for _, root := range closure.Roots() {
		manifest, err := ParseManifest(root.Manifest.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if manifest.RootPath() == "bundle" {
			bundle = manifest
		}
	}
	if bundle.IsZero() || bundle.RootKind() != EntryDirectory || bundle.TotalBytes() != uint64(len(large)) {
		t.Fatalf("bundle manifest = %#v", bundle)
	}
	entries := bundle.Entries()
	if len(entries) != 4 || entries[0].LogicalPath != "bundle" ||
		entries[1].LogicalPath != "bundle/empty" || entries[2].LogicalPath != "bundle/empty.txt" ||
		entries[3].LogicalPath != "bundle/result.bin" || len(entries[3].Blocks) != 2 ||
		entries[3].Blocks[0].LengthBytes != BlockSize || entries[3].Blocks[1].LengthBytes != 17 {
		t.Fatalf("bundle entries = %#v", entries)
	}

	replayCapturer, _ := NewCapturer(workspace, func() time.Time { return now.Add(time.Hour) })
	replay, err := replayCapturer.Capture(context.Background(),
		[]string{"bundle", "single.txt"}, cas)
	if err != nil || !closure.SameContent(replay) || replay.Checkpoint().String() != closure.Checkpoint().String() {
		t.Fatalf("exact replay = same %t checkpoint %s err %v",
			closure.SameContent(replay), replay.Checkpoint().String(), err)
	}
	roots := replay.Roots()
	roots[0].TotalBytes++
	if replay.Roots()[0].TotalBytes == roots[0].TotalBytes {
		t.Fatal("Closure Roots getter mutated closure")
	}
}

func TestCaptureRejectsUnsafeWorkspacePathsAndObjectTypes(t *testing.T) {
	workspace := shortTempDir(t)
	mustWrite(t, filepath.Join(workspace, "safe.txt"), []byte("safe"), 0o600)
	mustMkdir(t, filepath.Join(workspace, ".mnemon", "harness"))
	mustWrite(t, filepath.Join(workspace, ".mnemon", "harness", "secret"), []byte("secret"), 0o600)
	outside := filepath.Join(t.TempDir(), "outside.txt")
	mustWrite(t, outside, []byte("outside"), 0o600)
	if err := os.Symlink(outside, filepath.Join(workspace, "link")); err != nil {
		t.Fatal(err)
	}
	outsideDirectory := t.TempDir()
	mustWrite(t, filepath.Join(outsideDirectory, "file"), []byte("outside directory"), 0o600)
	if err := os.Symlink(outsideDirectory, filepath.Join(workspace, "linkdir")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(workspace, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(workspace, "socket"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cas := newCaptureCAS(t)
	capturer, _ := NewCapturer(workspace, fixedCaptureClock())
	invalid := []struct {
		name  string
		roots []string
	}{
		{"empty", []string{""}},
		{"absolute", []string{outside}},
		{"traversal", []string{"../outside.txt"}},
		{"embedded traversal", []string{"dir/../safe.txt"}},
		{"empty component", []string{"dir//file"}},
		{"nul", []string{"safe\x00.txt"}},
		{"invalid UTF-8", []string{string([]byte{0xff})}},
		{"internal direct", []string{".mnemon/harness"}},
		{"internal through root", []string{"."}},
		{"symlink", []string{"link"}},
		{"symlink parent component", []string{"linkdir/file"}},
		{"fifo", []string{"pipe"}},
		{"socket", []string{"socket"}},
		{"duplicate", []string{"safe.txt", "./safe.txt"}},
	}
	if _, err := capturer.Capture(context.Background(), nil, cas); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("zero root count error = %v", err)
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := capturer.Capture(context.Background(), test.roots, cas); err == nil {
				t.Fatalf("Capture accepted roots %#v", test.roots)
			}
		})
	}
	tooMany := make([]string, MaxRoots+1)
	for index := range tooMany {
		tooMany[index] = "safe.txt"
	}
	if _, err := capturer.Capture(context.Background(), tooMany, cas); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("root count limit error = %v", err)
	}
}

func TestCaptureEnforcesEntryByteAndLogicalPathBounds(t *testing.T) {
	t.Run("entries", func(t *testing.T) {
		workspace := t.TempDir()
		mustMkdir(t, filepath.Join(workspace, "dir"))
		mustWrite(t, filepath.Join(workspace, "dir", "one"), []byte("1"), 0o600)
		mustWrite(t, filepath.Join(workspace, "dir", "two"), []byte("2"), 0o600)
		cas := newCaptureCAS(t)
		capturer, _ := NewCapturer(workspace, fixedCaptureClock())
		capturer.limits.maxEntries = 2
		if _, err := capturer.Capture(context.Background(),
			[]string{"dir"}, cas); !errors.Is(err, ErrArtifactLimit) {
			t.Fatalf("entry limit error = %v", err)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		workspace := t.TempDir()
		mustWrite(t, filepath.Join(workspace, "large"), []byte("12345"), 0o600)
		cas := newCaptureCAS(t)
		capturer, _ := NewCapturer(workspace, fixedCaptureClock())
		capturer.limits.maxTotal = 4
		if _, err := capturer.Capture(context.Background(),
			[]string{"large"}, cas); !errors.Is(err, ErrArtifactLimit) {
			t.Fatalf("byte limit error = %v", err)
		}
	})

	t.Run("logical path", func(t *testing.T) {
		workspace := t.TempDir()
		first := strings.Repeat("a", 250)
		second := strings.Repeat("b", 250)
		third := strings.Repeat("c", 20)
		mustMkdir(t, filepath.Join(workspace, first, second))
		mustWrite(t, filepath.Join(workspace, first, second, third), []byte("x"), 0o600)
		cas := newCaptureCAS(t)
		capturer, _ := NewCapturer(workspace, fixedCaptureClock())
		if _, err := capturer.Capture(context.Background(),
			[]string{filepath.Join(first, second, third)}, cas); !errors.Is(err, ErrArtifactPath) {
			t.Fatalf("logical path limit error = %v", err)
		}
	})
}

func TestCaptureDetectsPathReplacementAfterOpen(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "target.txt")
	mustWrite(t, target, []byte("original"), 0o600)
	outside := filepath.Join(t.TempDir(), "outside.txt")
	mustWrite(t, outside, []byte("outside secret"), 0o600)
	cas := newCaptureCAS(t)
	capturer, _ := NewCapturer(workspace, fixedCaptureClock())
	capturer.afterOpen = func(opened string) {
		if opened != target {
			return
		}
		capturer.afterOpen = nil
		if err := os.Rename(target, target+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, target); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := capturer.Capture(context.Background(),
		[]string{"target.txt"}, cas); !errors.Is(err, ErrArtifactChanged) {
		t.Fatalf("replacement race error = %v", err)
	}
}

func TestCaptureParentRenameAndSymlinkNeverReadsOutsideRoot(t *testing.T) {
	workspace := t.TempDir()
	selected := filepath.Join(workspace, "selected")
	mustMkdir(t, selected)
	mustWrite(t, filepath.Join(selected, "result.bin"), []byte("inside bytes"), 0o600)
	outsideDirectory := t.TempDir()
	outside := []byte("outside bytes must never enter Artifact CAS")
	mustWrite(t, filepath.Join(outsideDirectory, "result.bin"), outside, 0o600)

	cas := newCaptureCAS(t)
	capturer, err := NewCapturer(workspace, fixedCaptureClock())
	if err != nil {
		t.Fatal(err)
	}
	capturer.afterOpen = func(opened string) {
		if opened != selected {
			return
		}
		capturer.afterOpen = nil
		if err := os.Rename(selected, selected+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outsideDirectory, selected); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := capturer.Capture(context.Background(),
		[]string{"selected"}, cas); !errors.Is(err, ErrArtifactChanged) {
		t.Fatalf("parent replacement capture error = %v", err)
	}

	outsideDigest := model.Sum(outside)
	if _, err := cas.Read(outsideDigest, len(outside)); err == nil {
		t.Fatal("outside block entered CAS through replaced parent path")
	}
	outsideManifest, err := NewManifest(ManifestSpec{RootKind: EntryDirectory,
		RootPath: "selected", Entries: []ManifestEntry{
			{Kind: EntryDirectory, LogicalPath: "selected", Mode: 0o700},
			{Kind: EntryFile, LogicalPath: "selected/result.bin", Mode: 0o600,
				SizeBytes: uint64(len(outside)), Blocks: []ManifestBlock{{
					Digest: outsideDigest, LengthBytes: uint64(len(outside)),
				}}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Read(outsideManifest.ManifestDigest(), MaxManifestBytes); err == nil {
		t.Fatal("outside-derived manifest entered CAS through replaced parent path")
	}
}

func TestCaptureRejectsReplacedWorkspaceAuthority(t *testing.T) {
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	mustMkdir(t, workspace)
	mustWrite(t, filepath.Join(workspace, "file"), []byte("original"), 0o600)
	cas := newCaptureCAS(t)
	capturer, err := NewCapturer(workspace, fixedCaptureClock())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(workspace, workspace+".original"); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, workspace)
	mustWrite(t, filepath.Join(workspace, "file"), []byte("replacement"), 0o600)
	if _, err := capturer.Capture(context.Background(),
		[]string{"file"}, cas); !errors.Is(err, ErrArtifactChanged) {
		t.Fatalf("replaced workspace error = %v", err)
	}
}

func TestCaptureRejectsInvalidClock(t *testing.T) {
	workspace := t.TempDir()
	mustWrite(t, filepath.Join(workspace, "file"), []byte("content"), 0o600)
	cas := newCaptureCAS(t)
	capturer, err := NewCapturer(workspace, func() time.Time {
		return time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capturer.Capture(context.Background(), []string{"file"}, cas); err == nil {
		t.Fatal("Capture accepted a clock outside canonical Unix nanoseconds")
	}
}

func TestCaptureCancellationAndClosureTamperFailClosed(t *testing.T) {
	workspace := t.TempDir()
	mustWrite(t, filepath.Join(workspace, "file"), []byte("content"), 0o600)
	cas := newCaptureCAS(t)
	capturer, _ := NewCapturer(workspace, fixedCaptureClock())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := capturer.Capture(ctx, []string{"file"}, cas); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled capture error = %v", err)
	}
	closure, err := capturer.Capture(context.Background(), []string{"file"}, cas)
	if err != nil {
		t.Fatal(err)
	}
	tampered := closure
	tampered.blockMap = append([]RootBlock{}, closure.blockMap...)
	tampered.blockMap[0].LengthBytes++
	if err := cas.VerifyClosure(context.Background(), tampered); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("tampered closure error = %v", err)
	}
	tampered = closure
	tampered.roots = append([]CapturedRoot{}, closure.roots...)
	tampered.roots[0].VerifiedAt = tampered.roots[0].VerifiedAt.Add(time.Nanosecond)
	if err := cas.VerifyClosure(context.Background(), tampered); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("tampered closure time error = %v", err)
	}
	manifestPath, _ := cas.objectPath(closure.roots[0].ManifestDigest, false)
	if err := os.WriteFile(manifestPath, []byte("tampered"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := cas.VerifyClosure(context.Background(), closure); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("tampered manifest object error = %v", err)
	}
}

func newCaptureCAS(t *testing.T) *CAS {
	t.Helper()
	cas, err := NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}

func fixedCaptureClock() Clock {
	return func() time.Time { return time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC) }
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
}

func TestClosureCheckpointIsStoreCompatible(t *testing.T) {
	workspace := t.TempDir()
	mustWrite(t, filepath.Join(workspace, "file"), []byte("content"), 0o600)
	cas := newCaptureCAS(t)
	capturer, _ := NewCapturer(workspace, fixedCaptureClock())
	closure, err := capturer.Capture(context.Background(), []string{"file"}, cas)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint struct {
		Roots []struct {
			ManifestDigest string `json:"manifest_digest"`
			RootDigest     string `json:"root_digest"`
		} `json:"roots"`
	}
	if err := modelJSONUnmarshal(closure.Checkpoint(), &checkpoint); err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Roots) != 1 || checkpoint.Roots[0].RootDigest != closure.Roots()[0].RootDigest.String() ||
		checkpoint.Roots[0].ManifestDigest != closure.Roots()[0].ManifestDigest.String() {
		t.Fatalf("checkpoint projection = %#v", checkpoint)
	}
}

func modelJSONUnmarshal(value model.JSON, destination any) error {
	return json.Unmarshal(value.Bytes(), destination)
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "mn-art-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}
