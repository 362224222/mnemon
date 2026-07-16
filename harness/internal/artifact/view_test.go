package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

func TestViewMaterializerReconstructsFileDirectoryAndMultipleBlocks(t *testing.T) {
	t.Parallel()
	t.Run("nested file root and multiple blocks", func(t *testing.T) {
		fixture := newViewFixture(t)
		content := bytes.Repeat([]byte("mnemon-view-block\x00"), BlockSize/18+100)
		viewTestWrite(t, filepath.Join(fixture.workspace, "nested", "review.bin"), content, 0o755)
		root := fixture.capture(t, "nested/review.bin")
		run := viewTestRun(t, "file")
		result, err := fixture.materializer.Materialize(context.Background(), viewTestSpec(run, 0, root))
		if err != nil {
			t.Fatal(err)
		}
		if result.Replayed || result.LogicalPath != "nested/review.bin" ||
			result.RelativePath != "views/"+run.String()+"/0/nested/review.bin" {
			t.Fatalf("Materialize() = %#v", result)
		}
		got, err := os.ReadFile(result.Path)
		if err != nil || !bytes.Equal(got, content) {
			t.Fatalf("materialized bytes = %d, %v", len(got), err)
		}
		if len(root.manifest.Entries()[0].Blocks) < 2 {
			t.Fatal("fixture did not exercise a multi-block file")
		}
		assertViewTreeModes(t, result.Directory)
		assertViewMode(t, result.Path, viewFileMode, false)
		assertViewMode(t, filepath.Dir(result.Path), viewDirectoryMode, true)
	})

	t.Run("directory root", func(t *testing.T) {
		fixture := newViewFixture(t)
		viewTestWrite(t, filepath.Join(fixture.workspace, "bundle", "README.md"), []byte("review notes\n"), 0o644)
		viewTestWrite(t, filepath.Join(fixture.workspace, "bundle", "result.txt"), []byte("passed\n"), 0o600)
		if err := os.MkdirAll(filepath.Join(fixture.workspace, "bundle", "empty"), 0o755); err != nil {
			t.Fatal(err)
		}
		root := fixture.capture(t, "bundle")
		result, err := fixture.materializer.Materialize(context.Background(),
			viewTestSpec(viewTestRun(t, "directory"), 3, root))
		if err != nil {
			t.Fatal(err)
		}
		if result.Path != filepath.Join(result.Directory, "bundle") {
			t.Fatalf("materialized root path = %q", result.Path)
		}
		for relative, want := range map[string]string{
			"README.md": "review notes\n", "result.txt": "passed\n",
		} {
			got, err := os.ReadFile(filepath.Join(result.Path, filepath.FromSlash(relative)))
			if err != nil || string(got) != want {
				t.Fatalf("read %s = %q, %v", relative, got, err)
			}
		}
		assertViewMode(t, filepath.Join(result.Path, "empty"), viewDirectoryMode, true)
		assertViewTreeModes(t, result.Directory)
	})
}

func TestViewMaterializerRejectsCorruptionAndTraversalWithoutFinalView(t *testing.T) {
	t.Parallel()
	t.Run("block corruption on exact replay", func(t *testing.T) {
		fixture := newViewFixture(t)
		viewTestWrite(t, filepath.Join(fixture.workspace, "result.txt"), []byte("trusted result"), 0o644)
		root := fixture.capture(t, "result.txt")
		run := viewTestRun(t, "block-corrupt")
		spec := viewTestSpec(run, 0, root)
		if _, err := fixture.materializer.Materialize(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
		block := root.manifest.Entries()[0].Blocks[0]
		object, err := fixture.cas.objectPath(block.Digest, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(object, []byte("changed bytes!"), casObjectMode); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.materializer.Materialize(context.Background(), spec); !errors.Is(err, ErrViewCorruption) {
			t.Fatalf("corrupt replay error = %v", err)
		}
	})

	t.Run("manifest corruption", func(t *testing.T) {
		fixture := newViewFixture(t)
		viewTestWrite(t, filepath.Join(fixture.workspace, "result.txt"), []byte("trusted result"), 0o644)
		root := fixture.capture(t, "result.txt")
		object, err := fixture.cas.objectPath(root.root.ManifestDigest, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(object, []byte(`{"corrupt":true}`), casObjectMode); err != nil {
			t.Fatal(err)
		}
		run := viewTestRun(t, "manifest-corrupt")
		if _, err := fixture.materializer.Materialize(context.Background(), viewTestSpec(run, 0, root)); !errors.Is(err, ErrViewCorruption) {
			t.Fatalf("corrupt manifest error = %v", err)
		}
		assertNoPublishedView(t, fixture.nodeState, run, 0)
	})

	t.Run("traversing manifest", func(t *testing.T) {
		fixture := newViewFixture(t)
		wire := manifestReadWire{SchemaVersion: SchemaVersion, RootKind: EntryFile,
			RootPath: "../escape", TotalBytes: 0, Entries: []manifestReadEntry{{
				Blocks: []manifestReadBlock{}, Kind: EntryFile, LogicalPath: "../escape", Mode: 0o644,
			}}}
		raw, err := model.CanonicalMarshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		manifestDigest := model.Sum(raw)
		rootMaterial := append([]byte("mnemon/artifact-root/v1\x00"), raw...)
		rootDigest := model.Sum(rootMaterial)
		if _, err := fixture.cas.Put(manifestDigest, raw); err != nil {
			t.Fatal(err)
		}
		run := viewTestRun(t, "traversal")
		_, err = fixture.materializer.Materialize(context.Background(), ViewSpec{RunID: run,
			RootDigest: rootDigest, ManifestDigest: manifestDigest})
		if !errors.Is(err, ErrViewCorruption) {
			t.Fatalf("traversing manifest error = %v", err)
		}
		assertNoPublishedView(t, fixture.nodeState, run, 0)
		if _, err := os.Lstat(filepath.Join(fixture.nodeState, "escape")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("traversal created outside path: %v", err)
		}
	})
}

func TestViewMaterializerSerializesRaceReplayAndRejectsDifferentRoot(t *testing.T) {
	t.Parallel()
	fixture := newViewFixture(t)
	viewTestWrite(t, filepath.Join(fixture.workspace, "first.txt"), []byte("first root"), 0o644)
	firstRoot := fixture.capture(t, "first.txt")
	run := viewTestRun(t, "race")
	spec := viewTestSpec(run, 6, firstRoot)
	const callers = 12
	results := make([]MaterializedView, callers)
	errorsByCall := make([]error, callers)
	materializers := make([]*ViewMaterializer, callers)
	for index := range materializers {
		var err error
		materializers[index], err = NewViewMaterializer(fixture.nodeState, fixture.cas)
		if err != nil {
			t.Fatal(err)
		}
	}
	var wait sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			results[index], errorsByCall[index] = materializers[index].Materialize(context.Background(), spec)
		}(index)
	}
	close(start)
	wait.Wait()
	created := 0
	for index := range results {
		if errorsByCall[index] != nil {
			t.Fatalf("concurrent Materialize[%d] error = %v", index, errorsByCall[index])
		}
		if !results[index].Replayed {
			created++
		}
		if results[index].Path != results[0].Path || results[index].RootDigest != spec.RootDigest {
			t.Fatalf("concurrent result[%d] = %#v", index, results[index])
		}
	}
	if created != 1 {
		t.Fatalf("new materialization count = %d, want 1", created)
	}

	viewTestWrite(t, filepath.Join(fixture.workspace, "second.txt"), []byte("second root"), 0o644)
	secondRoot := fixture.capture(t, "second.txt")
	if _, err := fixture.materializer.Materialize(context.Background(), viewTestSpec(run, 6, secondRoot)); !errors.Is(err, ErrViewConflict) {
		t.Fatalf("different root error = %v", err)
	}
	got, err := os.ReadFile(results[0].Path)
	if err != nil || string(got) != "first root" {
		t.Fatalf("conflict changed original view = %q, %v", got, err)
	}
}

func TestViewMaterializerCoordinatesPublishBarrierAcrossInstances(t *testing.T) {
	t.Parallel()
	fixture := newViewFixture(t)
	viewTestWrite(t, filepath.Join(fixture.workspace, "barrier.txt"), []byte("one publication"), 0o644)
	root := fixture.capture(t, "barrier.txt")
	spec := viewTestSpec(viewTestRun(t, "barrier"), 0, root)
	first, err := NewViewMaterializer(fixture.nodeState, fixture.cas)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewViewMaterializer(fixture.nodeState, fixture.cas)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	first.afterPending = func() {
		close(entered)
		<-release
	}
	type outcome struct {
		view MaterializedView
		err  error
	}
	firstDone := make(chan outcome, 1)
	secondDone := make(chan outcome, 1)
	go func() {
		view, err := first.Materialize(context.Background(), spec)
		firstDone <- outcome{view, err}
	}()
	<-entered
	runPath := filepath.Join(fixture.nodeState, "views", spec.RunID.String())
	if _, err := os.Lstat(filepath.Join(runPath, viewPendingReceiptName("0"))); err != nil {
		t.Fatalf("publish barrier has no durable pending receipt: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(runPath, "0")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("publish barrier exposed final before rename: %v", err)
	}
	go func() {
		view, err := second.Materialize(context.Background(), spec)
		secondDone <- outcome{view, err}
	}()
	select {
	case early := <-secondDone:
		t.Fatalf("second instance crossed active publish barrier: %#v", early)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	created, replay := <-firstDone, <-secondDone
	if created.err != nil || replay.err != nil || created.view.Replayed || !replay.view.Replayed ||
		created.view.Path != replay.view.Path {
		t.Fatalf("barrier outcomes = created %#v replay %#v", created, replay)
	}
}

func TestViewMaterializerHidesInterruptedAndRejectsTamperedFinal(t *testing.T) {
	t.Parallel()
	fixture := newViewFixture(t)
	viewTestWrite(t, filepath.Join(fixture.workspace, "answer.txt"), []byte("forty two"), 0o644)
	root := fixture.capture(t, "answer.txt")
	run := viewTestRun(t, "interrupt")
	spec := viewTestSpec(run, 0, root)
	ctx, cancel := context.WithCancel(context.Background())
	fixture.materializer.beforePublish = cancel
	if _, err := fixture.materializer.Materialize(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted Materialize error = %v", err)
	}
	fixture.materializer.beforePublish = nil
	assertNoPublishedView(t, fixture.nodeState, run, 0)
	assertNoViewStages(t, filepath.Join(fixture.nodeState, "views", run.String()))

	result, err := fixture.materializer.Materialize(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "external.txt")
	if err := os.WriteFile(external, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(result.Directory, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(result.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, result.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(result.Directory, viewDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.materializer.Materialize(context.Background(), spec); !errors.Is(err, ErrViewCorruption) {
		t.Fatalf("tampered view replay error = %v", err)
	}
	outside, err := os.ReadFile(external)
	if err != nil || string(outside) != "outside" {
		t.Fatalf("symlink verification touched external file = %q, %v", outside, err)
	}

	deviceRun := viewTestRun(t, "device")
	deviceSpec := viewTestSpec(deviceRun, 0, root)
	deviceView, err := fixture.materializer.Materialize(context.Background(), deviceSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(deviceView.Directory, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(deviceView.Path); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(deviceView.Path, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(deviceView.Directory, viewDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.materializer.Materialize(context.Background(), deviceSpec); !errors.Is(err, ErrViewCorruption) {
		t.Fatalf("device view replay error = %v", err)
	}
}

func TestViewMaterializerRecoversRecognizableRestartLeftovers(t *testing.T) {
	t.Parallel()
	fixture := newViewFixture(t)
	viewTestWrite(t, filepath.Join(fixture.workspace, "restart.txt"), []byte("restart-safe"), 0o644)
	root := fixture.capture(t, "restart.txt")
	run := viewTestRun(t, "restart")
	spec := viewTestSpec(run, 0, root)
	runPath := filepath.Join(fixture.nodeState, "views", run.String())
	if err := os.MkdirAll(runPath, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(fixture.nodeState, "views"), viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(runPath, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(runPath, ".stage-0-crash")
	if err := os.Mkdir(stage, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "partial"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runPath, ".meta-stage-0-crash"), []byte("partial"), 0o400); err != nil {
		t.Fatal(err)
	}
	receipt, err := newViewReceipt(spec, root.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runPath, viewPendingReceiptName("0")),
		receipt.canonical.Bytes(), viewFileMode); err != nil {
		t.Fatal(err)
	}
	incompleteFinal := filepath.Join(runPath, "0")
	if err := os.Mkdir(incompleteFinal, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(incompleteFinal, "pre-mode-crash"),
		[]byte("complete bytes but publication not ready"), 0o400); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewViewMaterializer(fixture.nodeState, fixture.cas)
	if err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Materialize(context.Background(), spec)
	if err != nil || result.Replayed {
		t.Fatalf("restart Materialize = (%#v, %v)", result, err)
	}
	assertNoViewStages(t, runPath)
	got, err := os.ReadFile(result.Path)
	if err != nil || string(got) != "restart-safe" {
		t.Fatalf("restart view bytes = %q, %v", got, err)
	}
}

func TestViewMaterializerCleanupRunIsExactAndDoesNotFollowSymlinks(t *testing.T) {
	t.Parallel()
	fixture := newViewFixture(t)
	viewTestWrite(t, filepath.Join(fixture.workspace, "cleanup.txt"), []byte("retain CAS"), 0o644)
	root := fixture.capture(t, "cleanup.txt")
	runA, runB := viewTestRun(t, "cleanup-a"), viewTestRun(t, "cleanup-b")
	viewA, err := fixture.materializer.Materialize(context.Background(), viewTestSpec(runA, 0, root))
	if err != nil {
		t.Fatal(err)
	}
	viewB, err := fixture.materializer.Materialize(context.Background(), viewTestSpec(runB, 0, root))
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.materializer.CleanupRun(context.Background(), runA); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Dir(viewA.Directory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleaned Run still exists: %v", err)
	}
	if got, err := os.ReadFile(viewB.Path); err != nil || string(got) != "retain CAS" {
		t.Fatalf("cleanup touched other Run = %q, %v", got, err)
	}
	if _, err := fixture.cas.Read(root.root.ManifestDigest, MaxManifestBytes); err != nil {
		t.Fatalf("cleanup touched CAS manifest: %v", err)
	}
	if err := fixture.materializer.CleanupRun(context.Background(), runA); err != nil {
		t.Fatalf("idempotent CleanupRun error = %v", err)
	}

	unsafe, err := model.ParseRunID("../outside")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.materializer.CleanupRun(context.Background(), unsafe); !errors.Is(err, ErrViewInput) {
		t.Fatalf("unsafe cleanup error = %v", err)
	}
	external := t.TempDir()
	marker := filepath.Join(external, "marker")
	if err := os.WriteFile(marker, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkRun := viewTestRun(t, "cleanup-link")
	link := filepath.Join(fixture.nodeState, "views", linkRun.String())
	if err := os.Symlink(external, link); err != nil {
		t.Fatal(err)
	}
	if err := fixture.materializer.CleanupRun(context.Background(), linkRun); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != "safe" {
		t.Fatalf("cleanup followed Run symlink = %q, %v", got, err)
	}
	if _, err := os.Lstat(link); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup did not remove Run symlink: %v", err)
	}
}

type viewFixture struct {
	nodeState    string
	workspace    string
	cas          *CAS
	materializer *ViewMaterializer
}

type viewCapturedRoot struct {
	root     CapturedRoot
	manifest Manifest
}

func newViewFixture(t *testing.T) *viewFixture {
	t.Helper()
	nodeState := t.TempDir()
	workspace := t.TempDir()
	if err := os.Chmod(nodeState, viewControlDirectoryMode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { viewTestMakeWritable(nodeState) })
	cas, err := NewCAS(filepath.Join(nodeState, "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewViewMaterializer(nodeState, cas)
	if err != nil {
		t.Fatal(err)
	}
	return &viewFixture{nodeState: nodeState, workspace: workspace, cas: cas,
		materializer: materializer}
}

func (fixture *viewFixture) capture(t *testing.T, logical string) viewCapturedRoot {
	t.Helper()
	capturer, err := NewCapturer(fixture.workspace, fixture.cas, func() time.Time {
		return time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(context.Background(), []string{logical})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.cas.VerifyClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	roots := closure.Roots()
	if len(roots) != 1 {
		t.Fatalf("captured roots = %d", len(roots))
	}
	manifest, err := ParseManifest(roots[0].Manifest.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return viewCapturedRoot{root: roots[0], manifest: manifest}
}

func viewTestSpec(run model.RunID, ordinal uint32, root viewCapturedRoot) ViewSpec {
	return ViewSpec{RunID: run, Ordinal: ordinal, RootDigest: root.root.RootDigest,
		ManifestDigest: root.root.ManifestDigest}
}

func viewTestRun(t *testing.T, suffix string) model.RunID {
	t.Helper()
	run, err := model.ParseRunID("run-view-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func viewTestWrite(t *testing.T, name string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, content, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(name, mode); err != nil {
		t.Fatal(err)
	}
}

func assertViewTreeModes(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("view contains symlink %s", path)
		}
		if info.IsDir() && info.Mode().Perm() != viewDirectoryMode {
			return fmt.Errorf("directory %s mode = %04o", path, info.Mode().Perm())
		}
		if !info.IsDir() && (!info.Mode().IsRegular() || info.Mode().Perm() != viewFileMode) {
			return fmt.Errorf("file %s mode/type = %s", path, info.Mode())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertViewMode(t *testing.T, path string, mode os.FileMode, directory bool) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || info.IsDir() != directory || info.Mode().Perm() != mode ||
		info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("path %s mode = %#v, %v", path, info, err)
	}
}

func assertNoPublishedView(t *testing.T, nodeState string, run model.RunID, ordinal uint32) {
	t.Helper()
	runPath := filepath.Join(nodeState, "views", run.String())
	ordinalName := fmt.Sprintf("%d", ordinal)
	for _, path := range []string{filepath.Join(runPath, ordinalName),
		filepath.Join(runPath, viewReceiptName(ordinalName))} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial final path %s exists: %v", path, err)
		}
	}
}

func assertNoViewStages(t *testing.T, runPath string) {
	t.Helper()
	entries, err := os.ReadDir(runPath)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".stage-") || strings.HasPrefix(entry.Name(), ".meta-stage-") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) != 0 {
		t.Fatalf("recognizable staging leftovers = %v", names)
	}
}

func viewTestMakeWritable(root string) {
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			_ = os.Chmod(path, 0o700)
		} else if info.Mode().IsRegular() {
			_ = os.Chmod(path, 0o600)
		}
		return nil
	})
}
