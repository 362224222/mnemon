package artifact

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestStageOwnerMarkerIsCanonicalAndExcludedFromLayout(t *testing.T) {
	cas := openTestCAS(t)
	owner := testOperationStageOwner(t, "operation-stage-marker", 7)
	stage, err := cas.OpenStage(owner)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("marker is not staged content")
	digest := model.Sum(content)
	if _, err := stage.Put(digest, content); err != nil {
		t.Fatal(err)
	}

	markerPath := filepath.Join(stage.path, stageOwnerMarkerName)
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := owner.markerBytes()
	if err != nil || !bytes.Equal(marker, expected) {
		t.Fatalf("owner marker = (%s,%v), want %s", marker, err, expected)
	}
	const canonical = `{"generation":7,"id":"operation-stage-marker","kind":"operation"}`
	if string(marker) != canonical {
		t.Fatalf("canonical owner marker = %q, want %q", marker, canonical)
	}
	parsed, snapshot, err := readStageOwnerMarker(markerPath)
	if err != nil || parsed != owner || snapshot.Mode().Perm() != casObjectMode {
		t.Fatalf("read owner marker = (%#v,%v,%v)", parsed, snapshot, err)
	}
	for _, malformed := range [][]byte{
		[]byte(`{"kind":"operation","id":"operation-stage-marker","generation":7}`),
		[]byte(canonical[:len(canonical)-1] + `,"extra":true}`),
	} {
		if _, err := parseStageOwnerMarker(malformed); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("noncanonical marker %q error = %v", malformed, err)
		}
	}

	cas.coordination.staging.Lock()
	layout, err := stage.scanLocked(false)
	cas.coordination.staging.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if layout.marker == nil || layout.markerMissing || len(layout.objects) != 1 ||
		layout.bytes != uint64(len(content)) {
		t.Fatalf("marker-aware stage layout = %#v", layout)
	}
	if _, found := layout.objects[digest]; !found {
		t.Fatal("marker-aware stage scan lost staged object")
	}
}

func TestStageMarkerRepairAndMissingMarkerFailureBoundaries(t *testing.T) {
	t.Run("empty directory is repaired by exact owner", func(t *testing.T) {
		cas := openTestCAS(t)
		owner := testInboxStageOwner(t, "inbox-stage-marker-repair", 1)
		stage, _ := cas.OpenStage(owner)
		if err := os.Mkdir(stage.path, casDirectoryMode); err != nil {
			t.Fatal(err)
		}
		content := []byte("first object follows repaired marker")
		if _, err := stage.Put(model.Sum(content), content); err != nil {
			t.Fatal(err)
		}
		parsed, _, err := readStageOwnerMarker(
			filepath.Join(stage.path, stageOwnerMarkerName))
		if err != nil || parsed != owner {
			t.Fatalf("repaired marker = (%#v,%v)", parsed, err)
		}
	})

	t.Run("nonempty missing marker fails closed", func(t *testing.T) {
		cas := openTestCAS(t)
		stage, _ := cas.OpenStage(testOperationStageOwner(
			t, "operation-stage-marker-missing", 1))
		if err := os.Mkdir(stage.path, casDirectoryMode); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stage.path, strings.Repeat("a", 64)),
			[]byte("unowned"), casObjectMode); err != nil {
			t.Fatal(err)
		}
		content := []byte("must not enter unmarked directory")
		if _, err := stage.Put(model.Sum(content), content); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("nonempty missing marker Put error = %v", err)
		}
	})

	t.Run("malformed marker fails closed", func(t *testing.T) {
		cas := openTestCAS(t)
		stage, _ := cas.OpenStage(testInboxStageOwner(
			t, "inbox-stage-marker-malformed", 1))
		content := []byte("existing staged object")
		digest := model.Sum(content)
		if _, err := stage.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		markerPath := filepath.Join(stage.path, stageOwnerMarkerName)
		if err := os.WriteFile(markerPath, []byte("{}"), casObjectMode); err != nil {
			t.Fatal(err)
		}
		if _, err := stage.Read(digest, len(content)); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("malformed marker Read error = %v", err)
		}
	})
}

func TestStagePutSyncsParentAfterMarkerBeforeObject(t *testing.T) {
	for _, setup := range []struct {
		name string
		run  func(*testing.T, *Stage)
	}{
		{name: "fresh"},
		{name: "empty repair", run: func(t *testing.T, stage *Stage) {
			t.Helper()
			if err := os.Mkdir(stage.path, casDirectoryMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "marker present retry", run: func(t *testing.T, stage *Stage) {
			t.Helper()
			if err := os.Mkdir(stage.path, casDirectoryMode); err != nil {
				t.Fatal(err)
			}
			info, err := os.Lstat(stage.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stage.writeOwnerMarkerLocked(info); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(setup.name, func(t *testing.T) {
			cas := openTestCAS(t)
			stage, err := cas.OpenStage(testInboxStageOwner(
				t, "inbox-stage-parent-sync-"+strings.ReplaceAll(setup.name, " ", "-"), 1))
			if err != nil {
				t.Fatal(err)
			}
			if setup.run != nil {
				setup.run(t, stage)
			}
			content := []byte("must follow durable stage parent")
			digest := model.Sum(content)
			cas.coordination.staging.Lock()
			_, faultErr := stage.putLocked(digest, content,
				filepath.Join(t.TempDir(), "missing-staging-parent"))
			cas.coordination.staging.Unlock()
			if faultErr == nil ||
				!strings.Contains(faultErr.Error(), "open Artifact CAS directory for fsync") {
				t.Fatalf("Put with failed parent fsync error = %v", faultErr)
			}
			entries, err := os.ReadDir(stage.path)
			if err != nil || len(entries) != 1 || entries[0].Name() != stageOwnerMarkerName {
				t.Fatalf("object preceded parent fsync after %s = (%v,%v)",
					setup.name, entries, err)
			}
			if _, err := stage.Put(digest, content); err != nil {
				t.Fatalf("Put after %s: %v", setup.name, err)
			}
			if _, _, err := readStageOwnerMarker(
				filepath.Join(stage.path, stageOwnerMarkerName)); err != nil {
				t.Fatalf("owner marker after %s Put: %v", setup.name, err)
			}
			if stored, err := stage.Read(digest, len(content)); err != nil ||
				!bytes.Equal(stored, content) {
				t.Fatalf("object after %s Put = (%q,%v)", setup.name, stored, err)
			}
		})
	}
}

func TestScanStageDirectoriesIsBoundedAndNeverGuessesOwner(t *testing.T) {
	cas := openTestCAS(t)
	owned, old := makeStageDirectoryScanFixtures(t, cas)
	candidates := scanAllStageDirectories(t, cas, 2)
	assertStageDirectoryScanCandidates(t, cas, candidates, owned, old)
}

func makeStageDirectoryScanFixtures(t *testing.T,
	cas *CAS,
) (StageOwner, time.Time) {
	t.Helper()
	owned := testOperationStageOwner(t, "operation-stage-scan-owned", 2)
	ownedStage, _ := cas.OpenStage(owned)
	content := []byte("owned scan bytes")
	if _, err := ownedStage.Put(model.Sum(content), content); err != nil {
		t.Fatal(err)
	}

	empty := makeUnmarkedStageDirectory(t, cas,
		testInboxStageOwner(t, "inbox-stage-scan-empty", 3))
	old := time.Now().Add(-2 * time.Hour).Round(0)
	if err := os.Chtimes(empty, old, old); err != nil {
		t.Fatal(err)
	}

	missing := makeUnmarkedStageDirectory(t, cas,
		testInboxStageOwner(t, "inbox-stage-scan-missing", 4))
	if err := os.WriteFile(filepath.Join(missing, "unexpected"),
		[]byte("not owner authority"), casObjectMode); err != nil {
		t.Fatal(err)
	}

	invalid := makeUnmarkedStageDirectory(t, cas,
		testOperationStageOwner(t, "operation-stage-scan-invalid", 5))
	invalidTarget := filepath.Join(t.TempDir(), "owner.json")
	if err := os.WriteFile(invalidTarget, []byte("{}"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(invalidTarget,
		filepath.Join(invalid, stageOwnerMarkerName)); err != nil {
		t.Fatal(err)
	}

	markerOwner := testOperationStageOwner(t, "operation-stage-scan-mismatch", 6)
	mismatchStage, _ := cas.OpenStage(markerOwner)
	if _, err := mismatchStage.Put(model.Sum([]byte("mismatch")),
		[]byte("mismatch")); err != nil {
		t.Fatal(err)
	}
	other := testOperationStageOwner(t, "operation-stage-scan-other", 6)
	otherName, _ := other.directoryName()
	if err := os.Rename(mismatchStage.path, filepath.Join(cas.staging, otherName)); err != nil {
		t.Fatal(err)
	}

	unsafeName := strings.Repeat("f", 64)
	if err := os.Symlink(t.TempDir(), filepath.Join(cas.staging, unsafeName)); err != nil {
		t.Fatal(err)
	}
	return owned, old
}

func assertStageDirectoryScanCandidates(t *testing.T, cas *CAS,
	candidates []StageDirectoryCandidate, owned StageOwner, old time.Time,
) {
	t.Helper()
	counts := make(map[StageDirectoryStatus]int)
	for _, candidate := range candidates {
		counts[candidate.Status()]++
		assertStageDirectoryCandidateOwner(t, candidate, owned, old)
	}
	assertStageDirectoryStatusCounts(t, counts)
	assertDiagnosticStageRemovalRejected(t, cas, candidates)
}

func assertStageDirectoryCandidateOwner(t *testing.T,
	candidate StageDirectoryCandidate, owned StageOwner, old time.Time,
) {
	t.Helper()
	if candidate.Status() == StageDirectoryOwned {
		if candidate.Owner() != owned {
			t.Fatalf("owned scan candidate = %#v, want %#v",
				candidate.Owner(), owned)
		}
		return
	}
	if !candidate.Owner().IsZero() {
		t.Fatalf("status %q guessed owner %#v",
			candidate.Status(), candidate.Owner())
	}
	if candidate.Status() == StageDirectoryEmptyUnmarked &&
		!candidate.ModifiedAt().Equal(old) {
		t.Fatalf("empty unmarked mtime = %s, want %s",
			candidate.ModifiedAt(), old)
	}
}

func assertStageDirectoryStatusCounts(t *testing.T,
	counts map[StageDirectoryStatus]int,
) {
	t.Helper()
	for _, status := range []StageDirectoryStatus{
		StageDirectoryOwned,
		StageDirectoryEmptyUnmarked,
		StageDirectoryMissingMarker,
		StageDirectoryInvalidMarker,
		StageDirectoryOwnerMismatch,
		StageDirectoryUnsafe,
	} {
		if counts[status] != 1 {
			t.Fatalf("scan status %q count = %d, all %#v",
				status, counts[status], counts)
		}
	}
}

func assertDiagnosticStageRemovalRejected(t *testing.T, cas *CAS,
	candidates []StageDirectoryCandidate,
) {
	t.Helper()
	for _, candidate := range candidates {
		switch candidate.Status() {
		case StageDirectoryOwned, StageDirectoryEmptyUnmarked:
			continue
		default:
			if err := cas.RemoveScannedStage(candidate); !errors.Is(err, ErrCASInput) {
				t.Fatalf("diagnostic status %q removal error = %v",
					candidate.Status(), err)
			}
			if _, err := os.Lstat(filepath.Join(cas.staging, candidate.name)); err != nil {
				t.Fatalf("diagnostic status %q was changed: %v",
					candidate.Status(), err)
			}
		}
	}
	if err := cas.RemoveScannedStage(StageDirectoryCandidate{
		status: StageDirectoryChanged,
	}); !errors.Is(err, ErrCASInput) {
		t.Fatalf("changed diagnostic removal error = %v", err)
	}
}

func TestStageDirectoryCursorUsesProcessLocalDirectorySnapshots(t *testing.T) {
	t.Run("empty stream reaches done", func(t *testing.T) {
		cas := openTestCAS(t)
		var cursor StageDirectoryCursor
		for cycle := 0; cycle < 4; cycle++ {
			page, err := cas.ScanStageDirectories(cursor, 1)
			if err != nil || page.Examined() != 0 {
				t.Fatalf("empty stage directory page = (%#v,%v)", page, err)
			}
			if page.Done() {
				return
			}
			cursor = page.Next()
		}
		t.Fatal("empty stage directory stream did not reach done")
	})

	t.Run("invalid limits fail before scanning", func(t *testing.T) {
		cas := openTestCAS(t)
		for _, maximum := range []int{0, maxStageDirectoryScan + 1} {
			if _, err := cas.ScanStageDirectories(
				StageDirectoryCursor{}, maximum); !errors.Is(err, ErrCASInput) {
				t.Fatalf("stage directory limit %d error = %v", maximum, err)
			}
		}
	})

	t.Run("mtime invalidation restarts bounded pass", func(t *testing.T) {
		cas := openTestCAS(t)
		for index := uint64(1); index <= 2; index++ {
			makeUnmarkedStageDirectory(t, cas, testOperationStageOwner(
				t, "operation-stage-cursor-restart", index))
		}
		first, err := cas.ScanStageDirectories(StageDirectoryCursor{}, 1)
		if err != nil || first.Done() || first.Examined() != 1 {
			t.Fatalf("first bounded cursor page = (%#v,%v)", first, err)
		}
		cursor := first.Next()
		changed := cursor.directory.ModTime().Add(2 * time.Second)
		if err := os.Chtimes(cas.staging, changed, changed); err != nil {
			t.Fatal(err)
		}
		second, err := cas.ScanStageDirectories(cursor, 1)
		if err != nil || second.Done() || second.Examined() != 1 {
			t.Fatalf("restarted bounded cursor page = (%#v,%v)", second, err)
		}
		if second.next.directory == nil ||
			!second.next.directory.ModTime().Equal(changed) {
			t.Fatalf("restarted cursor snapshot = %#v, want mtime %s",
				second.next.directory, changed)
		}
	})

	t.Run("identity replacement fails closed", func(t *testing.T) {
		cas := openTestCAS(t)
		makeUnmarkedStageDirectory(t, cas, testInboxStageOwner(
			t, "inbox-stage-cursor-replace", 1))
		first, err := cas.ScanStageDirectories(StageDirectoryCursor{}, 1)
		if err != nil || first.Done() {
			t.Fatalf("first identity cursor page = (%#v,%v)", first, err)
		}
		replaced := cas.staging + ".replaced"
		if err := os.Rename(cas.staging, replaced); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(cas.staging, casDirectoryMode); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.ScanStageDirectories(first.Next(), 1); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("replaced staging directory cursor error = %v", err)
		}
	})

	t.Run("opened stream must match expected snapshot", func(t *testing.T) {
		cas := openTestCAS(t)
		expected, err := os.Lstat(cas.staging)
		if err != nil {
			t.Fatal(err)
		}
		replaced := cas.staging + ".replaced"
		if err := os.Rename(cas.staging, replaced); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(cas.staging, casDirectoryMode); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := readStageDirectoryBatchAt(cas.staging, expected, 0); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("replacement directory stream error = %v", err)
		}
	})
}

func TestRemoveScannedStageRevalidatesExactSnapshots(t *testing.T) {
	t.Run("owned marker mutation", func(t *testing.T) {
		cas := openTestCAS(t)
		owner := testOperationStageOwner(t, "operation-stage-scan-remove", 1)
		stage, _ := cas.OpenStage(owner)
		content := []byte("remove scanned owned stage")
		if _, err := stage.Put(model.Sum(content), content); err != nil {
			t.Fatal(err)
		}
		candidate := onlyStageDirectoryCandidate(t, cas)
		if candidate.Status() != StageDirectoryOwned {
			t.Fatalf("owned candidate status = %q", candidate.Status())
		}
		markerPath := filepath.Join(stage.path, stageOwnerMarkerName)
		marker, err := os.ReadFile(markerPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(markerPath, time.Now().Add(time.Second),
			time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := cas.RemoveScannedStage(candidate); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("changed marker removal error = %v", err)
		}
		if _, err := os.Lstat(stage.path); err != nil {
			t.Fatalf("changed owned stage was removed: %v", err)
		}
		if err := os.WriteFile(markerPath, marker, casObjectMode); err != nil {
			t.Fatal(err)
		}
		candidate = onlyStageDirectoryCandidate(t, cas)
		if err := cas.RemoveScannedStage(candidate); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(stage.path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("exact owned stage remains: %v", err)
		}
	})

	t.Run("empty unmarked becomes nonempty", func(t *testing.T) {
		cas := openTestCAS(t)
		owner := testInboxStageOwner(t, "inbox-stage-scan-empty-race", 1)
		path := makeUnmarkedStageDirectory(t, cas, owner)
		candidate := onlyStageDirectoryCandidate(t, cas)
		if candidate.Status() != StageDirectoryEmptyUnmarked {
			t.Fatalf("empty candidate status = %q", candidate.Status())
		}
		unexpected := filepath.Join(path, "unexpected")
		if err := os.WriteFile(unexpected, nil, casObjectMode); err != nil {
			t.Fatal(err)
		}
		if err := cas.RemoveScannedStage(candidate); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("changed empty stage removal error = %v", err)
		}
		if err := os.Remove(unexpected); err != nil {
			t.Fatal(err)
		}
		candidate = onlyStageDirectoryCandidate(t, cas)
		if err := cas.RemoveScannedStage(candidate); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("exact empty unmarked stage remains: %v", err)
		}
	})
}

func TestRemoveStageReplaysCrashAfterMarkerRemoval(t *testing.T) {
	cas := openTestCAS(t)
	owner := testOperationStageOwner(t, "operation-stage-marker-remove-replay", 1)
	stage, _ := cas.OpenStage(owner)
	content := []byte("cleanup response-loss bytes")
	digest := model.Sum(content)
	if _, err := stage.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(stage.path, stagedObjectName(digest))); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(stage.path, stageOwnerMarkerName)); err != nil {
		t.Fatal(err)
	}
	if err := cas.RemoveStage(owner); err != nil {
		t.Fatalf("empty missing-marker RemoveStage replay = %v", err)
	}
	if _, err := os.Lstat(stage.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replayed removed stage remains: %v", err)
	}
}

func makeUnmarkedStageDirectory(t *testing.T, cas *CAS, owner StageOwner) string {
	t.Helper()
	name, err := owner.directoryName()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cas.staging, name)
	if err := os.Mkdir(path, casDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return path
}

func scanAllStageDirectories(t *testing.T, cas *CAS,
	maximum int,
) []StageDirectoryCandidate {
	t.Helper()
	var cursor StageDirectoryCursor
	var candidates []StageDirectoryCandidate
	for cycle := 0; cycle < 64; cycle++ {
		page, err := cas.ScanStageDirectories(cursor, maximum)
		if err != nil {
			t.Fatal(err)
		}
		if page.Examined() < 0 || page.Examined() > maximum ||
			len(page.Candidates()) != page.Examined() {
			t.Fatalf("unbounded stage directory page = %#v", page)
		}
		candidates = append(candidates, page.Candidates()...)
		if page.Done() {
			return candidates
		}
		cursor = page.Next()
	}
	t.Fatal("stage directory scan did not reach end")
	return nil
}

func onlyStageDirectoryCandidate(t *testing.T,
	cas *CAS,
) StageDirectoryCandidate {
	t.Helper()
	candidates := scanAllStageDirectories(t, cas, 8)
	if len(candidates) != 1 {
		t.Fatalf("stage directory candidate count = %d", len(candidates))
	}
	return candidates[0]
}
