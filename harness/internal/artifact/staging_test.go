package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestStageOwnerHashSeparatesKindAndGeneration(t *testing.T) {
	operationID, _ := model.ParseOperationID("shared-canonical-owner")
	inboxID, _ := model.ParseInboxID(operationID.String())
	operation, err := NewOperationStageOwner(operationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := NewInboxStageOwner(inboxID, 1)
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewOperationStageOwner(operationID, 2)
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, owner := range []StageOwner{operation, inbox, next} {
		name, err := owner.directoryName()
		if err != nil {
			t.Fatal(err)
		}
		if len(name) != 64 || strings.ToLower(name) != name ||
			strings.Contains(name, owner.CanonicalID()) || names[name] {
			t.Fatalf("unsafe or colliding owner directory name %q", name)
		}
		names[name] = true
	}
	if operation.Kind() != StageOwnerOperation ||
		operation.CanonicalID() != operationID.String() || operation.Generation() != 1 {
		t.Fatalf("operation owner getters = %#v", operation)
	}
	if _, err := NewOperationStageOwner(operationID, 0); !errors.Is(err, ErrCASInput) {
		t.Fatalf("zero generation error = %v", err)
	}
	if _, err := NewInboxStageOwner(model.InboxID{}, 1); !errors.Is(err, ErrCASInput) {
		t.Fatalf("zero Inbox error = %v", err)
	}
}

func TestStagePutReadReplayNeverCreatesFinalAuthority(t *testing.T) {
	cas := openTestCAS(t)
	owner := testOperationStageOwner(t, "operation-stage-put", 1)
	stage, err := cas.OpenStage(owner)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("owner-scoped staged bytes")
	digest := model.Sum(content)
	first, err := stage.Put(digest, content)
	if err != nil || first.Replayed || first.Digest != digest {
		t.Fatalf("first staged Put = (%#v, %v)", first, err)
	}
	replay, err := stage.Put(digest, append([]byte{}, content...))
	if err != nil || !replay.Replayed {
		t.Fatalf("staged Put replay = (%#v, %v)", replay, err)
	}
	got, err := stage.Read(digest, len(content))
	if err != nil || string(got) != string(content) {
		t.Fatalf("staged Read = (%q, %v)", got, err)
	}
	if _, err := cas.Read(digest, len(content)); err == nil {
		t.Fatal("Stage.Put created final CAS authority")
	}
	objectPath := filepath.Join(stage.path, stagedObjectName(digest))
	for _, check := range []struct {
		path string
		mode os.FileMode
	}{{cas.staging, casDirectoryMode}, {stage.path, casDirectoryMode},
		{objectPath, casObjectMode}} {
		info, err := os.Lstat(check.path)
		if err != nil || info.Mode().Perm() != check.mode {
			t.Fatalf("stage mode %s = (%v, %v), want %04o",
				check.path, info, err, check.mode)
		}
	}
}

func TestStageReadAvailablePrefersStageAndFallsBackOnlyWhenAbsent(t *testing.T) {
	cas := openTestCAS(t)
	stage, err := cas.OpenStage(testOperationStageOwner(t, "operation-stage-available", 1))
	if err != nil {
		t.Fatal(err)
	}
	final := []byte("published fallback")
	finalDigest := model.Sum(final)
	if _, err := cas.Put(finalDigest, final); err != nil {
		t.Fatal(err)
	}
	if got, err := stage.ReadAvailable(finalDigest, len(final)); err != nil ||
		string(got) != string(final) {
		t.Fatalf("final fallback = (%q, %v)", got, err)
	}

	staged := []byte("owner-local preferred")
	stagedDigest := model.Sum(staged)
	if _, err := cas.Put(stagedDigest, staged); err != nil {
		t.Fatal(err)
	}
	if _, err := stage.Put(stagedDigest, staged); err != nil {
		t.Fatal(err)
	}
	stagedPath := filepath.Join(stage.path, stagedObjectName(stagedDigest))
	corrupt := append([]byte(nil), staged...)
	corrupt[0] ^= 0xff
	if err := os.WriteFile(stagedPath, corrupt, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if _, err := stage.ReadAvailable(stagedDigest, len(staged)); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("valid final hid corrupt staged object: %v", err)
	}
}

func TestStageAggregateByteBudgetIncludesExistingLayoutAndRejectsOverflow(t *testing.T) {
	if MaxStagingBytes != 320<<20 {
		t.Fatalf("MaxStagingBytes = %d, want %d", MaxStagingBytes, 320<<20)
	}
	cas := openTestCAS(t)
	stage, err := cas.OpenStage(testOperationStageOwner(t, "operation-stage-budget", 1))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("existing owner staging bytes")
	if _, err := stage.Put(model.Sum(content), content); err != nil {
		t.Fatal(err)
	}
	cas.coordination.staging.Lock()
	layout, err := stage.scanLocked(false)
	cas.coordination.staging.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if layout.bytes != uint64(len(content)) {
		t.Fatalf("existing layout bytes = %d, want %d", layout.bytes, len(content))
	}
	if total, err := boundedStageBytes(layout.bytes,
		uint64(MaxStagingBytes)-layout.bytes); err != nil || total != MaxStagingBytes {
		t.Fatalf("exact aggregate bound = (%d, %v)", total, err)
	}
	for _, current := range []uint64{uint64(MaxStagingBytes), ^uint64(0)} {
		if _, err := boundedStageBytes(current, 1); !errors.Is(err, ErrArtifactLimit) {
			t.Fatalf("overflowing aggregate %d error = %v", current, err)
		}
	}
}

func TestStagePublishUsesMixedClosureAndRemovesOnlyExactGeneration(t *testing.T) {
	fixture := newStageClosureFixture(t)
	cas := openTestCAS(t)
	owner := testOperationStageOwner(t, "operation-stage-publish", 4)
	nextOwner := testOperationStageOwner(t, "operation-stage-publish", 5)
	stage, _ := cas.OpenStage(owner)
	nextStage, _ := cas.OpenStage(nextOwner)
	extra := []byte("unrelated next-generation bytes")
	extraDigest := model.Sum(extra)
	skip := putMixedStageFixture(t, cas, stage, nextStage, fixture, extra)
	if err := stage.VerifyClosure(context.Background(), fixture.closure); err != nil {
		t.Fatalf("mixed VerifyClosure() error = %v", err)
	}
	if _, err := stage.Read(skip, BlockSize); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stage.Read unexpectedly fell back to final = %v", err)
	}
	if err := stage.Publish(context.Background(), fixture.closure); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	assertPublishedStageFixture(t, cas, stage, nextStage, fixture, extraDigest, extra)
	if err := stage.Publish(context.Background(), fixture.closure); err != nil {
		t.Fatalf("idempotent Publish replay error = %v", err)
	}
	if err := cas.RemoveStage(owner); err != nil {
		t.Fatalf("idempotent RemoveStage error = %v", err)
	}
	if err := cas.RemoveStage(nextOwner); err != nil {
		t.Fatalf("RemoveStage(next generation) error = %v", err)
	}
}

func putMixedStageFixture(t testing.TB, cas *CAS, stage, nextStage *Stage,
	fixture stageClosureFixture, extra []byte,
) model.Digest {
	t.Helper()
	if _, err := nextStage.Put(model.Sum(extra), extra); err != nil {
		t.Fatal(err)
	}
	skip := fixture.closure.Blocks()[0].Digest
	if _, err := cas.Put(skip, fixture.objects[skip]); err != nil {
		t.Fatal(err)
	}
	for digest, content := range fixture.objects {
		if digest != skip {
			if _, err := stage.Put(digest, content); err != nil {
				t.Fatal(err)
			}
		}
	}
	ignored := []byte("valid owner-local retry debris")
	if _, err := stage.Put(model.Sum(ignored), ignored); err != nil {
		t.Fatal(err)
	}
	return skip
}

func assertPublishedStageFixture(t testing.TB, cas *CAS, stage, nextStage *Stage,
	fixture stageClosureFixture, extraDigest model.Digest, extra []byte,
) {
	t.Helper()
	if err := cas.VerifyClosure(context.Background(), fixture.closure); err != nil {
		t.Fatalf("final VerifyClosure() error = %v", err)
	}
	if _, err := os.Lstat(stage.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published owner stage remains: %v", err)
	}
	if got, err := nextStage.Read(extraDigest, len(extra)); err != nil ||
		string(got) != string(extra) {
		t.Fatalf("Publish removed another generation = (%q, %v)", got, err)
	}
}

func TestStagePublishRejectsCorruptFinalAndStagedCopy(t *testing.T) {
	fixture := newStageClosureFixture(t)
	cas := openTestCAS(t)
	owner := testInboxStageOwner(t, "inbox-stage-conflict", 1)
	stage, _ := cas.OpenStage(owner)
	for digest, content := range fixture.objects {
		if _, err := stage.Put(digest, content); err != nil {
			t.Fatal(err)
		}
	}
	digests := sortedStageFixtureDigests(fixture.objects)
	conflict := digests[0]
	path, err := cas.objectPath(conflict, true)
	if err != nil {
		t.Fatal(err)
	}
	forged := append([]byte{}, fixture.objects[conflict]...)
	forged[0] ^= 0xff
	if err := os.WriteFile(path, forged, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := stage.Publish(context.Background(), fixture.closure); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("Publish accepted corrupt existing final = %v", err)
	}
	if _, err := os.Lstat(stage.path); err != nil {
		t.Fatalf("failed Publish discarded recovery stage: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := stage.Publish(context.Background(), fixture.closure); err != nil {
		t.Fatalf("Publish recovery error = %v", err)
	}

	shadowOwner := testInboxStageOwner(t, "inbox-stage-conflict", 2)
	shadow, _ := cas.OpenStage(shadowOwner)
	content := fixture.objects[conflict]
	if _, err := shadow.Put(conflict, content); err != nil {
		t.Fatal(err)
	}
	stagedPath := filepath.Join(shadow.path, stagedObjectName(conflict))
	tampered := append([]byte{}, content...)
	tampered[0] ^= 0x55
	if err := os.WriteFile(stagedPath, tampered, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := shadow.VerifyClosure(context.Background(), fixture.closure); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("valid final hid corrupt staged copy = %v", err)
	}
}

func TestRemoveStageFailsClosedOnUnknownOrUnsafeEntries(t *testing.T) {
	cas := openTestCAS(t)
	owner := testOperationStageOwner(t, "operation-stage-remove", 1)
	stage, _ := cas.OpenStage(owner)
	content := []byte("remove exact owner stage")
	digest := model.Sum(content)
	if _, err := stage.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(stage.path, "local-debug.json")
	if err := os.WriteFile(unknown, []byte("{}"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := cas.RemoveStage(owner); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("RemoveStage accepted unknown entry = %v", err)
	}
	if _, err := os.Lstat(unknown); err != nil {
		t.Fatalf("fail-closed removal changed unknown entry: %v", err)
	}
	if err := os.Remove(unknown); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(stage.path, stagedObjectName(digest))
	if err := os.Chmod(objectPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cas.RemoveStage(owner); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("RemoveStage accepted unsafe mode = %v", err)
	}
	if err := os.Chmod(objectPath, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := cas.RemoveStage(owner); err != nil {
		t.Fatalf("valid RemoveStage error = %v", err)
	}
	if _, err := os.Lstat(stage.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("valid stage remains: %v", err)
	}
}

func TestRemoveStageRevalidatesExactEmptyDirectoryBeforeDeletion(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "replacement",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Rename(path, path+".replaced"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, casDirectoryMode); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Remove(filepath.Join(path, stageOwnerMarkerName)); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "mode",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "nonempty",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(path, "unexpected"), nil,
					casObjectMode); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cas := openTestCAS(t)
			stage, err := cas.OpenStage(testOperationStageOwner(t,
				"operation-stage-directory-"+test.name, 1))
			if err != nil {
				t.Fatal(err)
			}
			cas.coordination.staging.Lock()
			layout, err := stage.scanLocked(true)
			if err != nil {
				cas.coordination.staging.Unlock()
				t.Fatal(err)
			}
			test.mutate(t, stage.path)
			err = stage.removeLayoutLocked(layout)
			cas.coordination.staging.Unlock()
			if !errors.Is(err, ErrCASCorruption) {
				t.Fatalf("unsafe directory removal error = %v", err)
			}
			if _, err := os.Lstat(stage.path); err != nil {
				t.Fatalf("unsafe directory was removed: %v", err)
			}
		})
	}
}

func TestStageRecoversValidTempAndDropsInvalidPartial(t *testing.T) {
	cas := openTestCAS(t)
	owner := testInboxStageOwner(t, "inbox-stage-temp-recovery", 1)
	stage, _ := cas.OpenStage(owner)
	if _, _, err := stage.directory(true); err != nil {
		t.Fatal(err)
	}
	content := []byte("restartable staged transfer")
	digest := model.Sum(content)
	invalid := filepath.Join(stage.path,
		fmt.Sprintf("put-%s-%032x.tmp", stagedObjectName(digest), 1))
	if err := os.WriteFile(invalid, []byte("partial"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if _, err := stage.Read(digest, len(content)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid partial became authority = %v", err)
	}
	if _, err := os.Lstat(invalid); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid partial remains: %v", err)
	}
	valid := filepath.Join(stage.path,
		fmt.Sprintf("put-%s-%032x.tmp", stagedObjectName(digest), 2))
	if err := os.WriteFile(valid, content, casObjectMode); err != nil {
		t.Fatal(err)
	}
	got, err := stage.Read(digest, len(content))
	if err != nil || string(got) != string(content) {
		t.Fatalf("recovered staged temp = (%q, %v)", got, err)
	}
	if _, err := os.Lstat(valid); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered temp name remains: %v", err)
	}
}

type stageClosureFixture struct {
	closure Closure
	objects map[model.Digest][]byte
}

func newStageClosureFixture(t *testing.T) stageClosureFixture {
	t.Helper()
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "left.txt"),
		[]byte("left staged closure bytes"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "right.txt"),
		[]byte("right staged closure bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := openTestCAS(t)
	capturer, err := NewCapturer(workspace, func() time.Time {
		return time.Date(2026, 7, 26, 16, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(context.Background(),
		[]string{"left.txt", "right.txt"}, source)
	if err != nil {
		t.Fatal(err)
	}
	objects := make(map[model.Digest][]byte)
	for _, root := range closure.Roots() {
		objects[root.ManifestDigest] = root.Manifest.Bytes()
	}
	for _, block := range closure.Blocks() {
		content, err := source.Read(block.Digest, BlockSize)
		if err != nil {
			t.Fatal(err)
		}
		objects[block.Digest] = content
	}
	return stageClosureFixture{closure: closure, objects: objects}
}

func sortedStageFixtureDigests(objects map[model.Digest][]byte) []model.Digest {
	result := make([]model.Digest, 0, len(objects))
	for digest := range objects {
		result = append(result, digest)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].String() < result[right].String()
	})
	return result
}

func testOperationStageOwner(t *testing.T, value string, generation uint64) StageOwner {
	t.Helper()
	id, err := model.ParseOperationID(value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewOperationStageOwner(id, generation)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func testInboxStageOwner(t *testing.T, value string, generation uint64) StageOwner {
	t.Helper()
	id, err := model.ParseInboxID(value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewInboxStageOwner(id, generation)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}
