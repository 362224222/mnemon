package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestBuildImportedClosureBuildsVerifiableCanonicalShape(t *testing.T) {
	shared := []byte("one immutable block shared by three files")
	sharedDigest := model.Sum(shared)
	bundle := importTestManifest(t, ManifestSpec{RootKind: EntryDirectory, RootPath: "bundle",
		Entries: []ManifestEntry{
			{Kind: EntryDirectory, LogicalPath: "bundle", Mode: 0o750, Blocks: []ManifestBlock{}},
			{Kind: EntryFile, LogicalPath: "bundle/empty.txt", Mode: 0o600, Blocks: []ManifestBlock{}},
			{Kind: EntryFile, LogicalPath: "bundle/first.bin", Mode: 0o640,
				SizeBytes: uint64(len(shared)), Blocks: []ManifestBlock{{Digest: sharedDigest,
					LengthBytes: uint64(len(shared)), OffsetBytes: 0}}},
			{Kind: EntryFile, LogicalPath: "bundle/second.bin", Mode: 0o600,
				SizeBytes: uint64(len(shared)), Blocks: []ManifestBlock{{Digest: sharedDigest,
					LengthBytes: uint64(len(shared)), OffsetBytes: 0}}},
		}})
	single := importTestFileManifest(t, "single.bin", 0o644, sharedDigest, uint64(len(shared)))
	emptyDirectory := importTestManifest(t, ManifestSpec{RootKind: EntryDirectory, RootPath: "empty-dir",
		Entries: []ManifestEntry{{Kind: EntryDirectory, LogicalPath: "empty-dir", Mode: 0o700,
			Blocks: []ManifestBlock{}}}})
	emptyFile := importTestFileManifest(t, "empty.txt", 0o600, model.Digest{}, 0)
	inputs := []Manifest{single, emptyFile, bundle, emptyDirectory}

	cas := importTestCAS(t)
	if info, err := os.Stat(cas.Root()); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("CAS root mode = %v, error = %v", info.Mode().Perm(), err)
	}
	if _, err := cas.Put(sharedDigest, shared); err != nil {
		t.Fatal(err)
	}
	for _, manifest := range inputs {
		if _, err := cas.Put(manifest.ManifestDigest(), manifest.CanonicalJSON().Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	verifiedAt := time.Date(2026, 7, 19, 8, 9, 10, 123, time.UTC)
	closure, err := BuildImportedClosure(context.Background(), inputs, verifiedAt)
	if err != nil {
		t.Fatal(err)
	}
	if closure.IsZero() || closure.CapturedAt() != verifiedAt || len(closure.Roots()) != 4 ||
		len(closure.Blocks()) != 1 || len(closure.BlockMap()) != 3 {
		t.Fatalf("closure = roots %d blocks %d map %d at %s checkpoint %s",
			len(closure.Roots()), len(closure.Blocks()), len(closure.BlockMap()),
			closure.CapturedAt(), closure.Checkpoint().String())
	}
	if err := cas.VerifyClosure(context.Background(), closure); err != nil {
		t.Fatalf("VerifyClosure() error = %v", err)
	}

	roots := closure.Roots()
	for index, root := range roots {
		if root.CreatedAt != verifiedAt || root.VerifiedAt != verifiedAt ||
			(index > 0 && roots[index-1].RootDigest.String() >= root.RootDigest.String()) {
			t.Fatalf("root %d is not canonical: %#v", index, root)
		}
	}
	blocks := closure.Blocks()
	if blocks[0].Digest != sharedDigest || blocks[0].SizeBytes != uint64(len(shared)) ||
		blocks[0].CreatedAt != verifiedAt {
		t.Fatalf("unique shared block = %#v", blocks[0])
	}
	blockMap := closure.BlockMap()
	lastRoot := model.Digest{}
	var nextOrdinal uint64
	for index, row := range blockMap {
		if index == 0 || row.RootDigest != lastRoot {
			if index > 0 && lastRoot.String() >= row.RootDigest.String() {
				t.Fatalf("root block map is not root sorted: %#v", blockMap)
			}
			nextOrdinal = 0
			lastRoot = row.RootDigest
		}
		if row.Ordinal != nextOrdinal || row.BlockDigest != sharedDigest {
			t.Fatalf("root block row %d = %#v, want ordinal %d", index, row, nextOrdinal)
		}
		nextOrdinal++
	}
	var checkpoint struct {
		Roots []struct {
			ManifestDigest string `json:"manifest_digest"`
			RootDigest     string `json:"root_digest"`
		} `json:"roots"`
	}
	if err := json.Unmarshal(closure.Checkpoint().Bytes(), &checkpoint); err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Roots) != len(roots) {
		t.Fatalf("checkpoint roots = %#v", checkpoint.Roots)
	}
	for index, row := range checkpoint.Roots {
		if row.RootDigest != roots[index].RootDigest.String() ||
			row.ManifestDigest != roots[index].ManifestDigest.String() {
			t.Fatalf("checkpoint root %d = %#v", index, row)
		}
	}

	inputs[0] = Manifest{}
	roots[0].TotalBytes++
	blocks[0].SizeBytes++
	blockMap[0].LengthBytes++
	checkpointBytes := closure.Checkpoint().Bytes()
	checkpointBytes[0] = '!'
	if closure.Roots()[0].TotalBytes == roots[0].TotalBytes ||
		closure.Blocks()[0].SizeBytes == blocks[0].SizeBytes ||
		closure.BlockMap()[0].LengthBytes == blockMap[0].LengthBytes ||
		closure.Checkpoint().Bytes()[0] == '!' {
		t.Fatal("import closure did not defensively own its typed projections")
	}
	if err := cas.VerifyClosure(context.Background(), closure); err != nil {
		t.Fatalf("input/getter mutation changed closure: %v", err)
	}
}

func TestBuildImportedClosureMatchesEquivalentLocalClosure(t *testing.T) {
	workspace := t.TempDir()
	shared := []byte("same local and imported content")
	if err := os.WriteFile(filepath.Join(workspace, "left.bin"), shared, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "right.bin"), shared, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "empty.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "empty-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	cas := importTestCAS(t)
	verifiedAt := time.Date(2026, 7, 19, 9, 0, 0, 456, time.UTC)
	capturer, err := NewCapturer(workspace, func() time.Time { return verifiedAt })
	if err != nil {
		t.Fatal(err)
	}
	local, err := capturer.Capture(context.Background(),
		[]string{"left.bin", "empty-dir", "right.bin", "empty.txt"}, cas)
	if err != nil {
		t.Fatal(err)
	}
	manifests := make([]Manifest, 0, len(local.Roots()))
	for _, root := range local.Roots() {
		manifest, err := ParseManifest(root.Manifest.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, manifest)
	}
	for left, right := 0, len(manifests)-1; left < right; left, right = left+1, right-1 {
		manifests[left], manifests[right] = manifests[right], manifests[left]
	}
	imported, err := BuildImportedClosure(context.Background(), manifests, verifiedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !local.SameContent(imported) || !reflect.DeepEqual(local.Roots(), imported.Roots()) ||
		!reflect.DeepEqual(local.Blocks(), imported.Blocks()) ||
		!reflect.DeepEqual(local.BlockMap(), imported.BlockMap()) ||
		local.Checkpoint().String() != imported.Checkpoint().String() ||
		local.CapturedAt() != imported.CapturedAt() {
		t.Fatalf("imported closure differs from local typed shape\nlocal: %#v\nimported: %#v",
			local, imported)
	}
	if err := cas.VerifyClosure(context.Background(), imported); err != nil {
		t.Fatalf("VerifyClosure(imported) error = %v", err)
	}
}

func TestBuildImportedClosureRejectsTamperDuplicatesAndConflictingBlocks(t *testing.T) {
	content := []byte("content")
	digest := model.Sum(content)
	base := importTestFileManifest(t, "result.bin", 0o640, digest, uint64(len(content)))
	verifiedAt := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	tampered := []struct {
		name     string
		manifest Manifest
	}{
		{name: "zero", manifest: Manifest{}},
		{name: "root path", manifest: importTamperManifest(base, func(value *Manifest) {
			value.rootPath = "other.bin"
		})},
		{name: "root digest", manifest: importTamperManifest(base, func(value *Manifest) {
			value.rootDigest = model.Sum([]byte("forged root"))
		})},
		{name: "manifest digest", manifest: importTamperManifest(base, func(value *Manifest) {
			value.manifestDigest = model.Sum([]byte("forged manifest"))
		})},
		{name: "path", manifest: importTamperManifest(base, func(value *Manifest) {
			value.entries[0].LogicalPath = "../escape"
		})},
		{name: "mode", manifest: importTamperManifest(base, func(value *Manifest) {
			value.entries[0].Mode = 0o1000
		})},
		{name: "offset", manifest: importTamperManifest(base, func(value *Manifest) {
			value.entries[0].Blocks[0].OffsetBytes = 1
		})},
		{name: "block limit", manifest: importTamperManifest(base, func(value *Manifest) {
			value.entries[0].Blocks[0].LengthBytes = BlockSize + 1
		})},
	}
	for _, test := range tampered {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildImportedClosure(context.Background(), []Manifest{test.manifest},
				verifiedAt); !errors.Is(err, ErrClosureMismatch) {
				t.Fatalf("tampered manifest error = %v", err)
			}
		})
	}
	if _, err := BuildImportedClosure(context.Background(), []Manifest{base, base},
		verifiedAt); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("duplicate root error = %v", err)
	}

	first := importTestFileManifest(t, "same-path.bin", 0o600,
		model.Sum([]byte("version one")), uint64(len("version one")))
	second := importTestFileManifest(t, "same-path.bin", 0o600,
		model.Sum([]byte("version two")), uint64(len("version two")))
	if first.RootDigest() == second.RootDigest() {
		t.Fatal("same-path test roots unexpectedly match")
	}
	if closure, err := BuildImportedClosure(context.Background(), []Manifest{first, second},
		verifiedAt); err != nil || len(closure.Roots()) != 2 {
		t.Fatalf("distinct versions of one logical path = roots %d, error %v",
			len(closure.Roots()), err)
	}

	sharedDigest := model.Sum([]byte("shared digest identity"))
	short := importTestFileManifest(t, "short.bin", 0o600, sharedDigest, 1)
	long := importTestFileManifest(t, "long.bin", 0o600, sharedDigest, 2)
	if _, err := BuildImportedClosure(context.Background(), []Manifest{short, long},
		verifiedAt); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("conflicting shared block length error = %v", err)
	}

	cas := importTestCAS(t)
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Put(base.ManifestDigest(), base.CanonicalJSON().Bytes()); err != nil {
		t.Fatal(err)
	}
	closure, err := BuildImportedClosure(context.Background(), []Manifest{base}, verifiedAt)
	if err != nil {
		t.Fatal(err)
	}
	changed := closure
	changed.blockMap = append([]RootBlock(nil), closure.blockMap...)
	changed.blockMap[0].LengthBytes++
	if err := cas.VerifyClosure(context.Background(), changed); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("tampered closure verification error = %v", err)
	}
}

func TestBuildImportedClosureEnforcesAggregateAndTimeLimits(t *testing.T) {
	verifiedAt := time.Date(2026, 7, 19, 11, 0, 0, 0, time.UTC)
	base := importTestFileManifest(t, "base", 0o600, model.Digest{}, 0)
	if _, err := BuildImportedClosure(nil, []Manifest{base}, verifiedAt); !errors.Is(err, ErrClosureMismatch) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := BuildImportedClosure(context.Background(), nil, verifiedAt); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("empty root error = %v", err)
	}
	tooMany := make([]Manifest, MaxRoots+1)
	for index := range tooMany {
		tooMany[index] = base
	}
	if _, err := BuildImportedClosure(context.Background(), tooMany, verifiedAt); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("root count error = %v", err)
	}

	entryLimit := []Manifest{
		importTestEmptyTreeManifest(t, "tree-a", MaxEntries/2-1),
		importTestEmptyTreeManifest(t, "tree-b", MaxEntries/2-1),
	}
	if _, err := BuildImportedClosure(context.Background(), entryLimit, verifiedAt); err != nil {
		t.Fatalf("closed aggregate entry limit error = %v", err)
	}
	if _, err := BuildImportedClosure(context.Background(), append(entryLimit, base), verifiedAt); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("aggregate entry error = %v", err)
	}
	byteLimit := []Manifest{
		importTestVirtualFileManifest(t, "large-a", MaxTotalBytes/2, "large-a"),
		importTestVirtualFileManifest(t, "large-b", MaxTotalBytes/2, "large-b"),
	}
	if _, err := BuildImportedClosure(context.Background(), byteLimit, verifiedAt); err != nil {
		t.Fatalf("closed aggregate byte limit error = %v", err)
	}
	oneByte := importTestFileManifest(t, "one-byte", 0o600, model.Sum([]byte("one-byte")), 1)
	if _, err := BuildImportedClosure(context.Background(), append(byteLimit, oneByte), verifiedAt); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("aggregate byte error = %v", err)
	}

	noncanonicalTimes := []time.Time{
		{},
		verifiedAt.In(time.FixedZone("zero-but-not-UTC", 0)),
		time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, candidate := range noncanonicalTimes {
		if _, err := BuildImportedClosure(context.Background(), []Manifest{base}, candidate); !errors.Is(err, ErrClosureMismatch) {
			t.Fatalf("noncanonical time %v error = %v", candidate, err)
		}
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := BuildImportedClosure(cancelled, []Manifest{base}, verifiedAt); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled build error = %v", err)
	}
	cas := importTestCAS(t)
	if _, err := cas.Put(base.ManifestDigest(), base.CanonicalJSON().Bytes()); err != nil {
		t.Fatal(err)
	}
	closure, err := BuildImportedClosure(context.Background(), []Manifest{base}, verifiedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := cas.VerifyClosure(cancelled, closure); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled verification error = %v", err)
	}
}

func importTestCAS(t *testing.T) *CAS {
	t.Helper()
	cas, err := NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}

func importTestManifest(t *testing.T, spec ManifestSpec) Manifest {
	t.Helper()
	manifest, err := NewManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseManifest(manifest.CanonicalJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func importTestFileManifest(t *testing.T, logical string, mode uint32,
	digest model.Digest, length uint64,
) Manifest {
	t.Helper()
	blocks := make([]ManifestBlock, 0, 1)
	if length != 0 {
		blocks = append(blocks, ManifestBlock{Digest: digest, LengthBytes: length})
	}
	return importTestManifest(t, ManifestSpec{RootKind: EntryFile, RootPath: logical,
		Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: logical, Mode: mode,
			SizeBytes: length, Blocks: blocks}}})
}

func importTamperManifest(base Manifest, mutate func(*Manifest)) Manifest {
	result := base
	result.entries = cloneManifestEntries(base.entries)
	mutate(&result)
	return result
}

func importTestEmptyTreeManifest(t *testing.T, root string, emptyFiles int) Manifest {
	t.Helper()
	entries := make([]ManifestEntry, 0, emptyFiles+1)
	entries = append(entries, ManifestEntry{Kind: EntryDirectory, LogicalPath: root,
		Mode: 0o700, Blocks: []ManifestBlock{}})
	for index := 0; index < emptyFiles; index++ {
		entries = append(entries, ManifestEntry{Kind: EntryFile,
			LogicalPath: fmt.Sprintf("%s/file-%04d", root, index), Mode: 0o600,
			Blocks: []ManifestBlock{}})
	}
	return importTestManifest(t, ManifestSpec{RootKind: EntryDirectory,
		RootPath: root, Entries: entries})
}

func importTestVirtualFileManifest(t *testing.T, logical string, size uint64,
	seed string,
) Manifest {
	t.Helper()
	blocks := make([]ManifestBlock, 0, (size+BlockSize-1)/BlockSize)
	for offset, ordinal := uint64(0), uint64(0); offset < size; ordinal++ {
		length := uint64(BlockSize)
		if remaining := size - offset; remaining < length {
			length = remaining
		}
		blocks = append(blocks, ManifestBlock{Digest: model.Sum([]byte(fmt.Sprintf("%s-%d", seed, ordinal))),
			OffsetBytes: offset, LengthBytes: length})
		offset += length
	}
	return importTestManifest(t, ManifestSpec{RootKind: EntryFile, RootPath: logical,
		Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: logical, Mode: 0o600,
			SizeBytes: size, Blocks: blocks}}})
}
