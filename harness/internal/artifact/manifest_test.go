package artifact

import (
	"errors"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestManifestCanonicalRoundTripAndDomainSeparatedRoot(t *testing.T) {
	first := model.Sum([]byte("first block"))
	second := model.Sum([]byte("second block"))
	manifest, err := NewManifest(ManifestSpec{RootKind: EntryDirectory, RootPath: "bundle", Entries: []ManifestEntry{
		{Kind: EntryFile, LogicalPath: "bundle/result.txt", Mode: 0o640, SizeBytes: BlockSize + 5,
			Blocks: []ManifestBlock{{Digest: first, LengthBytes: BlockSize},
				{Digest: second, OffsetBytes: BlockSize, LengthBytes: 5}}},
		{Kind: EntryDirectory, LogicalPath: "bundle", Mode: 0o750},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.TotalBytes() != BlockSize+5 || manifest.ManifestDigest() != model.Sum(manifest.CanonicalJSON().Bytes()) ||
		manifest.RootDigest() == manifest.ManifestDigest() {
		t.Fatalf("manifest digest/size = total %d manifest %s root %s", manifest.TotalBytes(),
			manifest.ManifestDigest().String(), manifest.RootDigest().String())
	}
	rootMaterial := append([]byte("mnemon/artifact-root/v1\x00"), manifest.CanonicalJSON().Bytes()...)
	if manifest.RootDigest() != model.Sum(rootMaterial) {
		t.Fatalf("root digest does not use the frozen v1 domain formula: %s", manifest.RootDigest())
	}
	entries := manifest.Entries()
	if len(entries) != 2 || entries[0].LogicalPath != "bundle" || entries[1].LogicalPath != "bundle/result.txt" ||
		entries[0].Blocks == nil || entries[1].Blocks[1].OffsetBytes != BlockSize {
		t.Fatalf("canonical entries = %#v", entries)
	}
	entries[1].Blocks[0].LengthBytes = 1
	if manifest.Entries()[1].Blocks[0].LengthBytes != BlockSize {
		t.Fatal("Manifest Entries getter mutated block map")
	}
	parsed, err := ParseManifest(manifest.CanonicalJSON().Bytes())
	if err != nil || parsed.RootDigest() != manifest.RootDigest() ||
		parsed.CanonicalJSON().String() != manifest.CanonicalJSON().String() {
		t.Fatalf("ParseManifest() = (%#v, %v)", parsed, err)
	}
}

func TestParseManifestRejectsNoncanonicalUnknownAndDriftedFields(t *testing.T) {
	manifest := manifestTestFile(t, "result.txt", []byte("hello"))
	raw := manifest.CanonicalJSON().String()
	tests := []struct {
		name string
		raw  string
	}{
		{"whitespace", " " + raw},
		{"unknown field", strings.Replace(raw, "{", `{"unknown":true,`, 1)},
		{"total drift", strings.Replace(raw, `"total_bytes":5`, `"total_bytes":6`, 1)},
		{"schema drift", strings.Replace(raw, `"schema_version":1`, `"schema_version":2`, 1)},
		{"invalid block digest", strings.Replace(raw,
			manifest.Entries()[0].Blocks[0].Digest.String(), "sha256:ABC", 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(test.raw)); err == nil {
				t.Fatalf("ParseManifest accepted %s", test.raw)
			}
		})
	}

	reversed, err := model.JSONFrom(manifestWire{SchemaVersion: SchemaVersion,
		RootKind: EntryDirectory, RootPath: "dir", TotalBytes: 1, Entries: []ManifestEntry{
			{Kind: EntryFile, LogicalPath: "dir/z", Mode: 0o600, SizeBytes: 1,
				Blocks: []ManifestBlock{{Digest: model.Sum([]byte("z")), LengthBytes: 1}}},
			{Kind: EntryDirectory, LogicalPath: "dir", Mode: 0o700, Blocks: []ManifestBlock{}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseManifest(reversed.Bytes()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("unordered manifest error = %v", err)
	}
}

func TestManifestRejectsInvalidTreePathsAndBlockMaps(t *testing.T) {
	digest := model.Sum([]byte("block"))
	tests := []struct {
		name string
		spec ManifestSpec
		err  error
	}{
		{"absolute root", ManifestSpec{RootKind: EntryFile, RootPath: "/tmp/file",
			Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: "/tmp/file"}}}, ErrArtifactPath},
		{"internal root", ManifestSpec{RootKind: EntryDirectory, RootPath: ".mnemon/harness",
			Entries: []ManifestEntry{{Kind: EntryDirectory, LogicalPath: ".mnemon/harness"}}}, ErrArtifactPath},
		{"missing root", ManifestSpec{RootKind: EntryDirectory, RootPath: "dir",
			Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: "dir/file", SizeBytes: 1,
				Blocks: []ManifestBlock{{Digest: digest, LengthBytes: 1}}}}}, ErrInvalidManifest},
		{"missing parent", ManifestSpec{RootKind: EntryDirectory, RootPath: "dir",
			Entries: []ManifestEntry{{Kind: EntryDirectory, LogicalPath: "dir"},
				{Kind: EntryFile, LogicalPath: "dir/sub/file", SizeBytes: 1,
					Blocks: []ManifestBlock{{Digest: digest, LengthBytes: 1}}}}}, ErrInvalidManifest},
		{"directory content", ManifestSpec{RootKind: EntryDirectory, RootPath: "dir",
			Entries: []ManifestEntry{{Kind: EntryDirectory, LogicalPath: "dir", SizeBytes: 1}}}, ErrInvalidManifest},
		{"block gap", ManifestSpec{RootKind: EntryFile, RootPath: "file",
			Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: "file", SizeBytes: 1,
				Blocks: []ManifestBlock{{Digest: digest, OffsetBytes: 1, LengthBytes: 1}}}}}, ErrInvalidManifest},
		{"short non-final", ManifestSpec{RootKind: EntryFile, RootPath: "file",
			Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: "file", SizeBytes: 2,
				Blocks: []ManifestBlock{{Digest: digest, LengthBytes: 1},
					{Digest: digest, OffsetBytes: 1, LengthBytes: 1}}}}}, ErrInvalidManifest},
		{"conflicting digest lengths", ManifestSpec{RootKind: EntryDirectory, RootPath: "dir",
			Entries: []ManifestEntry{{Kind: EntryDirectory, LogicalPath: "dir"},
				{Kind: EntryFile, LogicalPath: "dir/one", SizeBytes: 1,
					Blocks: []ManifestBlock{{Digest: digest, LengthBytes: 1}}},
				{Kind: EntryFile, LogicalPath: "dir/two", SizeBytes: 2,
					Blocks: []ManifestBlock{{Digest: digest, LengthBytes: 2}}}}}, ErrInvalidManifest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewManifest(test.spec); !errors.Is(err, test.err) {
				t.Fatalf("NewManifest() error = %v, want %v", err, test.err)
			}
		})
	}

	tooMany := make([]ManifestEntry, MaxEntries+1)
	for index := range tooMany {
		tooMany[index] = ManifestEntry{Kind: EntryDirectory,
			LogicalPath: "root/" + strings.Repeat("a", index%20+1)}
	}
	if _, err := NewManifest(ManifestSpec{RootKind: EntryDirectory, RootPath: "root",
		Entries: tooMany}); !errors.Is(err, ErrArtifactLimit) {
		t.Fatalf("entry limit error = %v", err)
	}
}

func manifestTestFile(t *testing.T, logical string, content []byte) Manifest {
	t.Helper()
	blocks := make([]ManifestBlock, 0)
	if len(content) > 0 {
		blocks = append(blocks, ManifestBlock{Digest: model.Sum(content), LengthBytes: uint64(len(content))})
	}
	manifest, err := NewManifest(ManifestSpec{RootKind: EntryFile, RootPath: logical,
		Entries: []ManifestEntry{{Kind: EntryFile, LogicalPath: logical, Mode: 0o600,
			SizeBytes: uint64(len(content)), Blocks: blocks}}})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}
