package artifact

import (
	"bytes"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func FuzzParseManifest(f *testing.F) {
	valid := manifestFuzzSeeds(f)
	if _, err := ParseManifest(make([]byte, MaxManifestBytes+1)); !errors.Is(err, ErrArtifactLimit) {
		f.Fatalf("oversized manifest error = %v, want ErrArtifactLimit", err)
	}

	for _, raw := range [][]byte{
		nil,
		{},
		[]byte(`{}`),
		[]byte{0xff, 0x00, 0x01},
		append([]byte(" "), valid[0]...),
		append(append([]byte(nil), valid[0]...), '\n'),
	} {
		f.Add(raw)
	}
	for _, seed := range valid {
		for _, cut := range []int{1, len(seed) / 2, len(seed) - 1} {
			f.Add(append([]byte(nil), seed[:cut]...))
		}
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		manifest, err := ParseManifest(raw)
		if err != nil {
			if len(raw) > MaxManifestBytes && !errors.Is(err, ErrArtifactLimit) {
				t.Fatalf("oversized manifest error = %v, want ErrArtifactLimit", err)
			}
			return
		}
		if len(raw) == 0 || len(raw) > MaxManifestBytes {
			t.Fatalf("ParseManifest accepted %d bytes", len(raw))
		}
		if !bytes.Equal(manifest.CanonicalJSON().Bytes(), raw) {
			t.Fatal("parsed manifest did not retain the exact canonical wire")
		}
		if manifest.ManifestDigest() != model.Sum(raw) {
			t.Fatal("parsed manifest digest differs from its canonical wire")
		}

		rebuilt, err := NewManifest(ManifestSpec{
			RootKind: manifest.RootKind(),
			RootPath: manifest.RootPath(),
			Entries:  manifest.Entries(),
		})
		if err != nil {
			t.Fatalf("rebuild parsed manifest: %v", err)
		}
		if !bytes.Equal(rebuilt.CanonicalJSON().Bytes(), raw) ||
			rebuilt.ManifestDigest() != manifest.ManifestDigest() ||
			rebuilt.RootDigest() != manifest.RootDigest() ||
			rebuilt.TotalBytes() != manifest.TotalBytes() {
			t.Fatal("manifest did not round-trip through NewManifest")
		}

		roundTrip, err := ParseManifest(rebuilt.CanonicalJSON().Bytes())
		if err != nil {
			t.Fatalf("parse rebuilt manifest: %v", err)
		}
		if roundTrip.RootKind() != manifest.RootKind() ||
			roundTrip.RootPath() != manifest.RootPath() ||
			roundTrip.TotalBytes() != manifest.TotalBytes() ||
			roundTrip.ManifestDigest() != manifest.ManifestDigest() ||
			roundTrip.RootDigest() != manifest.RootDigest() {
			t.Fatal("manifest round-trip changed canonical identity")
		}
	})
}

func manifestFuzzSeeds(f *testing.F) [][]byte {
	f.Helper()
	empty, err := NewManifest(ManifestSpec{
		RootKind: EntryFile,
		RootPath: "empty.txt",
		Entries: []ManifestEntry{{
			Kind: EntryFile, LogicalPath: "empty.txt", Mode: 0o600,
		}},
	})
	if err != nil {
		f.Fatal(err)
	}
	content := []byte("manifest fuzz seed")
	file, err := NewManifest(ManifestSpec{
		RootKind: EntryFile,
		RootPath: "result.txt",
		Entries: []ManifestEntry{{
			Kind: EntryFile, LogicalPath: "result.txt", Mode: 0o640,
			SizeBytes: uint64(len(content)),
			Blocks: []ManifestBlock{{
				Digest: model.Sum(content), LengthBytes: uint64(len(content)),
			}},
		}},
	})
	if err != nil {
		f.Fatal(err)
	}
	directory, err := NewManifest(ManifestSpec{
		RootKind: EntryDirectory,
		RootPath: "bundle",
		Entries: []ManifestEntry{
			{Kind: EntryFile, LogicalPath: "bundle/result.txt", Mode: 0o640,
				SizeBytes: uint64(len(content)), Blocks: []ManifestBlock{{
					Digest: model.Sum(content), LengthBytes: uint64(len(content)),
				}}},
			{Kind: EntryDirectory, LogicalPath: "bundle", Mode: 0o750},
		},
	})
	if err != nil {
		f.Fatal(err)
	}
	return [][]byte{
		empty.CanonicalJSON().Bytes(),
		file.CanonicalJSON().Bytes(),
		directory.CanonicalJSON().Bytes(),
	}
}
