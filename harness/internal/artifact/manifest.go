package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	SchemaVersion       = 1
	BlockSize           = 1 << 20
	MaxRoots            = 16
	MaxEntries          = 4096
	MaxLogicalPathBytes = 512
	MaxTotalBytes       = 256 << 20
	MaxManifestBytes    = 4 << 20
)

var (
	ErrInvalidManifest = errors.New("invalid Artifact manifest")
	ErrArtifactLimit   = errors.New("Artifact capture limit exceeded")
	ErrArtifactPath    = errors.New("invalid Artifact workspace path")
)

type EntryKind string

const (
	EntryFile      EntryKind = "file"
	EntryDirectory EntryKind = "directory"
)

func (kind EntryKind) Valid() bool {
	return kind == EntryFile || kind == EntryDirectory
}

type ManifestBlock struct {
	Digest      model.Digest `json:"block_digest"`
	LengthBytes uint64       `json:"length_bytes"`
	OffsetBytes uint64       `json:"offset_bytes"`
}

type ManifestEntry struct {
	Blocks      []ManifestBlock `json:"blocks"`
	Kind        EntryKind       `json:"kind"`
	LogicalPath string          `json:"logical_path"`
	Mode        uint32          `json:"mode"`
	SizeBytes   uint64          `json:"size_bytes"`
}

type ManifestSpec struct {
	RootKind EntryKind
	RootPath string
	Entries  []ManifestEntry
}

// Manifest is the exact canonical description of one selected workspace
// root. ManifestDigest is SHA-256 over the canonical bytes. RootDigest is
// domain-separated SHA-256 over those same bytes so a root identifier cannot
// be confused with the address of the manifest object itself.
type Manifest struct {
	rootKind       EntryKind
	rootPath       string
	entries        []ManifestEntry
	totalBytes     uint64
	canonical      model.JSON
	manifestDigest model.Digest
	rootDigest     model.Digest
}

func NewManifest(spec ManifestSpec) (Manifest, error) {
	if !spec.RootKind.Valid() {
		return Manifest{}, fmt.Errorf("%w: unknown root kind", ErrInvalidManifest)
	}
	rootPath, err := validateLogicalPath(spec.RootPath)
	if err != nil {
		return Manifest{}, err
	}
	if len(spec.Entries) == 0 || len(spec.Entries) > MaxEntries {
		return Manifest{}, fmt.Errorf("%w: manifest has %d entries, want 1..%d",
			ErrArtifactLimit, len(spec.Entries), MaxEntries)
	}
	entries := cloneManifestEntries(spec.Entries)
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].LogicalPath < entries[right].LogicalPath
	})
	total, err := validateManifestEntries(rootPath, spec.RootKind, entries)
	if err != nil {
		return Manifest{}, err
	}
	wire := manifestWire{SchemaVersion: SchemaVersion, RootKind: spec.RootKind,
		RootPath: rootPath, TotalBytes: total, Entries: entries}
	canonical, err := model.JSONFrom(wire)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: canonical encoding: %v", ErrInvalidManifest, err)
	}
	if len(canonical.Bytes()) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: manifest has %d bytes, max %d",
			ErrArtifactLimit, len(canonical.Bytes()), MaxManifestBytes)
	}
	manifestDigest := model.Sum(canonical.Bytes())
	rootMaterial := append([]byte("mnemon/artifact-root/v1\x00"), canonical.Bytes()...)
	return Manifest{rootKind: spec.RootKind, rootPath: rootPath, entries: entries,
		totalBytes: total, canonical: canonical, manifestDigest: manifestDigest,
		rootDigest: model.Sum(rootMaterial)}, nil
}

type manifestWire struct {
	Entries       []ManifestEntry `json:"entries"`
	RootKind      EntryKind       `json:"root_kind"`
	RootPath      string          `json:"root_path"`
	SchemaVersion int             `json:"schema_version"`
	TotalBytes    uint64          `json:"total_bytes"`
}

// The model Digest type is deliberately marshal-only: every trust boundary
// must parse its textual form explicitly. Keep a separate read wire here so
// manifest decoding cannot bypass that validation.
type manifestReadWire struct {
	Entries       []manifestReadEntry `json:"entries"`
	RootKind      EntryKind           `json:"root_kind"`
	RootPath      string              `json:"root_path"`
	SchemaVersion int                 `json:"schema_version"`
	TotalBytes    uint64              `json:"total_bytes"`
}

type manifestReadEntry struct {
	Blocks      []manifestReadBlock `json:"blocks"`
	Kind        EntryKind           `json:"kind"`
	LogicalPath string              `json:"logical_path"`
	Mode        uint32              `json:"mode"`
	SizeBytes   uint64              `json:"size_bytes"`
}

type manifestReadBlock struct {
	Digest      string `json:"block_digest"`
	LengthBytes uint64 `json:"length_bytes"`
	OffsetBytes uint64 `json:"offset_bytes"`
}

func ParseManifest(raw []byte) (Manifest, error) {
	if len(raw) == 0 || len(raw) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: manifest byte length is outside 1..%d",
			ErrArtifactLimit, MaxManifestBytes)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return Manifest{}, fmt.Errorf("%w: bytes are not exact canonical JSON", ErrInvalidManifest)
	}
	var wire manifestReadWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Manifest{}, fmt.Errorf("%w: closed schema: %v", ErrInvalidManifest, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Manifest{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidManifest)
	}
	if wire.SchemaVersion != SchemaVersion {
		return Manifest{}, fmt.Errorf("%w: unsupported schema version", ErrInvalidManifest)
	}
	entries := make([]ManifestEntry, len(wire.Entries))
	for entryIndex, readEntry := range wire.Entries {
		blocks := make([]ManifestBlock, len(readEntry.Blocks))
		for blockIndex, readBlock := range readEntry.Blocks {
			digest, err := model.ParseDigest(readBlock.Digest)
			if err != nil {
				return Manifest{}, fmt.Errorf("%w: entry %q block digest: %v",
					ErrInvalidManifest, readEntry.LogicalPath, err)
			}
			blocks[blockIndex] = ManifestBlock{Digest: digest,
				LengthBytes: readBlock.LengthBytes, OffsetBytes: readBlock.OffsetBytes}
		}
		entries[entryIndex] = ManifestEntry{Blocks: blocks, Kind: readEntry.Kind,
			LogicalPath: readEntry.LogicalPath, Mode: readEntry.Mode, SizeBytes: readEntry.SizeBytes}
	}
	manifest, err := NewManifest(ManifestSpec{RootKind: wire.RootKind,
		RootPath: wire.RootPath, Entries: entries})
	if err != nil {
		return Manifest{}, err
	}
	if wire.TotalBytes != manifest.TotalBytes() || manifest.CanonicalJSON().String() != string(raw) {
		return Manifest{}, fmt.Errorf("%w: projection, order, or total differs", ErrInvalidManifest)
	}
	return manifest, nil
}

func validateManifestEntries(root string, rootKind EntryKind,
	entries []ManifestEntry,
) (uint64, error) {
	paths := make(map[string]EntryKind, len(entries))
	blockLengths := make(map[model.Digest]uint64)
	var total uint64
	for index := range entries {
		entry := &entries[index]
		logical, err := validateLogicalPath(entry.LogicalPath)
		if err != nil {
			return 0, err
		}
		entry.LogicalPath = logical
		if index > 0 && entries[index-1].LogicalPath >= logical {
			return 0, fmt.Errorf("%w: entries are duplicate or not strictly ordered", ErrInvalidManifest)
		}
		if !logicalWithinRoot(root, logical) {
			return 0, fmt.Errorf("%w: entry %q escapes root %q", ErrInvalidManifest, logical, root)
		}
		if !entry.Kind.Valid() || entry.Mode > 0o777 {
			return 0, fmt.Errorf("%w: entry %q has invalid kind or mode", ErrInvalidManifest, logical)
		}
		blocks, size, err := validateManifestEntryContent(*entry)
		if err != nil {
			return 0, err
		}
		entry.Blocks = blocks
		entry.SizeBytes = size
		for _, block := range blocks {
			if previous, present := blockLengths[block.Digest]; present && previous != block.LengthBytes {
				return 0, fmt.Errorf("%w: block digest has conflicting lengths", ErrInvalidManifest)
			}
			blockLengths[block.Digest] = block.LengthBytes
		}
		if total > MaxTotalBytes-size {
			return 0, fmt.Errorf("%w: root content exceeds %d bytes", ErrArtifactLimit, MaxTotalBytes)
		}
		total += size
		paths[logical] = entry.Kind
	}
	rootEntryKind, present := paths[root]
	if !present || rootEntryKind != rootKind {
		return 0, fmt.Errorf("%w: root entry does not match root identity", ErrInvalidManifest)
	}
	if rootKind == EntryFile && len(entries) != 1 {
		return 0, fmt.Errorf("%w: file root must contain exactly one entry", ErrInvalidManifest)
	}
	for _, entry := range entries {
		if entry.LogicalPath == root {
			continue
		}
		parent := path.Dir(entry.LogicalPath)
		if parent == "." && root == "." {
			parent = "."
		}
		if paths[parent] != EntryDirectory {
			return 0, fmt.Errorf("%w: entry %q has no manifest directory parent",
				ErrInvalidManifest, entry.LogicalPath)
		}
	}
	return total, nil
}

func validateManifestEntryContent(entry ManifestEntry) ([]ManifestBlock, uint64, error) {
	blocks := append([]ManifestBlock{}, entry.Blocks...)
	if entry.Kind == EntryDirectory {
		if entry.SizeBytes != 0 || len(blocks) != 0 {
			return nil, 0, fmt.Errorf("%w: directory %q carries file content",
				ErrInvalidManifest, entry.LogicalPath)
		}
		return make([]ManifestBlock, 0), 0, nil
	}
	if entry.SizeBytes > MaxTotalBytes {
		return nil, 0, fmt.Errorf("%w: file %q exceeds %d bytes",
			ErrArtifactLimit, entry.LogicalPath, MaxTotalBytes)
	}
	if entry.SizeBytes == 0 {
		if len(blocks) != 0 {
			return nil, 0, fmt.Errorf("%w: empty file %q has blocks", ErrInvalidManifest, entry.LogicalPath)
		}
		return make([]ManifestBlock, 0), 0, nil
	}
	if len(blocks) == 0 {
		return nil, 0, fmt.Errorf("%w: nonempty file %q has no blocks", ErrInvalidManifest, entry.LogicalPath)
	}
	var offset uint64
	for index, block := range blocks {
		if block.Digest.IsZero() || block.OffsetBytes != offset || block.LengthBytes == 0 ||
			block.LengthBytes > BlockSize {
			return nil, 0, fmt.Errorf("%w: file %q has an invalid block map", ErrInvalidManifest, entry.LogicalPath)
		}
		if index < len(blocks)-1 && block.LengthBytes != BlockSize {
			return nil, 0, fmt.Errorf("%w: file %q has a short non-final block", ErrInvalidManifest, entry.LogicalPath)
		}
		offset += block.LengthBytes
	}
	if offset != entry.SizeBytes {
		return nil, 0, fmt.Errorf("%w: file %q block length differs from size", ErrInvalidManifest, entry.LogicalPath)
	}
	return blocks, entry.SizeBytes, nil
}

func validateLogicalPath(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 ||
		len(value) > MaxLogicalPathBytes || strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("%w: path must be relative UTF-8 within %d bytes",
			ErrArtifactPath, MaxLogicalPathBytes)
	}
	for _, component := range strings.Split(value, "/") {
		if component == ".." || component == "" {
			return "", fmt.Errorf("%w: path contains traversal or an empty component", ErrArtifactPath)
		}
	}
	clean := path.Clean(value)
	if clean != value || clean == ".." || strings.HasPrefix(clean, "../") || internalHarnessPath(clean) {
		return "", fmt.Errorf("%w: path is noncanonical, traversing, or internal", ErrArtifactPath)
	}
	return clean, nil
}

func internalHarnessPath(logical string) bool {
	return logical == ".mnemon/harness" || strings.HasPrefix(logical, ".mnemon/harness/")
}

func logicalWithinRoot(root, entry string) bool {
	if root == "." {
		return true
	}
	return entry == root || strings.HasPrefix(entry, root+"/")
}

func cloneManifestEntries(entries []ManifestEntry) []ManifestEntry {
	result := make([]ManifestEntry, len(entries))
	for index := range entries {
		result[index] = entries[index]
		result[index].Blocks = append([]ManifestBlock{}, entries[index].Blocks...)
	}
	return result
}

func (manifest Manifest) RootKind() EntryKind          { return manifest.rootKind }
func (manifest Manifest) RootPath() string             { return manifest.rootPath }
func (manifest Manifest) Entries() []ManifestEntry     { return cloneManifestEntries(manifest.entries) }
func (manifest Manifest) TotalBytes() uint64           { return manifest.totalBytes }
func (manifest Manifest) CanonicalJSON() model.JSON    { return manifest.canonical }
func (manifest Manifest) ManifestDigest() model.Digest { return manifest.manifestDigest }
func (manifest Manifest) RootDigest() model.Digest     { return manifest.rootDigest }
func (manifest Manifest) IsZero() bool                 { return manifest.canonical.IsZero() }
func (manifest Manifest) MarshalJSON() ([]byte, error) { return manifest.canonical.MarshalJSON() }
